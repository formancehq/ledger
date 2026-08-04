package check

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"slices"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Ordering classes for the emitted findings. Events are sorted by
// (class, keyID, message) so two runs over the same store produce a
// byte-identical event stream — both sides of the comparison are Go maps, whose
// iteration order is randomized.
//
// The incomplete-coverage class is deliberately last: it frames everything above
// it as a partial result rather than a clean comparison.
const (
	signingClassUndecodable = iota
	signingClassMissing
	signingClassUnaudited
	signingClassPublicKey
	signingClassParent
	signingClassConfig
	signingClassIncomplete
)

// signingKeyExpectation is the audit-derived expected value of one
// SubGlobSigningKey row. Public-key bytes are kept verbatim and never
// normalized: the comparison must catch a single flipped byte.
type signingKeyExpectation struct {
	publicKey   []byte
	parentKeyID string
}

// signingFinding is one event to emit, held until the whole comparison is done
// so the emission order can be made deterministic.
type signingFinding struct {
	class     int
	keyID     string
	errorType servicepb.CheckStoreErrorType
	message   string
}

// signingVerifier re-derives the two signing projections — the SubGlobSigningKey
// rows and the SubGlobSigningConfig require-signatures flag — from chain-bound
// orders, and compares that expectation to what is persisted.
//
// Both projections are invariant-#8 projections with no pass before this one:
// state.Recovery loads them straight into the runtime key store and the
// requireSignatures gate, which admission consults to accept or reject every
// signed write. A disk edit therefore changes who may write, and CheckStore
// previously reported the store healthy.
//
// The expected state is NEVER seeded from the live projection, and never from the
// baseline checkpoint either: attributes.writeBaselineAttributes copies the
// attribute zone verbatim from the live store, so seeding from it would verify
// old, never-touched keys against a copy of themselves. This is the one
// structural difference from the baseline-seeded passes (compareSchema,
// compareIndexes, …), and it is why incomplete archived coverage must be
// reported instead of papered over.
//
// One instance serves two audit ranges — the live logs and, once wired, the
// archived ones read back through cold storage — so the fold is a plain
// order-at-a-time method with no range awareness.
type signingVerifier struct {
	// keys is the expected SubGlobSigningKey content, keyed by key ID. Row
	// absence is the only representation of revocation, so this map's key set is
	// itself load-bearing: a resurrected key and a lost key are both failures.
	keys map[string]signingKeyExpectation
	// requireSignatures starts false, matching how an absent config row decodes.
	requireSignatures bool
	// coldComplete records that the archived audit range was replayed into this
	// expectation. It is fail-closed: a zero-value verifier reports incomplete
	// coverage rather than presenting a live-range-only replay as clean.
	coldComplete bool
	// archiveEndSeq is the highest log sequence covered by the archived chapters
	// foldArchived walked — the boundary above which the live range takes over.
	// The live fold uses it to skip items it already folded from cold storage:
	// the archived audit items are purged from the live store but the surviving
	// AuditItem rows can still reach below the boundary, and re-applying a
	// register there would resurrect a key a later revoke removed.
	archiveEndSeq uint64
}

func newSigningVerifier() *signingVerifier {
	return &signingVerifier{keys: make(map[string]signingKeyExpectation)}
}

// applyOrder folds one order into the expected signing state, ignoring every
// order shape that is not a system-scoped signing order.
//
// Callers MUST only pass orders from SUCCESSFUL audit entries: a rejected order
// left no trace in the projection, so folding it would manufacture a divergence.
func (v *signingVerifier) applyOrder(order *raftcmdpb.Order) {
	switch payload := order.GetSystemScoped().GetPayload().(type) {
	case *raftcmdpb.SystemScopedOrder_RegisterSigningKey:
		register := payload.RegisterSigningKey

		// Upsert, not insert: the FSM has no duplicate-ID rejection, so
		// re-registering a key ID legitimately replaces its material and its
		// parent link. The bytes are copied because the order may be reused or
		// mutated after this returns — an aliased expectation would follow that
		// mutation and end up comparing a row against itself.
		v.keys[register.GetKeyId()] = signingKeyExpectation{
			publicKey:   append([]byte(nil), register.GetPublicKey()...),
			parentKeyID: register.GetParentKeyId(),
		}
	case *raftcmdpb.SystemScopedOrder_RevokeSigningKey:
		revoke := payload.RevokeSigningKey

		revoked := []string{revoke.GetKeyId()}
		if revoke.GetCascade() {
			// Re-derived from the expected parent map, never read off the log's
			// cascaded_key_ids: re-deriving the cascade is the entire point of
			// this pass, and trusting the recorded list would make a tampered
			// projection able to justify itself.
			revoked = append(revoked, v.descendantsOf(revoke.GetKeyId())...)
		}

		// Deleting an unknown key is a no-op, matching the FSM.
		for _, keyID := range revoked {
			delete(v.keys, keyID)
		}
	case *raftcmdpb.SystemScopedOrder_SetSigningConfig:
		v.requireSignatures = payload.SetSigningConfig.GetRequireSignatures()
	}
}

// descendantsOf returns every key reachable from keyID through the expected
// parent relation, excluding keyID itself.
//
// Traversal order is irrelevant and this deliberately does NOT reproduce the
// FSM's: state.WriteSet.GetSigningKeyChildren returns sorted committed children
// followed by un-re-sorted in-batch additions, and reproducing that would couple
// the checker to an implementation detail for no gain. The reachable *set* is
// fully determined by the parent relation, and the comparison is over the final
// key set, so any traversal that visits the whole subtree yields the same answer.
//
// The visited set is what makes the walk terminate: re-registration can point a
// key at a descendant of itself, and a parent cycle would otherwise loop forever.
func (v *signingVerifier) descendantsOf(keyID string) []string {
	var (
		descendants []string
		queue       = []string{keyID}
		visited     = map[string]struct{}{keyID: {}}
	)

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]

		// Collected into a slice rather than deleted in place: mutating v.keys
		// while ranging over it is what this loop must not do.
		for candidate, expectation := range v.keys {
			if expectation.parentKeyID != current {
				continue
			}

			if _, already := visited[candidate]; already {
				continue
			}

			visited[candidate] = struct{}{}
			descendants = append(descendants, candidate)
			queue = append(queue, candidate)
		}
	}

	return descendants
}

// foldArchived replays the signing orders of every ARCHIVED chapter into the
// expectation, oldest chapter first, and records whether that coverage is
// complete.
//
// Signing state has no TTL: a key registered before an archive boundary stays
// authoritative forever, so unlike the idempotency window there is no cutoff to
// stop the walk early — every archived chapter must be read. The ordering is the
// other difference: signing state accumulates forward, so a chapter must be
// folded before any later chapter whose revoke may target one of its keys, where
// reDeriveArchivedIdempotency walks newest-first because each freeze is
// independent.
//
// Seeding from the baseline checkpoint is not an option: it is a verbatim copy of
// the projection under test (see signingVerifier), so cold storage is the only
// audit-derived source for pre-boundary keys.
//
// Like reDeriveArchivedIdempotency, archived entries are trusted as read and are
// NOT re-verified against the hash chain: cold storage sits outside the
// follower-disk threat model this pass targets. Widening the model to cover
// cold-storage tampering would mean re-walking the chain over the whole archived
// history.
//
// Every failure mode — no cold reader, a failed read, an undecodable order — is a
// coverage gap, not a checker failure: coldComplete stays false, compare reports
// SIGNING_VERIFICATION_INCOMPLETE and Check() carries on with the remaining
// passes. The error return is therefore always nil today; it is kept so a future
// caller-fatal condition does not have to churn the call site.
func (v *signingVerifier) foldArchived(
	ctx context.Context,
	chapters []*commonpb.Chapter,
	coldReader *coldstorage.ColdReader,
	logger logging.Logger,
) error {
	archived := make([]*commonpb.Chapter, 0, len(chapters))

	for _, ch := range chapters {
		if ch.GetStatus() != commonpb.ChapterStatus_CHAPTER_ARCHIVED {
			continue
		}

		archived = append(archived, ch)

		// CloseSequence is the last LOG sequence of the chapter — the boundary the
		// live fold compares AuditItem.LogSequence against. Not to be confused
		// with CloseAuditSequence (the last AUDIT sequence), which orders the walk
		// below; the two are different fields and not interchangeable.
		if ch.GetCloseSequence() > v.archiveEndSeq {
			v.archiveEndSeq = ch.GetCloseSequence()
		}
	}

	// Nothing archived: the live audit range already spans the whole history, so
	// the expectation is complete without any cold read.
	if len(archived) == 0 {
		v.coldComplete = true

		return nil
	}

	// Restore and CLI paths legitimately run without cold storage. Reporting the
	// gap is the correct outcome there — not an error.
	if coldReader == nil {
		logger.Info("archived chapters exist but cold storage is unavailable; signing keys registered before the archive boundary cannot be verified")

		return nil
	}

	// Oldest first: a revoke in a later chapter must see the keys the earlier
	// chapters registered, or it would delete nothing and leave the key expected.
	slices.SortFunc(archived, func(a, b *commonpb.Chapter) int {
		return cmp.Compare(a.GetCloseAuditSequence(), b.GetCloseAuditSequence())
	})

	for _, ch := range archived {
		coldPebble, err := coldReader.GetReader(ctx, ch.GetId())
		if err != nil {
			logger.Infof("reading archived chapter %d for signing verification failed: %v", ch.GetId(), err)

			return nil
		}

		if err := v.foldChapter(coldPebble); err != nil {
			logger.Infof("folding signing orders from archived chapter %d failed: %v", ch.GetId(), err)

			return nil
		}
	}

	v.coldComplete = true

	return nil
}

// foldChapter folds the signing orders of one archived chapter's audit items into
// the expectation. Any failure is reported to the caller, which downgrades the
// whole fold to incomplete coverage: a partially folded chapter would carry a
// register whose revoke was never read.
func (v *signingVerifier) foldChapter(reader dal.PebbleReader) error {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneCold, dal.SubColdAuditItem},
		UpperBound: []byte{dal.ZoneCold, dal.SubColdAuditItem + 1},
	})
	if err != nil {
		return fmt.Errorf("creating archived audit item iter: %w", err)
	}

	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		value, err := iter.ValueAndErr()
		if err != nil {
			return fmt.Errorf("reading archived audit item value: %w", err)
		}

		item := &auditpb.AuditItem{}
		if err := item.UnmarshalVT(value); err != nil {
			return fmt.Errorf("unmarshaling archived audit item %x: %w", iter.Key(), err)
		}

		// Failure-side items carry LogSequence=0 and changed no state, so they must
		// not contribute to the expected set. Sound because buildAuditItems
		// (state/audit.go:57-78) sets LogSequence only from a CreatedLog sequence,
		// failure entries carry no logs, and NextSequenceID starts at 1
		// (state/fsmstate.go:62) so 0 is never a real log sequence.
		if item.GetLogSequence() == 0 {
			continue
		}

		order := &raftcmdpb.Order{}
		if err := order.UnmarshalVT(item.GetSerializedOrder()); err != nil {
			return fmt.Errorf("decoding order from archived audit item %x: %w", iter.Key(), err)
		}

		v.applyOrder(order)
	}

	return iter.Error()
}

// compare reads both persisted signing projections and reports every divergence
// from the audit-derived expectation.
//
// Findings are accumulated, sorted and only then emitted: both the expectation
// and the stored side are maps, so emitting as they are discovered would make
// two Check() runs over the same store produce different event streams.
//
// Public-key bytes never appear in a message. The key ID plus the name of the
// diverging field identifies the problem completely, and the material is
// sensitive-adjacent.
func (v *signingVerifier) compare(reader dal.PebbleReader, callback func(*servicepb.CheckStoreEvent)) error {
	stored, malformed, err := query.ReadSigningKeys(reader)
	if err != nil {
		return fmt.Errorf("reading the stored signing keys: %w", err)
	}

	storedRequireSignatures, err := query.ReadSigningConfig(reader)
	if err != nil {
		return fmt.Errorf("reading the stored signing config: %w", err)
	}

	findings := make([]signingFinding, 0, len(malformed)+len(v.keys)+len(stored))

	// A row too short to decode is reported in its own right. ReadSigningKeys
	// skips it, so an audited key on such a row ALSO reads as absent below —
	// two symptoms of one corruption, which is more useful to an operator than
	// either alone.
	for _, row := range malformed {
		findings = append(findings, signingFinding{
			class:     signingClassUndecodable,
			keyID:     row.KeyID,
			errorType: servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH,
			message: fmt.Sprintf(
				"signing key %q has an undecodable stored row: %s (value length %d)",
				row.KeyID, row.Reason, row.ValueLength),
		})
	}

	// An incomplete archived fold makes the expectation a PREFIX of the real
	// history, which is unsound in both directions: a revoke in an unread chapter
	// leaves its key expected (reported as missing from the store), and a register
	// in an unread chapter leaves its row unexpected (reported as injected). Both
	// are false positives against a healthy store, so the key and config
	// comparisons are skipped entirely and the run reports only that it could not
	// verify. Suppressing detection for that run is the honest outcome — claiming a
	// mismatch we cannot substantiate is worse than admitting the gap, and the same
	// reasoning is why the empty-audit path folds cold storage instead of assuming
	// an empty expectation.
	//
	// The malformed-row class above is deliberately outside this guard: a row too
	// short to decode is a fact about that row and needs no audit oracle at all.
	if !v.coldComplete {
		findings = append(findings, signingFinding{
			class:     signingClassIncomplete,
			errorType: servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_VERIFICATION_INCOMPLETE,
			message: "signing state could not be verified over the whole history: the archived audit range was not replayed, " +
				"so keys registered before the archive boundary — which stay authoritative forever, signing state having no TTL — are unverified. " +
				"The key and config comparisons are skipped for this run rather than reported against a partial expectation",
		})

		emitSigningFindings(findings, callback)

		return nil
	}

	for keyID, expected := range v.keys {
		actual, present := stored[keyID]
		if !present {
			findings = append(findings, signingFinding{
				class:     signingClassMissing,
				keyID:     keyID,
				errorType: servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH,
				message: fmt.Sprintf(
					"signing key %q was registered by an audited order but missing from the store",
					keyID),
			})

			continue
		}

		// Each diverging field is its own finding: a row with both altered
		// material and a re-pointed parent has been tampered with twice, and
		// collapsing that into one event loses half the evidence.
		if !bytes.Equal(expected.publicKey, actual.PublicKey) {
			findings = append(findings, signingFinding{
				class:     signingClassPublicKey,
				keyID:     keyID,
				errorType: servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH,
				message: fmt.Sprintf(
					"signing key %q has stored public-key bytes that differ from the audited registration",
					keyID),
			})
		}

		if expected.parentKeyID != actual.ParentKeyID {
			findings = append(findings, signingFinding{
				class:     signingClassParent,
				keyID:     keyID,
				errorType: servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH,
				message: fmt.Sprintf(
					"signing key %q has stored parent_key_id %q, but the audited registration declares %q",
					keyID, actual.ParentKeyID, expected.parentKeyID),
			})
		}
	}

	for keyID := range stored {
		if _, expected := v.keys[keyID]; expected {
			continue
		}

		findings = append(findings, signingFinding{
			class:     signingClassUnaudited,
			keyID:     keyID,
			errorType: servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_KEY_MISMATCH,
			message: fmt.Sprintf(
				"stored signing key %q has no audited registration (injected, or an audited revocation was lost)",
				keyID),
		})
	}

	if storedRequireSignatures != v.requireSignatures {
		findings = append(findings, signingFinding{
			class:     signingClassConfig,
			errorType: servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_CONFIG_MISMATCH,
			message: fmt.Sprintf(
				"stored require-signatures flag is %t, but the audited SetSigningConfig orders derive %t",
				storedRequireSignatures, v.requireSignatures),
		})
	}

	emitSigningFindings(findings, callback)

	return nil
}

// emitSigningFindings sorts the accumulated findings and emits them.
//
// Both sides of the comparison are Go maps, so emitting as findings are
// discovered would make two Check() runs over the same store produce different
// event streams. Shared by the complete and incomplete-coverage exits so neither
// can drift into emitting unsorted.
func emitSigningFindings(findings []signingFinding, callback func(*servicepb.CheckStoreEvent)) {
	slices.SortFunc(findings, func(a, b signingFinding) int {
		return cmp.Or(
			cmp.Compare(a.class, b.class),
			cmp.Compare(a.keyID, b.keyID),
			cmp.Compare(a.message, b.message),
		)
	})

	for _, finding := range findings {
		callback(errorEvent(finding.errorType, finding.message, 0, "", "", ""))
	}
}
