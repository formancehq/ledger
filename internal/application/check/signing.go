package check

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
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
// signed write. A disk edit therefore changes who may write.
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
	// liveTruncated records that the LIVE audit range was NOT folded to its end.
	// verifyAuditHashChain returns early on a chain break — an entry carrying
	// embedded items, a header that cannot be re-hashed, or a hash mismatch — and
	// Check() deliberately carries on from there to surface other projection
	// errors. The signing fold lives inside that loop, so the expectation stops at
	// the break while coldComplete may already be true.
	//
	// That leaves exactly the prefix state coldComplete guards against, and it is
	// unsound in both directions: every key registered past the break reads as
	// injected, and every revoke past it leaves its key expected, hence reported
	// missing. So a truncated live fold suppresses the comparisons the same way an
	// unread archive does — reporting mismatches we cannot substantiate against a
	// store whose real problem is the chain break is strictly worse than saying so.
	liveTruncated bool
	// proposalParents is the parent relation as it stood BEFORE any of the current
	// proposal's orders were folded — the checker's stand-in for the FSM's
	// committed state. Rebuilt lazily (see ensureProposalSnapshot) because most
	// audit entries carry no signing order at all.
	proposalParents map[string]string
	// proposalEdges is every parent edge the current proposal ASSERTED, keyed by
	// child, including edges a later registration in the same proposal replaced.
	// GetSigningKeyChildren appends the key of every pending addition whose
	// parentKeyID matches, so a superseded in-proposal edge still cascades on the
	// live path — and the running relation alone cannot see it, because the
	// replacement overwrote it, while proposalParents cannot either, because a key
	// first registered inside this proposal has no pre-proposal entry. Reset with
	// proposalParents.
	proposalEdges map[string][]string
	// proposalRevoked is every key the current proposal has ALREADY removed — each
	// revoke target plus every descendant that revoke's cascade reached.
	//
	// It reproduces GetSigningKeyChildren's pendingRemovals filter, which is built
	// over the WHOLE pendingSigningKeyUpdates slice with no ordering awareness: once
	// a proposal removes a key, that key is excluded from EVERY cascade in the same
	// proposal, including one evaluated after a later registration put it back.
	//
	// Absence from v.keys cannot express that. Absence is a point-in-time fact and a
	// re-registration restores the row, so the walk would follow the reinstated edge
	// and cascade a key the FSM left in the store. Reset with proposalParents.
	proposalRevoked map[string]struct{}
	// proposalSnapshotValid says whether proposalParents describes the current
	// proposal. Cleared per entry in O(1); the map is only populated if that entry
	// turns out to fold a signing order.
	proposalSnapshotValid bool
	// archiveEndSeq is the highest log sequence covered by the archived chapters
	// foldArchived walked — the boundary above which the live range takes over.
	// The live fold uses it to skip items it already folded from cold storage:
	// the archived audit items are purged from the live store but the surviving
	// AuditItem rows can still reach below the boundary, and re-applying a
	// register there would resurrect a key a later revoke removed.
	archiveEndSeq uint64
}

func newSigningVerifier() *signingVerifier {
	return &signingVerifier{
		keys:            make(map[string]signingKeyExpectation),
		proposalParents: make(map[string]string),
		proposalEdges:   make(map[string][]string),
		proposalRevoked: make(map[string]struct{}),
	}
}

// markLiveTruncated records that the live audit fold stopped short of the end of
// the range. Called from every non-error early exit in verifyAuditHashChain; see
// the liveTruncated field for why a truncated fold cannot be compared.
func (v *signingVerifier) markLiveTruncated() {
	v.liveTruncated = true
}

// beginProposal marks the start of a new proposal's orders. One proposal is one
// audit entry, matching WriteSet.Reset, which the FSM calls once per proposal and
// which is what makes "committed" mean "before this proposal" there too.
//
// O(1): it only invalidates the snapshot. Building it is deferred to
// ensureProposalSnapshot so the overwhelming majority of audit entries — those
// with no signing order — cost nothing.
func (v *signingVerifier) beginProposal() {
	v.proposalSnapshotValid = false
}

// ensureProposalSnapshot captures the parent relation before the current
// proposal's first signing order mutates it.
func (v *signingVerifier) ensureProposalSnapshot() {
	if v.proposalSnapshotValid {
		return
	}

	clear(v.proposalParents)
	clear(v.proposalEdges)
	clear(v.proposalRevoked)

	for keyID, expectation := range v.keys {
		v.proposalParents[keyID] = expectation.parentKeyID
	}

	v.proposalSnapshotValid = true
}

// applyOrder folds one order into the expected signing state, ignoring every
// order shape that is not a system-scoped signing order.
//
// Callers MUST only pass orders from SUCCESSFUL audit entries: a rejected order
// left no trace in the projection, so folding it would manufacture a divergence.
func (v *signingVerifier) applyOrder(order *raftcmdpb.Order) {
	switch payload := order.GetSystemScoped().GetPayload().(type) {
	case *raftcmdpb.SystemScopedOrder_RegisterSigningKey:
		// Taken before the mutation below: a re-registration that moves a key to a
		// new parent must not erase the old edge the FSM's cascade still sees.
		v.ensureProposalSnapshot()

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

		// Recorded even though the line above already holds this edge: a LATER
		// registration in the same proposal would overwrite it, and the FSM would
		// still cascade from it (GetSigningKeyChildren walks every pending addition).
		// Root registrations are skipped because "" is not a cascade source.
		if parent := register.GetParentKeyId(); parent != "" {
			v.proposalEdges[register.GetKeyId()] = append(v.proposalEdges[register.GetKeyId()], parent)
		}
	case *raftcmdpb.SystemScopedOrder_RevokeSigningKey:
		// Required here too, not just on the register path: descendantsOf reads
		// proposalParents, so without this a cascade in a proposal that registered
		// nothing would walk the snapshot left behind by an EARLIER proposal and
		// cascade from a parent link that has since been committed away.
		v.ensureProposalSnapshot()

		revoke := payload.RevokeSigningKey

		revoked := []string{revoke.GetKeyId()}
		if revoke.GetCascade() {
			// Re-derived from the expected parent map, never read off the log's
			// cascaded_key_ids: re-deriving the cascade is the entire point of
			// this pass, and trusting the recorded list would make a tampered
			// projection able to justify itself.
			revoked = append(revoked, v.descendantsOf(revoke.GetKeyId())...)
		}

		// proposalRevoked is filled only HERE, after descendantsOf has run, and that
		// ordering is load-bearing rather than incidental: processRevokeSigningKey
		// walks the whole child relation before calling RemoveSigningKey, so the
		// pendingRemovals set a cascade sees holds the removals of the EARLIER orders
		// in its proposal and none of its own. Filling it first would make a revoke
		// exclude its own targets from its own cascade.
		//
		// Deleting an unknown key is a no-op, matching the FSM.
		for _, keyID := range revoked {
			delete(v.keys, keyID)
			v.proposalRevoked[keyID] = struct{}{}
		}
	case *raftcmdpb.SystemScopedOrder_SetSigningConfig:
		v.requireSignatures = payload.SetSigningConfig.GetRequireSignatures()
	}
}

// descendantsOf returns every key reachable from keyID through the expected
// parent relation, excluding keyID itself.
//
// Traversal ORDER is irrelevant and this deliberately does not reproduce the
// FSM's: state.WriteSet.GetSigningKeyChildren returns sorted committed children
// followed by un-re-sorted in-proposal additions, and the comparison is over the
// final key set, so any traversal visiting the whole subtree agrees.
//
// The set of EDGES is not irrelevant, and this is where a subtle divergence lives.
// GetSigningKeyChildren unions two relations: the COMMITTED children of the key
// (minus those the same proposal removed) and the key of EVERY pending addition in
// the proposal whose parentKeyID matches. So a key re-registered under a new parent
// within the same proposal as a cascade revoke of its OLD parent is cascaded from
// both — the FSM never consults the reassigned pointer to exclude it. Walking only
// the running relation would drop that key from the cascade, leave it in the
// expected set, and report a false SIGNING_KEY_MISMATCH against a store that
// legitimately deleted it.
//
// Hence THREE edge sources below, and all three are load-bearing:
//   - the running relation, for keys whose parent is unchanged in this proposal;
//   - proposalParents, for a key whose pre-proposal (committed) parent this
//     proposal reassigned — the FSM still sees the committed edge;
//   - proposalEdges, for an edge this proposal asserted and then SUPERSEDED with a
//     later registration of the same key. That case is invisible to the other two:
//     the replacement overwrote the running pointer, and a key first registered
//     inside this proposal has no pre-proposal entry at all. "Pending addition"
//     means every element of pendingSigningKeyUpdates, not just the last one per
//     key, so register(child→parent) + register(child→root) + cascade-revoke(parent)
//     in ONE proposal does delete child on the live path.
//
// Keys the proposal already removed are excluded through proposalRevoked, which
// is what reproduces GetSigningKeyChildren's pendingRemovals filter. Absence from
// v.keys is NOT enough on its own: that filter is a set over the whole proposal,
// while absence is a point-in-time fact, so revoke(X) + register(X under P) +
// cascade-revoke(P) in ONE proposal has the FSM keep X — its removal excludes it
// from the cascade even though the registration came after — while a walk driven
// by v.keys alone follows the reinstated edge and reports a false mismatch against
// the surviving row.
//
// The exclusion has to skip the candidate ENTIRELY rather than just drop it from
// the result: GetSigningKeyChildren filters it out of what it returns, so the FSM's
// BFS never recurses through it and its own subtree survives too.
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
			// Checked before the edges: a key this proposal already removed is no
			// longer a cascade candidate on the live path, whatever edge a later
			// registration in the same proposal gave it back.
			if _, removed := v.proposalRevoked[candidate]; removed {
				continue
			}

			// Any of the three edges makes it a child, mirroring the union
			// GetSigningKeyChildren returns.
			if expectation.parentKeyID != current &&
				v.proposalParents[candidate] != current &&
				!slices.Contains(v.proposalEdges[candidate], current) {
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

		foldErr := v.foldChapter(ctx, coldPebble)
		closeErr := coldPebble.Close()
		if foldErr != nil {
			logger.Infof("folding signing orders from archived chapter %d failed: %v", ch.GetId(), foldErr)

			return nil
		}
		if closeErr != nil {
			logger.Infof("closing archived chapter %d after signing verification failed: %v", ch.GetId(), closeErr)

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
//
// Items are paired with their entry rather than scanned on their own, because two
// things can only be decided per-entry. Only SUCCESSFUL entries are folded — a
// rejected order left no trace in the projection — and only items inside the
// entry's fresh-log window [MinLogSequence, MaxLogSequence], which excludes the
// legacy pre-f9ee1e829 per-order replay references that point back at a log an
// earlier entry already folded. Both mirror the live fold in verifyAuditHashChain;
// the archived range needs them for the same reason, since replaying a register
// after its revoke resurrects a key and reports a false mismatch.
func (v *signingVerifier) foldChapter(ctx context.Context, reader dal.PebbleReader) error {
	entries, err := query.ReadAuditEntries(ctx, reader, nil)
	if err != nil {
		return fmt.Errorf("reading archived audit entries: %w", err)
	}

	defer func() { _ = entries.Close() }()

	for {
		entry, err := entries.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("reading archived audit entry: %w", err)
		}

		success := entry.GetSuccess()
		if success == nil {
			continue
		}

		items, err := query.ReadAuditItems(ctx, reader, entry.GetSequence())
		if err != nil {
			return fmt.Errorf("reading archived audit items for sequence %d: %w", entry.GetSequence(), err)
		}

		// One entry is one proposal, which is the boundary the FSM's notion of
		// "committed" is defined against.
		v.beginProposal()

		for _, item := range items {
			logSeq := item.GetLogSequence()
			if logSeq == 0 || logSeq < success.GetMinLogSequence() || logSeq > success.GetMaxLogSequence() {
				continue
			}

			order := &raftcmdpb.Order{}
			if err := order.UnmarshalVT(item.GetSerializedOrder()); err != nil {
				return fmt.Errorf("decoding order from archived audit item at log %d: %w", logSeq, err)
			}

			v.applyOrder(order)
		}
	}

	return nil
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

	// A fold that did not cover the whole audit history makes the expectation a
	// PREFIX of the real one, which is unsound in both directions: a revoke in the
	// unread part leaves its key expected (reported as missing from the store), and
	// a register in it leaves its row unexpected (reported as injected). Both are
	// false positives against a healthy store, so the key and config comparisons
	// are skipped entirely and the run reports only that it could not verify.
	// Suppressing detection for that run is the honest outcome — claiming a mismatch
	// we cannot substantiate is worse than admitting the gap, and the same reasoning
	// is why the empty-audit path folds cold storage instead of assuming an empty
	// expectation.
	//
	// Two independent causes, same consequence: the archived range was never
	// replayed (coldComplete), or the live range was cut short by an audit chain
	// break (liveTruncated). Neither can be salvaged from the accumulated prefix.
	//
	// The malformed-row class above is deliberately outside this guard: a row too
	// short to decode is a fact about that row and needs no audit oracle at all.
	if !v.coldComplete || v.liveTruncated {
		findings = append(findings, signingFinding{
			class:     signingClassIncomplete,
			errorType: servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SIGNING_VERIFICATION_INCOMPLETE,
			message:   signingIncompleteMessage(v.coldComplete, v.liveTruncated),
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

// signingIncompleteMessage explains WHICH part of the audit history went unread,
// so an operator can tell a missing cold-storage backend from a broken hash chain
// without correlating events by hand. Both causes can hold at once.
//
// The consequence sentence is shared: whatever the cause, the key and config
// comparisons were skipped, and that is the part a reader must not have to infer.
func signingIncompleteMessage(coldComplete, liveTruncated bool) string {
	const (
		prefix = "signing state could not be verified over the whole history: "
		suffix = ". The key and config comparisons are skipped for this run rather than " +
			"reported against a partial expectation"

		coldGap = "the archived audit range was not replayed, so keys registered before the archive " +
			"boundary — which stay authoritative forever, signing state having no TTL — are unverified"
		liveGap = "the live audit range was cut short by a hash chain break, so every signing order " +
			"recorded after it is unread"
	)

	switch {
	case coldComplete:
		return prefix + liveGap + suffix
	case !liveTruncated:
		return prefix + coldGap + suffix
	default:
		return prefix + coldGap + "; the live range was also cut short by a hash chain break" + suffix
	}
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
