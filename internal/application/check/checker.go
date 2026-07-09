package check

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"slices"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/zeebo/blake3"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	domainreplay "github.com/formancehq/ledger/v3/internal/domain/replay"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const progressInterval = 100

// Checker verifies store integrity by replaying logs and comparing derived state.
type Checker struct {
	store     *dal.Store
	attrs     *attributes.Attributes
	logger    logging.Logger
	clusterID string
	// coldReader gives the idempotency pass read access to archived audit
	// entries so a still-live frozen outcome whose freezing entry has been
	// archived can be re-derived rather than skipped. nil when cold storage
	// is not configured (e.g. the CLI / restore call sites) — the pass then
	// keeps the post-archive boundary as its verification floor.
	coldReader *coldstorage.ColdReader
	// idempotencyTTL is the boot-validated runtime idempotency TTL, used to
	// size the cold re-derivation window. It is preferred over the persisted
	// projection because it lives in process memory (not on the audited disk),
	// so a disk-tampered PersistedConfig cannot shrink the window. nil where no
	// trusted runtime config exists (CLI / restore backup validation) — the
	// pass then falls back to the persisted TTL.
	idempotencyTTL *time.Duration
}

// NewChecker creates a new Checker. clusterID is used to derive the
// per-cluster key for verifying audit-hash chain entries — it must match
// the value the FSM used when writing those entries (enforced via
// PersistedConfig immutability). coldReader may be nil when cold storage is
// not configured. idempotencyTTL may be nil when no trusted runtime config is
// available (the pass then falls back to the persisted TTL).
func NewChecker(store *dal.Store, attrs *attributes.Attributes, clusterID string, coldReader *coldstorage.ColdReader, idempotencyTTL *time.Duration, logger logging.Logger) *Checker {
	return &Checker{
		store:          store,
		attrs:          attrs,
		logger:         logger,
		clusterID:      clusterID,
		coldReader:     coldReader,
		idempotencyTTL: idempotencyTTL,
	}
}

// Check verifies the store integrity and calls the callback for each event.
// It verifies:
// 1. Log sequence continuity (no gaps)
// 2. BLAKE3 hash chain integrity
// 3. Reversion invariants (no double reverts, valid revert targets)
// 4. Volume consistency (input/output per account/asset)
// 5. Account metadata consistency
// 6. Transaction update consistency
// 7. Archived chapter sealing hash decomposition
// 8. Archived state via baseline checkpoint + 3-way merge comparison.
func (c *Checker) Check(ctx context.Context, callback func(*servicepb.CheckStoreEvent)) error {
	// Take a point-in-time snapshot so that log iteration and live attribute
	// reads see the same committed state. Without this, entries committed
	// between the log scan and the attribute scan cause false-positive
	// mismatches (the live volumes include effects of logs the replay never saw).
	snap, err := c.store.NewReadHandle()
	if err != nil {
		return fmt.Errorf("creating read snapshot: %w", err)
	}

	defer func() { _ = snap.Close() }()

	lastSequence, err := query.ReadLastSequence(snap)
	if err != nil {
		return fmt.Errorf("getting last sequence: %w", err)
	}

	if lastSequence == 0 {
		callback(&servicepb.CheckStoreEvent{
			Type: &servicepb.CheckStoreEvent_Progress{
				Progress: &servicepb.CheckStoreProgress{
					LogsChecked: 0,
					TotalLogs:   0,
				},
			},
		})

		return nil
	}

	// Read archived chapters to adjust the starting point for log replay.
	chaptersCursor, err := query.ReadChapters(ctx, snap)
	if err != nil {
		return fmt.Errorf("reading chapters: %w", err)
	}

	chapters, err := cursor.Collect(chaptersCursor)
	if err != nil {
		return fmt.Errorf("collecting chapters: %w", err)
	}

	var (
		hasArchivedChapters  bool
		archiveEndSeq        uint64 // max close_sequence among archived chapters
		archiveLastAuditHash []byte // last_audit_hash from the latest archived chapter
	)

	for _, p := range chapters {
		if p.GetStatus() == commonpb.ChapterStatus_CHAPTER_ARCHIVED {
			hasArchivedChapters = true

			if len(p.GetSealingHash()) == 0 {
				callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH,
					fmt.Sprintf("archived chapter %d has no sealing hash (unsealed before archive)", p.GetId()),
					p.GetCloseSequence(), "", "", ""))
			} else {
				verifySealingHash(p, callback)
			}

			if p.GetCloseSequence() > archiveEndSeq {
				archiveEndSeq = p.GetCloseSequence()
				archiveLastAuditHash = p.GetLastAuditHash()
			}
		}
	}

	// Create replay store (replaces in-memory maps + txStateStore)
	replay, err := newReplayStore()
	if err != nil {
		return fmt.Errorf("creating replay store: %w", err)
	}

	defer func() { _ = replay.Close() }()

	// Idempotency TTL, in microseconds, used by the hash-chain pass to bound the
	// cold re-derivation window. Prefer the boot-validated runtime config (in
	// process memory, off the audited disk) over the persisted projection, so a
	// disk-tampered PersistedConfig cannot shrink the window; fall back to the
	// persisted value where no runtime config exists (CLI / restore). nil (no
	// runtime config and no persisted config) means the window is unknown, so
	// the cold pass is skipped rather than guessed.
	//
	// "now" for the window is NOT a projection either — it is the highest
	// timestamp the hash chain verifies in this same run (see
	// verifyAuditHashChain), so a tampered lastAppliedTimestamp cannot shift it.
	persisted, err := query.ReadPersistedConfig(snap)
	if err != nil {
		return fmt.Errorf("reading persisted config: %w", err)
	}

	idempotencyTTLMicros := resolveIdempotencyTTLMicros(c.idempotencyTTL, persisted)

	// Open the baseline checkpoint FIRST — its state must be folded into
	// chainBound BEFORE verifyAuditHashChain replays live audit items,
	// so live CreateTransaction / RevertTransaction attribute their
	// tx-scoped metadata to the correct tx-id targets (the FSM counter
	// is seeded from LedgerBoundaries.NextTransactionId, which only the
	// baseline holds for ledgers whose CreateLedger sits in an archived
	// chapter).
	var baselineDB *pebble.DB

	if hasArchivedChapters {
		baselinePath, exists := c.store.BaselineCheckpointPath()
		if exists {
			db, openErr := pebble.Open(baselinePath, &pebble.Options{
				Logger:   dal.NewPebbleLogger(c.logger),
				ReadOnly: true,
			})
			if openErr != nil {
				c.logger.Infof("failed to open baseline checkpoint: %v (skipping entry-by-entry comparison)", openErr)
			} else {
				baselineDB = db

				defer func() { _ = baselineDB.Close() }()
			}
		}
	}

	// chainBound aggregates the audit-derived state feeding
	// verifySkippedOrder: references, reverted, metadata timeline,
	// account-type timeline, per-ledger nextTxID counter, and the set
	// of ledgers whose CreateLedger was observed in the live range.
	// Seeded from baseline first, then verifyAuditHashChain layers the
	// live audit-chain mutations on top.
	chainBound := newChainBoundState()

	baselineReferencesAvailable, err := c.foldBaselineReferences(baselineDB, chainBound.references, chainBound.referenceTxIDs)
	if err != nil {
		return fmt.Errorf("loading baseline references: %w", err)
	}

	// Sibling folds for the non-CONFLICT skip reasons: seed reverted /
	// metadata / accountTypes / ledgerCreationSeen / nextTxID from the
	// baseline. Every entry lands at sentinel seq=0 so the backward
	// walk in mutationStateAtSeq picks up baseline claims below any
	// live log seq.
	baselineChainStateAvailable, err := c.foldBaselineChainState(baselineDB, chainBound)
	if err != nil {
		return fmt.Errorf("loading baseline skip-replay state: %w", err)
	}

	// Verify the audit hash chain before log replay. This iterates
	// every non-archived audit entry and recomputes each hash from the
	// stored orders, chaining from archiveLastAuditHash. Populates
	// expectedSkippable + layers live mutations onto chainBound.
	expectedSkippable, err := c.verifyAuditHashChain(ctx, snap, chapters, archiveLastAuditHash, chainBound, idempotencyTTLMicros, callback)
	if err != nil {
		return fmt.Errorf("verifying audit hash chain: %w", err)
	}

	proposalBoundaries, err := c.newProposalBoundaryReader(ctx, snap, chapters, archiveEndSeq)
	if err != nil {
		return fmt.Errorf("reading proposal log boundaries: %w", err)
	}
	defer func() { _ = proposalBoundaries.Close() }()

	// State tracked during log replay
	var (
		knownLedgers = make(map[string]struct{}) // set of ledger names
		// Per-ledger reversion tracking using bitsets (1 bit per tx ID)
		ledgerKnownTxIDs    = make(map[string]*bitset.Bitset)
		ledgerRevertedTxIDs = make(map[string]*bitset.Bitset)
		// Per-ledger account types for ephemeral purge simulation
		rawLedgerTypes     = make(map[string]map[string]*commonpb.AccountType)
		ledgerAccountTypes = make(map[string][]accounttype.CompiledType)
		// Expected SubAttrIndex registry state derived from CreateIndex /
		// DropIndex / RemovedMetadataFieldType / DeleteLedger logs. The
		// checker compares this against the stored projection in
		// compareIndexes. BuildStatus is intentionally excluded — the
		// BUILDING → READY flip rides on a non-audited IndexReady
		// TechnicalUpdate, so presence + identity (Ledger, Id) are the
		// fields we can re-derive from the audit-bound logs.
		expectedIndexes = make(map[domain.IndexKey]*commonpb.Index)
		// Index keys that had ANY replay activity (CreateIndex /
		// DropIndex / RemovedMetadataFieldType cascade) in the verified
		// range. Used by compareIndexes to decide whether a stored entry
		// missing from `expectedIndexes` is an archive-orphan (no
		// activity → CreateIndex may live in an archived chapter) or a
		// genuine drop the projection should have honoured (activity →
		// surviving entry is tampering, even on a pre-archive ledger).
		indexReplayActivity = make(map[domain.IndexKey]struct{})
		// Ledgers that had a DeleteLedger log replayed in the verified
		// range. Combined with pendingCleanupLedgers in compareIndexes:
		// if a ledger was deleted in replay AND its deferred Pebble purge
		// has already run (not in pendingCleanupLedgers), every stored
		// SubAttrIndex row for it is tampering — even when no per-key
		// replay activity is recorded (e.g. CreateIndex was archived, so
		// neither expectedIndexes nor replayActivity ever held the key).
		deletedInReplay = make(map[string]struct{})
	)

	// excluded is built incrementally as SimulateEphemeralPurge decides to
	// delete a (ledger, account, asset) volume during replay. Deriving the
	// set this way binds it to the audit hash chain via the orders the
	// replay consumes — a tampered AppliedProposal.TransientVolumes or
	// LedgerLog.PurgedVolumes record cannot influence the integrity check.
	excluded := excludedVolumesSet{}
	exclusionCollector := func(ledger, account, asset string) {
		set, exists := excluded[ledger]
		if !exists {
			set = make(map[domain.AccountAssetKey]struct{})
			excluded[ledger] = set
		}
		set[domain.AccountAssetKey{Account: account, Asset: asset}] = struct{}{}
	}

	// stored mirrors `excluded` but is built from the Pebble projections
	// (LedgerLog.PurgedVolumes per log + AppliedProposal.TransientVolumes
	// per proposal). It is compared to `excluded` at the end of replay so
	// any corruption of those records — which the index builder consumes
	// directly — surfaces as EXCLUSION_RECORD_MISMATCH instead of going
	// silent. The audit hash chain protects the orders this comparison
	// indirectly relies on, so a tampered cache cannot make a corrupted
	// state look consistent.
	stored := excludedVolumesSet{}
	addStored := func(ledger, account, asset string) {
		set, exists := stored[ledger]
		if !exists {
			set = make(map[domain.AccountAssetKey]struct{})
			stored[ledger] = set
		}
		set[domain.AccountAssetKey{Account: account, Asset: asset}] = struct{}{}
	}

	nextProposalEnd, hasProposalEnd, err := proposalBoundaries.Next()
	if err != nil {
		return fmt.Errorf("reading first proposal log boundary: %w", err)
	}

	var ephemeralPurgeBuffer *domainreplay.EphemeralPurgeBuffer
	if hasProposalEnd {
		ephemeralPurgeBuffer = domainreplay.NewEphemeralPurgeBuffer()
	}

	// If chapters were archived, pre-populate knownLedgers from Pebble
	// since the CreateLedger logs have been purged.
	if hasArchivedChapters {
		ledgerCursor, err := query.ReadLedgers(ctx, snap)
		if err != nil {
			return fmt.Errorf("reading ledgers for archive recovery: %w", err)
		}

		ledgers, err := cursor.Collect(ledgerCursor)
		if err != nil {
			return fmt.Errorf("collecting ledgers: %w", err)
		}

		for _, info := range ledgers {
			if info.GetDeletedAt() == nil {
				knownLedgers[info.GetName()] = struct{}{}

				if types := info.GetAccountTypes(); len(types) > 0 {
					rawLedgerTypes[info.GetName()] = types
					ledgerAccountTypes[info.GetName()] = accounttype.CompileTypes(types)
				}
			}
		}

		// Pre-populate knownTxIDs from archived transaction states so that
		// reversion invariant checks work correctly for non-archived logs.
		txIter, err := c.attrs.Transaction.NewStreamingIter(snap, nil)
		if err != nil {
			return fmt.Errorf("creating tx streaming iter for archive recovery: %w", err)
		}

		for txIter.Next() {
			entry := txIter.Entry()

			var tk domain.TransactionKey
			if err := tk.Unmarshal(entry.CanonicalKey); err != nil {
				continue // skip unparsable keys
			}

			trackTxID(ledgerKnownTxIDs, tk.LedgerName, tk.ID)

			if entry.Value.GetRevertedByTransaction() != 0 {
				trackTxID(ledgerRevertedTxIDs, tk.LedgerName, tk.ID)
			}
		}

		if err := txIter.Close(); err != nil {
			return fmt.Errorf("closing tx streaming iter: %w", err)
		}

		if err := txIter.Err(); err != nil {
			return fmt.Errorf("pre-populating knownTxIDs: %w", err)
		}
	}

	// Pass 1: Single forward iterator over all logs.
	logIter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneCold, dal.SubColdLog},
		UpperBound: []byte{dal.ZoneCold, dal.SubColdLog, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
	})
	if err != nil {
		return fmt.Errorf("creating log iterator: %w", err)
	}

	defer func() { _ = logIter.Close() }()

	// Start after archived sequences (archived logs are purged from Pebble).
	expectedSeq := archiveEndSeq + 1

	for logIter.First(); logIter.Valid(); logIter.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Extract sequence from key: [ZoneCold(1)][SubColdLog(1)][sequence(8)]
		seq := binary.BigEndian.Uint64(logIter.Key()[2:10])

		for ephemeralPurgeBuffer != nil && hasProposalEnd && seq > nextProposalEnd {
			if err := ephemeralPurgeBuffer.Flush(replay, ledgerAccountTypes, exclusionCollector); err != nil {
				return fmt.Errorf("flushing replay ephemeral purge at missing log boundary %d: %w", nextProposalEnd, err)
			}

			nextProposalEnd, hasProposalEnd, err = proposalBoundaries.Next()
			if err != nil {
				return fmt.Errorf("reading next proposal log boundary: %w", err)
			}
		}

		// 1. Detect gaps
		for expectedSeq < seq {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP,
				fmt.Sprintf("log sequence %d is missing", expectedSeq), expectedSeq, "", "", ""))

			expectedSeq++
		}

		expectedSeq = seq + 1

		value, err := logIter.ValueAndErr()
		if err != nil {
			return fmt.Errorf("reading log %d value: %w", seq, err)
		}

		log := &commonpb.Log{}
		if err := log.UnmarshalVT(value); err != nil {
			return fmt.Errorf("unmarshaling log %d: %w", seq, err)
		}

		// Hash chain verification is now done via audit entries (see audit hash pass below).

		// 2. Replay log to update expected state
		if log.GetPayload() != nil {
			switch payload := log.GetPayload().GetType().(type) {
			case *commonpb.LogPayload_CreateLedger:
				if payload.CreateLedger != nil {
					knownLedgers[payload.CreateLedger.GetName()] = struct{}{}
				}
			case *commonpb.LogPayload_DeleteLedger:
				if payload.DeleteLedger != nil {
					name := payload.DeleteLedger.GetName()
					delete(knownLedgers, name)
					deletedInReplay[name] = struct{}{}

					// DeleteLedger purges every SubAttrIndex entry scoped to
					// this ledger via the deferred Pebble range delete
					// queued by MarkLedgerForCleanup (see processor_ledger.go
					// + batch.deleteLedgerData). Mirror the cascade on the
					// expected projection so a stored entry that survives a
					// ledger deletion still surfaces as a mismatch.
					for key := range expectedIndexes {
						if key.LedgerName == name {
							delete(expectedIndexes, key)
						}
					}
				}
			case *commonpb.LogPayload_Apply:
				if payload.Apply != nil {
					ledgerName := payload.Apply.GetLedgerName()

					if _, ok := knownLedgers[ledgerName]; !ok {
						callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_UNKNOWN_LEDGER,
							fmt.Sprintf("log %d references unknown ledger %q", seq, ledgerName),
							seq, ledgerName, "", ""))

						continue
					}

					if payload.Apply.GetLog() != nil && payload.Apply.GetLog().GetData() != nil {
						if err := domainreplay.ReplayLedgerLog(ledgerName, seq, payload.Apply.GetLog().GetData(), replay, rawLedgerTypes, ledgerAccountTypes, ephemeralPurgeBuffer); err != nil {
							return fmt.Errorf("replaying log %d: %w", seq, err)
						}

						// Index registry derivation: every CreateIndex /
						// DropIndex / RemovedMetadataFieldType log entry
						// shifts the expected SubAttrIndex projection. The
						// build status (BUILDING ↔ READY) rides on a non-
						// audited IndexReady TU and is not tracked here —
						// compareIndexes verifies presence + identity only.
						//
						// Account builtin indexes (e.g. ACCT_BUILTIN_INDEX_ASSET)
						// are verified here at registry level (presence + identity)
						// like every other index. Readstore *contents* (the
						// asset→account entries) are intentionally NOT re-derived:
						// no index type has content verification today; adding it is
						// a cross-cutting invariant-#8 effort tracked separately.
						switch d := payload.Apply.GetLog().GetData().GetPayload().(type) {
						case *commonpb.LedgerLogPayload_CreateIndex:
							if id := d.CreateIndex.GetId(); id != nil {
								key := domain.IndexKey{
									LedgerName: ledgerName,
									Canonical:  indexes.Canonical(id),
								}
								expectedIndexes[key] = &commonpb.Index{Id: id, Ledger: ledgerName}
								indexReplayActivity[key] = struct{}{}
							}
						case *commonpb.LedgerLogPayload_DropIndex:
							if id := d.DropIndex.GetId(); id != nil {
								key := domain.IndexKey{
									LedgerName: ledgerName,
									Canonical:  indexes.Canonical(id),
								}
								delete(expectedIndexes, key)
								indexReplayActivity[key] = struct{}{}
							}
						case *commonpb.LedgerLogPayload_RemovedMetadataFieldType:
							// processRemoveMetadataFieldType cascades into a
							// DropIndex when an index was attached to the
							// removed field; the dropped id rides on the log
							// so the cascade is auditable.
							if id := d.RemovedMetadataFieldType.GetDroppedIndex(); id != nil {
								key := domain.IndexKey{
									LedgerName: ledgerName,
									Canonical:  indexes.Canonical(id),
								}
								delete(expectedIndexes, key)
								indexReplayActivity[key] = struct{}{}
							}
						}

						checkReversionInvariants(ledgerName, seq, payload.Apply.GetLog().GetData(), ledgerKnownTxIDs, ledgerRevertedTxIDs, callback)

						verifySkippedOrder(ledgerName, seq, payload.Apply.GetLog().GetData(), expectedSkippable, chainBound, hasArchivedChapters, baselineReferencesAvailable, baselineChainStateAvailable, callback)

						// Accumulate the LedgerLog.PurgedVolumes side of the
						// stored projection while we have the log in hand;
						// AppliedProposal.TransientVolumes is added in a
						// single pass below.
						for _, v := range payload.Apply.GetLog().GetPurgedVolumes() {
							addStored(ledgerName, v.GetAccount(), v.GetAsset())
						}
					}
				}
			}
		}

		// Elision guard: run at every seq (outside the payload switch) so
		// it also fires for tampered projections that never reach the
		// well-formed Apply branch above — non-Apply payloads, nil
		// Apply.Log, or nil Data — where verifySkippedOrder is unreachable.
		dispatchElisionCheck(seq, log, expectedSkippable, chainBound, hasArchivedChapters, callback)

		if ephemeralPurgeBuffer != nil && hasProposalEnd && seq == nextProposalEnd {
			if err := ephemeralPurgeBuffer.Flush(replay, ledgerAccountTypes, exclusionCollector); err != nil {
				return fmt.Errorf("flushing replay ephemeral purge at log %d: %w", seq, err)
			}

			nextProposalEnd, hasProposalEnd, err = proposalBoundaries.Next()
			if err != nil {
				return fmt.Errorf("reading next proposal log boundary: %w", err)
			}
		}

		// Emit progress periodically
		if seq%progressInterval == 0 || seq == lastSequence {
			callback(&servicepb.CheckStoreEvent{
				Type: &servicepb.CheckStoreEvent_Progress{
					Progress: &servicepb.CheckStoreProgress{
						LogsChecked: seq,
						TotalLogs:   lastSequence,
					},
				},
			})
		}
	}

	if err := logIter.Error(); err != nil {
		return fmt.Errorf("log iterator error: %w", err)
	}

	if ephemeralPurgeBuffer != nil {
		if err := ephemeralPurgeBuffer.Flush(replay, ledgerAccountTypes, exclusionCollector); err != nil {
			return fmt.Errorf("flushing final replay ephemeral purge: %w", err)
		}
	}

	// Pull the AppliedProposal.TransientVolumes side of the stored
	// projection. Combined with the LedgerLog.PurgedVolumes already
	// accumulated above, `stored` now represents the full per-ledger
	// exclusion set the index builder will consume.
	if err := c.collectStoredTransientVolumes(ctx, snap, addStored); err != nil {
		return fmt.Errorf("reading applied proposals for exclusion check: %w", err)
	}

	// Compare the stored projection against the replay-derived ground
	// truth. Mismatches indicate either:
	//   - a corrupted AppliedProposal / LedgerLog record (tampering or
	//     hardware fault on the projection caches), or
	//   - an FSM bug emitting projections that disagree with what
	//     SimulateEphemeralPurge / partitionVolumes would produce for
	//     the same orders.
	// Both turn into spurious index entries downstream, so we surface
	// them via EXCLUSION_RECORD_MISMATCH for human review.
	compareExclusionProjections(stored, excluded, callback)

	// baselineDB was opened earlier (before log iteration) so the
	// reference-fold pass and the archived-state comparison passes share
	// the same handle. If archived chapters exist but no baseline is
	// available, we can't do
	// entry-by-entry comparison (the replay only covers non-archived logs).
	// This is expected after a restore. Warn and skip comparison passes.
	if hasArchivedChapters && baselineDB == nil {
		c.logger.Info("no baseline checkpoint available for archived state comparison; skipping entry-by-entry verification")

		return nil
	}

	// `excluded` was populated incrementally by the replay-time
	// exclusionCollector. It is the audit-derived ground truth — the
	// AppliedProposal.TransientVolumes and LedgerLog.PurgedVolumes proto
	// records are intentionally NOT read here (they are not bound to the
	// audit hash chain and would let a tampered store hide live mutations
	// on otherwise-purged accounts).

	// Comparison passes: 3-way merge (baseline + replay + live).
	// When no archived chapters exist, baseline is nil and expected = replay delta only.
	c.compareVolumes(ctx, snap, baselineDB, replay, excluded, callback)
	c.compareMetadata(ctx, snap, baselineDB, replay, excluded, callback)
	c.compareTransactions(ctx, snap, baselineDB, replay, callback)
	pendingCleanups, err := query.ReadPendingLedgerCleanups(snap)
	if err != nil {
		return fmt.Errorf("reading pending ledger cleanups for index registry verification: %w", err)
	}

	pendingCleanupLedgers := make(map[string]struct{}, len(pendingCleanups))
	for name := range pendingCleanups {
		pendingCleanupLedgers[name] = struct{}{}
	}

	c.compareIndexes(snap, expectedIndexes, indexReplayActivity, deletedInReplay, hasArchivedChapters, pendingCleanupLedgers, callback)

	return nil
}

// collectStoredTransientVolumes walks the AppliedProposal stream and feeds
// every (ledger, account, asset) declared in TransientVolumes into the
// addStored callback. Paired with the LedgerLog.PurgedVolumes captured
// during the replay loop, this builds the "stored" projection the checker
// compares against the audit-derived ground truth.
func (c *Checker) collectStoredTransientVolumes(
	ctx context.Context,
	reader dal.PebbleReader,
	addStored func(ledger, account, asset string),
) error {
	proposals, err := query.ReadAppliedProposals(ctx, reader, nil)
	if err != nil {
		return fmt.Errorf("reading applied proposals: %w", err)
	}

	defer func() { _ = proposals.Close() }()

	for {
		entry, err := proposals.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}

			return fmt.Errorf("reading applied proposal: %w", err)
		}

		for ledgerName, volumeList := range entry.GetTransientVolumes() {
			for _, v := range volumeList.GetVolumes() {
				addStored(ledgerName, v.GetAccount(), v.GetAsset())
			}
		}
	}
}

// compareExclusionProjections emits one EXCLUSION_RECORD_MISMATCH event
// per (ledger, account, asset) tuple that appears in `stored` but not in
// `derived` (corruption / spurious record) and per tuple that appears in
// `derived` but not in `stored` (missing record). Identical sets emit
// nothing. The comparison is symmetric difference rather than equality so
// the report tells the operator exactly where the divergence is.
//
// Known limitation (tracked in EN-1329): the comparison is currently
// ledger-wide. Tampering that MOVES a record between two logs (for
// PurgedVolumes) or between two proposals (for TransientVolumes) of the
// same ledger cancels out in the union and is not detected here. Per-log
// / per-proposal scoping would require threading log_seq through the
// replay-time ephemeralPurgeBuffer collector — a substantial refactor
// of internal/domain/replay/replay.go that is deferred for now.
func compareExclusionProjections(stored, derived excludedVolumesSet, callback func(*servicepb.CheckStoreEvent)) {
	for ledger, set := range stored {
		ref := derived[ledger]
		for vk := range set {
			if _, ok := ref[vk]; ok {
				continue
			}

			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_EXCLUSION_RECORD_MISMATCH,
				fmt.Sprintf("exclusion record for %q/%q exists in projections (AppliedProposal/LedgerLog) but not in the replay-derived set", vk.Account, vk.Asset),
				0, ledger, vk.Account, vk.Asset,
			))
		}
	}

	for ledger, set := range derived {
		ref := stored[ledger]
		for vk := range set {
			if _, ok := ref[vk]; ok {
				continue
			}

			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_EXCLUSION_RECORD_MISMATCH,
				fmt.Sprintf("replay-derived exclusion for %q/%q is missing from projections (AppliedProposal/LedgerLog)", vk.Account, vk.Asset),
				0, ledger, vk.Account, vk.Asset,
			))
		}
	}
}

// compareIndexes emits INDEX_MISMATCH events when the stored SubAttrIndex
// registry diverges from the set the checker re-derived from the audit-bound
// CreateIndex / DropIndex / RemovedMetadataFieldType / DeleteLedger logs.
// Three failure shapes are surfaced:
//
//   - stored entry that has no matching audit-derived CreateIndex (or survives
//     a DropIndex / RemoveMetadataFieldType cascade / ledger deletion) →
//     "no matching audit entry"
//   - audit-derived expected entry that the registry never produced → "missing"
//   - identity drift: stored Ledger or Id field disagrees with the audit
//     payload that produced the entry → "diverges from audit-derived"
//
// BuildStatus is intentionally not compared: the BUILDING → READY transition
// rides on the IndexReady TechnicalUpdate, which is not part of the hash-
// chained audit. Bucket-scoped entries (LedgerName == "") are also skipped
// because no audit-chain producer exists for them today (#436 reserved).
// Drift on those is invisible to this pass until the bucket-scoped producer
// lands and threads an audit-bound order through the same machinery.
//
// Two replay-boundary cases are skipped without mismatch to mirror the
// trade-offs the rest of the checker already accepts:
//
//   - archive boundary — when a CreateIndex log lives in an archived chapter
//     the replay (which starts at archiveEndSeq+1) never repopulates the
//     expected map for it. We can detect this case ONLY by the absence of
//     replay activity for the exact key: a stored entry missing from
//     `expected` AND missing from `replayActivity` AND archives exist is
//     treated as an archive-orphan, mirroring compareIdempotencyOutcomes'
//     verifiedRangeStartTs trade-off. A stored entry that DOES appear in
//     `replayActivity` (CreateIndex/DropIndex/RemovedMetadataFieldType
//     cascade replayed) must NOT be skipped — the replay decided what the
//     projection should hold, and any divergence is tampering.
//   - pendingCleanupLedgers — the deferred Pebble range delete queued by
//     MarkLedgerForCleanup only runs when a chapter-purge range catches the
//     DeleteLedger sequence. Between apply and that purge, the stored
//     SubAttrIndex entries are still on disk while the DeleteLedger log has
//     already wiped them from expected. Skip those instead of flagging the
//     transient window.
func (c *Checker) compareIndexes(
	reader dal.PebbleReader,
	expected map[domain.IndexKey]*commonpb.Index,
	replayActivity map[domain.IndexKey]struct{},
	deletedInReplay map[string]struct{},
	hasArchivedChapters bool,
	pendingCleanupLedgers map[string]struct{},
	callback func(*servicepb.CheckStoreEvent),
) {
	iter, err := c.attrs.Index.NewStreamingIter(reader, nil)
	if err != nil {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
			fmt.Sprintf("opening index registry iterator: %v", err),
			0, "", "", "",
		))

		return
	}

	defer func() { _ = iter.Close() }()

	seen := make(map[domain.IndexKey]struct{}, len(expected))

	for iter.Next() {
		entry := iter.Entry()

		stored := entry.Value
		if stored == nil || stored.GetId() == nil {
			continue
		}

		var key domain.IndexKey
		if err := key.Unmarshal(entry.CanonicalKey); err != nil {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
				fmt.Sprintf("stored index has unparsable canonical key %x: %v", entry.CanonicalKey, err),
				0, stored.GetLedger(), "", "",
			))

			continue
		}

		// Bucket-scoped entries are produced by future audit-chain
		// producers (#436); skip them here so this pass never emits a
		// false positive on the reserved slot.
		if key.LedgerName == "" {
			continue
		}

		// Deferred-purge window: DeleteLedger's apply already wiped the
		// expected entry but the Pebble range delete queued by
		// MarkLedgerForCleanup runs only when a chapter-purge range
		// catches the delete sequence. Until then the stored entry is
		// legitimate, not corruption.
		if _, awaiting := pendingCleanupLedgers[key.LedgerName]; awaiting {
			continue
		}

		seen[key] = struct{}{}

		exp, ok := expected[key]
		if !ok {
			// Ledger was deleted in the verified replay range AND its
			// deferred Pebble purge has already run (otherwise it would
			// still appear in pendingCleanupLedgers above). Any stored
			// SubAttrIndex row for that ledger is tampering — even when
			// the per-key replayActivity guard would otherwise classify
			// it as an archive-orphan (e.g. CreateIndex pre-archive +
			// DeleteLedger post-archive: the cascade can't mark the
			// individual key because expectedIndexes never held it).
			if _, deleted := deletedInReplay[key.LedgerName]; deleted {
				callback(errorEvent(
					servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
					fmt.Sprintf("registry has Index entry for ledger %q with id %q surviving a replayed DeleteLedger + completed cleanup", key.LedgerName, key.Canonical),
					0, key.LedgerName, "", "",
				))

				continue
			}

			// Archive boundary: a stored entry missing from `expected`
			// AND never seen by the replay (no CreateIndex / DropIndex /
			// RemovedMetadataFieldType cascade for this exact key) is an
			// archive-orphan candidate — the CreateIndex log may live in
			// an archived chapter. We can only accept it as such when
			// archives are known to exist; otherwise (no archives at all)
			// any unmatched stored entry is a hard mismatch.
			if hasArchivedChapters {
				if _, hadActivity := replayActivity[key]; !hadActivity {
					continue
				}
			}

			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
				fmt.Sprintf("registry has Index entry for ledger %q with id %q that has no matching CreateIndex in the audit chain", key.LedgerName, key.Canonical),
				0, key.LedgerName, "", "",
			))

			continue
		}

		if stored.GetLedger() != exp.GetLedger() {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
				fmt.Sprintf("Index entry for ledger %q id %q: stored Ledger=%q diverges from audit-derived Ledger=%q",
					key.LedgerName, key.Canonical, stored.GetLedger(), exp.GetLedger()),
				0, key.LedgerName, "", "",
			))
		}

		if !indexes.Equal(stored.GetId(), exp.GetId()) {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
				fmt.Sprintf("Index entry for ledger %q id %q: stored Id diverges from audit-derived",
					key.LedgerName, key.Canonical),
				0, key.LedgerName, "", "",
			))
		}
	}

	if err := iter.Err(); err != nil {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
			fmt.Sprintf("scanning index registry: %v", err),
			0, "", "", "",
		))
	}

	for key := range expected {
		if _, ok := seen[key]; ok {
			continue
		}

		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INDEX_MISMATCH,
			fmt.Sprintf("audit chain expects Index entry for ledger %q id %q but the registry has no matching row",
				key.LedgerName, key.Canonical),
			0, key.LedgerName, "", "",
		))
	}
}

// excludedVolumesSet maps ledger name to a set of (account, asset) tuples
// that legitimately diverge between the replay store and the live Pebble
// store. The set is populated incrementally by the replay-time
// exclusionCollector in Check() — i.e. derived from the audit log (the
// only hash-chain-bound source). AppliedProposal.TransientVolumes and
// LedgerLog.PurgedVolumes are NOT consulted: they are caches for the
// index builder and cannot be trusted by the integrity checker.
type excludedVolumesSet map[string]map[domain.AccountAssetKey]struct{}

func (e excludedVolumesSet) contains(ledgerName, account, asset string) bool {
	if e == nil {
		return false
	}

	keys, ok := e[ledgerName]
	if !ok {
		return false
	}

	_, has := keys[domain.AccountAssetKey{Account: account, Asset: asset}]

	return has
}

// containsAccount returns true when any asset of the given account is in
// the exclusion set. Used by compareMetadata which is keyed per account,
// not per (account, asset).
func (e excludedVolumesSet) containsAccount(ledgerName, account string) bool {
	if e == nil {
		return false
	}

	for k := range e[ledgerName] {
		if k.Account == account {
			return true
		}
	}

	return false
}

// compareVolumes performs a 3-way merge comparison for volumes.
// expected = baseline + replay delta; compare with live (actual).
// `excluded` lists per-ledger accounts whose volumes legitimately diverge
// (transient + purged ephemeral, sourced from the audit log).
func (c *Checker) compareVolumes(ctx context.Context, reader dal.PebbleReader, baselineDB *pebble.DB, replay *replayStore, excluded excludedVolumesSet, callback func(*servicepb.CheckStoreEvent)) int {
	errorCount := 0

	// Collect live volumes
	liveVolumes := make(map[string]*raftcmdpb.VolumePair)

	liveIter, err := c.attrs.Volume.NewStreamingIter(reader, nil)
	if err != nil {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
			fmt.Sprintf("failed to create live volume iterator: %v", err), 0, "", "", ""))

		return 1
	}

	for liveIter.Next() {
		e := liveIter.Entry()
		liveVolumes[string(e.CanonicalKey)] = e.Value
	}

	if err := liveIter.Close(); err != nil {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
			fmt.Sprintf("closing live volume iterator: %v", err), 0, "", "", ""))

		return 1
	}

	if err := liveIter.Err(); err != nil {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
			fmt.Sprintf("live volume iterator error: %v", err), 0, "", "", ""))

		return 1
	}

	// Collect baseline volumes (if available)
	baselineVolumes := make(map[string]*raftcmdpb.VolumePair)

	if baselineDB != nil {
		baselineIter, err := c.attrs.Volume.NewStreamingIter(baselineDB, nil)
		if err != nil {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
				fmt.Sprintf("failed to create baseline volume iterator: %v", err), 0, "", "", ""))

			return 1
		}

		for baselineIter.Next() {
			e := baselineIter.Entry()
			baselineVolumes[string(e.CanonicalKey)] = e.Value
		}

		if err := baselineIter.Close(); err != nil {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
				fmt.Sprintf("closing baseline volume iterator: %v", err), 0, "", "", ""))

			return 1
		}

		if err := baselineIter.Err(); err != nil {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
				fmt.Sprintf("baseline volume iterator error: %v", err), 0, "", "", ""))

			return 1
		}
	}

	// Collect replay volume deltas
	replayDeltas := make(map[string]*raftcmdpb.VolumePair)

	replayIter, err := replay.newPrefixIter(replayPrefixVolume)
	if err != nil {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
			fmt.Sprintf("failed to create replay volume iterator: %v", err), 0, "", "", ""))

		return 1
	}

	for replayIter.First(); replayIter.Valid(); replayIter.Next() {
		canonicalKey := replayIter.Key()[1:] // strip prefix byte

		valBytes, valErr := replayIter.ValueAndErr()
		if valErr != nil {
			_ = replayIter.Close()

			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
				fmt.Sprintf("reading replay volume: %v", valErr), 0, "", "", ""))

			return 1
		}

		pair := &raftcmdpb.VolumePair{}
		if err := pair.UnmarshalVT(valBytes); err != nil {
			_ = replayIter.Close()

			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
				fmt.Sprintf("unmarshaling replay volume: %v", err), 0, "", "", ""))

			return 1
		}

		replayDeltas[string(canonicalKey)] = pair
	}

	if err := replayIter.Close(); err != nil {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
			fmt.Sprintf("closing replay volume iterator: %v", err), 0, "", "", ""))

		return 1
	}

	// Collect all keys
	allKeys := make(map[string]struct{})
	for k := range liveVolumes {
		allKeys[k] = struct{}{}
	}

	for k := range baselineVolumes {
		allKeys[k] = struct{}{}
	}

	for k := range replayDeltas {
		allKeys[k] = struct{}{}
	}

	// Compare: expected = baseline + delta
	for key := range allKeys {
		if ctx.Err() != nil {
			return errorCount
		}

		var vk domain.VolumeKey

		if err := vk.Unmarshal([]byte(key)); err != nil {
			continue
		}

		// Compute expected input/output
		expectedInput := big.NewInt(0)
		expectedOutput := big.NewInt(0)

		if base := baselineVolumes[key]; base != nil {
			expectedInput = base.GetInput().ToBigInt()
			expectedOutput = base.GetOutput().ToBigInt()
		}

		if delta := replayDeltas[key]; delta != nil {
			expectedInput.Add(expectedInput, delta.GetInput().ToBigInt())
			expectedOutput.Add(expectedOutput, delta.GetOutput().ToBigInt())
		}

		// Get actual
		actualInput := big.NewInt(0)
		actualOutput := big.NewInt(0)

		if actual := liveVolumes[key]; actual != nil {
			actualInput = actual.GetInput().ToBigInt()
			actualOutput = actual.GetOutput().ToBigInt()
		}

		// Skip volumes the replay-time ephemeral-purge collector recorded
		// during this Check() run (see exclusionCollector at the top of
		// Check). That set is derived from the hash-chain-bound audit
		// trail — NOT from AppliedProposal.TransientVolumes or
		// LedgerLog.PurgedVolumes, which are unhashed caches and must
		// stay untrusted here. The exclusion key is (account, asset) so a
		// multi-asset account whose USD was purged still has its EUR
		// compared. Do not "align" this code to consult those proto
		// records — it would reintroduce the tampering vector this
		// design deliberately removes.
		if excluded.contains(vk.LedgerName, vk.Account, vk.Asset) {
			continue
		}

		if expectedInput.Cmp(actualInput) != 0 {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
				fmt.Sprintf("input mismatch for %s/%s: expected %s, got %s",
					vk.Account, vk.Asset, expectedInput.String(), actualInput.String()),
				0, vk.LedgerName, vk.Account, vk.Asset))

			errorCount++
		}

		if expectedOutput.Cmp(actualOutput) != 0 {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
				fmt.Sprintf("output mismatch for %s/%s: expected %s, got %s",
					vk.Account, vk.Asset, expectedOutput.String(), actualOutput.String()),
				0, vk.LedgerName, vk.Account, vk.Asset))

			errorCount++
		}
	}

	return errorCount
}

// compareMetadata performs a 3-way merge comparison for account metadata.
// Replay entries encode SET (flag 0x00 + value) or DELETED (flag 0x01).
// expected = replay override if present, else baseline; compare with live.
// `excluded` lists per-ledger accounts whose state legitimately diverges
// (transient + purged ephemeral, sourced from the audit log) — metadata on
// such accounts is skipped to avoid false positives.
func (c *Checker) compareMetadata(ctx context.Context, reader dal.PebbleReader, baselineDB *pebble.DB, replay *replayStore, excluded excludedVolumesSet, callback func(*servicepb.CheckStoreEvent)) int {
	errorCount := 0

	// Collect live metadata
	liveMetadata := make(map[string]string)

	liveIter, err := c.attrs.Metadata.NewStreamingIter(reader, nil)
	if err != nil {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
			fmt.Sprintf("failed to create live metadata iterator: %v", err), 0, "", "", ""))

		return 1
	}

	for liveIter.Next() {
		e := liveIter.Entry()
		liveMetadata[string(e.CanonicalKey)] = commonpb.MetadataValueToString(e.Value)
	}

	if err := liveIter.Close(); err != nil {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
			fmt.Sprintf("closing live metadata iterator: %v", err), 0, "", "", ""))

		return 1
	}

	if err := liveIter.Err(); err != nil {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
			fmt.Sprintf("live metadata iterator error: %v", err), 0, "", "", ""))

		return 1
	}

	// Collect baseline metadata (if available)
	baselineMetadata := make(map[string]string) // key -> string value

	if baselineDB != nil {
		baselineIter, err := c.attrs.Metadata.NewStreamingIter(baselineDB, nil)
		if err != nil {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
				fmt.Sprintf("failed to create baseline metadata iterator: %v", err), 0, "", "", ""))

			return 1
		}

		for baselineIter.Next() {
			e := baselineIter.Entry()
			baselineMetadata[string(e.CanonicalKey)] = commonpb.MetadataValueToString(e.Value)
		}

		if err := baselineIter.Close(); err != nil {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
				fmt.Sprintf("closing baseline metadata iterator: %v", err), 0, "", "", ""))

			return 1
		}

		if err := baselineIter.Err(); err != nil {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
				fmt.Sprintf("baseline metadata iterator error: %v", err), 0, "", "", ""))

			return 1
		}
	}

	// Collect replay metadata state
	type replayMeta struct {
		deleted bool
		value   string // only valid when !deleted
	}

	replayEntries := make(map[string]replayMeta)

	replayIter, err := replay.newPrefixIter(replayPrefixMetadata)
	if err != nil {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
			fmt.Sprintf("failed to create replay metadata iterator: %v", err), 0, "", "", ""))

		return 1
	}

	for replayIter.First(); replayIter.Valid(); replayIter.Next() {
		canonicalKey := replayIter.Key()[1:] // strip prefix byte

		valBytes, valErr := replayIter.ValueAndErr()
		if valErr != nil {
			_ = replayIter.Close()

			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
				fmt.Sprintf("reading replay metadata: %v", valErr), 0, "", "", ""))

			return 1
		}

		if len(valBytes) == 0 {
			continue
		}

		if valBytes[0] == metaFlagDeleted {
			replayEntries[string(canonicalKey)] = replayMeta{deleted: true}
		} else {
			replayEntries[string(canonicalKey)] = replayMeta{value: string(valBytes[1:])}
		}
	}

	if err := replayIter.Close(); err != nil {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
			fmt.Sprintf("closing replay metadata iterator: %v", err), 0, "", "", ""))

		return 1
	}

	// Collect all keys
	allKeys := make(map[string]struct{})
	for k := range liveMetadata {
		allKeys[k] = struct{}{}
	}

	for k := range baselineMetadata {
		allKeys[k] = struct{}{}
	}

	for k := range replayEntries {
		allKeys[k] = struct{}{}
	}

	// Compare
	for key := range allKeys {
		if ctx.Err() != nil {
			return errorCount
		}

		var mk domain.MetadataKey

		if err := mk.Unmarshal([]byte(key)); err != nil {
			continue
		}

		// Metadata is keyed per account (no asset dimension). Skip when any
		// asset of the account is in the exclusion set — conservative: if a
		// single asset is transient/purged we assume the metadata diverges.
		if excluded.containsAccount(mk.LedgerName, mk.Account) {
			continue
		}

		// Compute expected value
		var expectedValue string
		expectedExists := false

		if rm, hasReplay := replayEntries[key]; hasReplay {
			if !rm.deleted {
				expectedValue = rm.value
				expectedExists = true
			}
			// If deleted by replay, expectedExists stays false
		} else if baseVal, hasBase := baselineMetadata[key]; hasBase {
			expectedValue = baseVal
			expectedExists = true
		}

		// Get actual
		actualValue, actualExists := liveMetadata[key]

		if expectedExists != actualExists {
			if expectedExists {
				callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
					fmt.Sprintf("metadata missing for %s/%s: expected %q",
						mk.Account, mk.Key, expectedValue),
					0, mk.LedgerName, mk.Account, ""))
			} else {
				callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
					fmt.Sprintf("unexpected metadata for %s/%s: got %q",
						mk.Account, mk.Key, actualValue),
					0, mk.LedgerName, mk.Account, ""))
			}

			errorCount++
		} else if expectedExists && expectedValue != actualValue {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
				fmt.Sprintf("metadata mismatch for %s/%s: expected %q, got %q",
					mk.Account, mk.Key, expectedValue, actualValue),
				0, mk.LedgerName, mk.Account, ""))

			errorCount++
		}
	}

	return errorCount
}

// compareTransactions performs a 3-way merge comparison for transaction states.
// expected = replay override if present, else baseline; compare with live.
//
// Compared to compareVolumes / compareMetadata, this pass historically only
// iterated replay ∪ baseline, so a transaction present in the live store
// without a matching log entry (fabricated state, corruption, FSM bug) went
// undetected. The fix in #347 widens allKeys to the union with live and
// instruments every abort path with an error event so that swallowed
// iterator/unmarshal failures cannot make the check look clean.
func (c *Checker) compareTransactions(ctx context.Context, reader dal.PebbleReader, baselineDB *pebble.DB, replay *replayStore, callback func(*servicepb.CheckStoreEvent)) int {
	errorCount := 0

	emitErr := func(msg string) {
		callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH, msg, "", 0))
	}

	// Collect live transaction states up-front so that fabricated entries
	// (live without replay/baseline) are part of allKeys.
	liveTx := make(map[string]*commonpb.TransactionState)

	liveIter, err := c.attrs.Transaction.NewStreamingIter(reader, nil)
	if err != nil {
		emitErr(fmt.Sprintf("failed to create live transaction iterator: %v", err))

		return 1
	}

	for liveIter.Next() {
		e := liveIter.Entry()
		liveTx[string(e.CanonicalKey)] = e.Value
	}

	if err := liveIter.Close(); err != nil {
		emitErr(fmt.Sprintf("closing live transaction iterator: %v", err))

		return 1
	}

	if err := liveIter.Err(); err != nil {
		emitErr(fmt.Sprintf("live transaction iterator error: %v", err))

		return 1
	}

	// Collect replay transaction states.
	replayTx := make(map[string]*commonpb.TransactionState)

	replayIter, err := replay.newPrefixIter(replayPrefixTransaction)
	if err != nil {
		emitErr(fmt.Sprintf("failed to create replay transaction iterator: %v", err))

		return 1
	}

	for replayIter.First(); replayIter.Valid(); replayIter.Next() {
		canonicalKey := replayIter.Key()[1:]

		valBytes, valErr := replayIter.ValueAndErr()
		if valErr != nil {
			_ = replayIter.Close()
			emitErr(fmt.Sprintf("reading replay transaction value: %v", valErr))

			return 1
		}

		// Values are prefixed with txOpFinalized tag from the merger's Finish output.
		if len(valBytes) == 0 || valBytes[0] != 0x00 {
			_ = replayIter.Close()
			emitErr(fmt.Sprintf("malformed replay transaction tag at key %x", canonicalKey))

			return 1
		}

		state := &commonpb.TransactionState{}
		if err := state.UnmarshalVT(valBytes[1:]); err != nil {
			_ = replayIter.Close()
			emitErr(fmt.Sprintf("unmarshaling replay transaction at key %x: %v", canonicalKey, err))

			return 1
		}

		replayTx[string(canonicalKey)] = state
	}

	if err := replayIter.Close(); err != nil {
		emitErr(fmt.Sprintf("closing replay transaction iterator: %v", err))

		return 1
	}

	// Collect baseline transaction states (if available)
	baselineTx := make(map[string]*commonpb.TransactionState)

	if baselineDB != nil {
		baselineIter, err := c.attrs.Transaction.NewStreamingIter(baselineDB, nil)
		if err != nil {
			emitErr(fmt.Sprintf("failed to create baseline transaction iterator: %v", err))

			return 1
		}

		for baselineIter.Next() {
			e := baselineIter.Entry()
			baselineTx[string(e.CanonicalKey)] = e.Value
		}

		if err := baselineIter.Close(); err != nil {
			emitErr(fmt.Sprintf("closing baseline transaction iterator: %v", err))

			return 1
		}

		if baselineIter.Err() != nil {
			emitErr(fmt.Sprintf("baseline transaction iterator error: %v", baselineIter.Err()))

			return 1
		}
	}

	// Collect all keys to check: replay ∪ baseline ∪ live.
	allKeys := make(map[string]struct{})
	for k := range replayTx {
		allKeys[k] = struct{}{}
	}

	for k := range baselineTx {
		allKeys[k] = struct{}{}
	}

	for k := range liveTx {
		allKeys[k] = struct{}{}
	}

	for key := range allKeys {
		if ctx.Err() != nil {
			return errorCount
		}

		var tk domain.TransactionKey
		if err := tk.Unmarshal([]byte(key)); err != nil {
			continue
		}

		// Expected: replay overrides baseline. Stays nil when only the live
		// store has the entry (fabricated/corrupted state).
		var expected *commonpb.TransactionState
		if rs, ok := replayTx[key]; ok {
			expected = rs
		} else if bs, ok := baselineTx[key]; ok {
			expected = bs
		}

		actualState := liveTx[key]

		if expected == nil {
			if actualState != nil {
				callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH,
					fmt.Sprintf("unexpected transaction in live store for tx %d (no matching log or baseline)", tk.ID),
					tk.LedgerName, tk.ID))

				errorCount++
			}

			continue
		}

		if actualState == nil {
			callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH,
				fmt.Sprintf("transaction state missing for tx %d", tk.ID),
				tk.LedgerName, tk.ID))

			errorCount++

			continue
		}

		// Normalize empty metadata map to nil so that proto.Equal does not
		// treat nil vs empty map as a mismatch.
		// todo: this should be handled at source
		normalizeTransactionState(expected)
		normalizeTransactionState(actualState)

		if !proto.Equal(expected, actualState) {
			callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH,
				fmt.Sprintf("transaction state mismatch for tx %d: expected %s, got %s",
					tk.ID, expected.String(), actualState.String()),
				tk.LedgerName, tk.ID))

			errorCount++
		}
	}

	return errorCount
}

// verifyAuditHashChain iterates all non-archived audit entries, recomputes
// each hash from the stored orders, and verifies the chain starting from
// archiveLastAuditHash. Reports CHECK_STORE_ERROR_TYPE_HASH_MISMATCH on
// the first mismatch.
//
// Archived audit entries have been purged from Pebble, so the chain starts
// at archiveLastAuditHash (from the latest archived chapter) or nil if no
// chapters have been archived.
//
// chainBound is passed in by the caller (already seeded from the baseline
// checkpoint); this function layers the live audit-chain mutations onto it:
// per-ledger references, reverted tx-ids, metadata / account-type timelines,
// nextTxID counters, and ledgerCreationSeen. It is built from chain-bound
// `serialized_order` so a tampered LedgerLog projection cannot forge a
// "prior claim" for a fake skip.
//
// Returns expectedSkippable: the per-log-seq skippable_reasons whitelist +
// reason correlator re-derived from the chain-bound Orders, consumed by
// verifySkippedOrder.
//
// The LedgerLog projection is not hash-chain bound, so without these
// checks a tampered skip log could let a fabricated outcome slip past
// Check().
func (c *Checker) verifyAuditHashChain(
	ctx context.Context,
	reader dal.PebbleReader,
	chapters []*commonpb.Chapter,
	archiveLastAuditHash []byte,
	chainBound *chainBoundState,
	idempotencyTTLMicros *uint64,
	callback func(*servicepb.CheckStoreEvent),
) (map[uint64]*expectedSkippableOrder, error) {
	// Find the last archived audit sequence to start iteration after it.
	//
	// CloseAuditSequence is the last audit entry written BEFORE the CloseChapter
	// proposal. Purging deletes entries [start, CloseAuditSequence], so the
	// CloseChapter entry at CloseAuditSequence + 1 survives and is the first
	// entry we verify. chapter.LastAuditHash is the hash of the predecessor
	// (entry at CloseAuditSequence), which is the chain input for verifying
	// the surviving entry.
	var afterAuditSeq *uint64

	for _, p := range chapters {
		if p.GetStatus() == commonpb.ChapterStatus_CHAPTER_ARCHIVED {
			closeAuditSeq := p.GetCloseAuditSequence()
			if afterAuditSeq == nil || closeAuditSeq > *afterAuditSeq {
				afterAuditSeq = &closeAuditSeq
			}
		}
	}

	auditCursor, err := query.ReadAuditEntries(ctx, reader, afterAuditSeq)
	if err != nil {
		return nil, fmt.Errorf("reading audit entries: %w", err)
	}

	defer func() { _ = auditCursor.Close() }()

	var (
		lastHash   = archiveLastAuditHash
		hashBuf    []byte
		checked    uint64
		generators = make(map[uint32]processing.HashGenerator, 2)
		// Frozen idempotency outcomes the projection should hold, re-derived
		// from each verified audit entry and compared to SubIdempKeys below.
		expectedIdem = make(map[idemExpectedKey]expectedIdempotency)
		// Per-log-sequence skippable_reasons whitelist plus reason-specific
		// correlator (e.g. reference for TRANSACTION_REFERENCE_CONFLICT),
		// re-derived from the chain-bound Order. Consumed by
		// verifySkippedOrder during the log iteration loop.
		expectedSkippable = make(map[uint64]*expectedSkippableOrder)
		// chainBound is passed in by the caller — Check() pre-folds the
		// baseline snapshot into it so ledgers whose CreateLedger sits
		// in an archived chapter have their nextTxID counter and
		// ledgerCreationSeen anchor set BEFORE live replay attributes
		// metadata to tx-id targets.
		//
		// hasVerifiedRange records whether any entry was verified; a dedicated
		// bool rather than a 0-sentinel, since HLC timestamp 0 is a legitimate
		// value (mirrors the *uint64 idemReportFloor tri-state below).
		hasVerifiedRange bool
		// Timestamps of the first (lowest-sequence) and last (highest-sequence)
		// verified entries. HLC timestamps are monotonic with sequence, so the
		// first is the archive boundary (default idempotency report floor) and
		// the last is the hash-chain-verified "now" used to size the TTL window.
		verifiedRangeStartTs uint64
		verifiedRangeEndTs   uint64
	)

	for {
		entry, err := auditCursor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			// Real iterator/unmarshal failure: surface it. A silent break
			// here would let a corrupted audit entry partially-verify the
			// hash chain and report "no mismatch" even though entries
			// past the failure point were never checked.
			return nil, fmt.Errorf("reading audit entry for hash chain verification: %w", err)
		}

		if !hasVerifiedRange {
			hasVerifiedRange = true
			verifiedRangeStartTs = entry.GetTimestamp().GetData()
		}

		// Entries arrive in ascending sequence (hence ascending HLC), so the
		// last one seen carries the highest verified timestamp.
		verifiedRangeEndTs = entry.GetTimestamp().GetData()

		// `items` on the stored AuditEntry value is reserved for
		// GetAuditEntry response shaping — the apply path forces it
		// nil. A non-empty list here is a tampering attempt: items
		// smuggled into the entry value would be returned by
		// ListAuditEntries / GetAuditEntry without being bound by the
		// chain (the chain hashes the items from their own keys, not
		// this list). Flag and stop.
		if len(entry.GetItems()) > 0 {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH,
				fmt.Sprintf("audit entry %d carries %d embedded items in its persisted value; entry.items must be nil on disk",
					entry.GetSequence(), len(entry.GetItems())),
				logSequenceFromAuditEntry(entry), "", "", "",
			))

			// Return the partially populated maps (not nil) so
			// foldBaselineReferences does not panic on a nil-map write.
			// Check() keeps running after a chain break to surface
			// other projection errors.
			return expectedSkippable, nil
		}

		// Read the audit items for this entry, then rebuild the canonical
		// per-item bytes that fed the hash chain at apply time. Combined
		// with the rebuilt header payload (which binds every other
		// AuditEntry field via state.BuildHashedHeaderPayload), the hash
		// pre-image is reconstructed from the stored fields — no proto
		// re-marshalling, immune to vtprotobuf and Order schema drift.
		items, itemsErr := query.ReadAuditItems(ctx, reader, entry.GetSequence())
		if itemsErr != nil {
			return nil, fmt.Errorf("reading audit items for sequence %d: %w", entry.GetSequence(), itemsErr)
		}

		headerPayload, headerErr := state.BuildHashedHeaderPayload(entry)
		if headerErr != nil {
			// A persisted entry that fails to rebuild its own header
			// is either tampered (e.g. outcome wiped) or pre-dated a
			// schema change. Either way the chain is unreproducible
			// from here, so emit a mismatch and stop.
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH,
				fmt.Sprintf("audit entry %d cannot be re-hashed: %v", entry.GetSequence(), headerErr),
				logSequenceFromAuditEntry(entry), "", "", "",
			))

			// Return the partially populated maps (not nil) so
			// foldBaselineReferences does not panic on a nil-map write.
			// Check() keeps running after a chain break to surface
			// other projection errors.
			return expectedSkippable, nil
		}

		hashSlices := make([][]byte, 0, 1+len(items))
		hashSlices = append(hashSlices, headerPayload)

		for _, item := range items {
			hashSlices = append(hashSlices, state.BuildPerItemPayload(item))
		}

		// Recompute the hash using a generator matching the entry's stored
		// algorithm version. Lazily cached per version (~2 entries max).
		version := entry.GetHashVersion()

		gen, ok := generators[version]
		if !ok {
			gen = processing.NewHashGenerator(commonpb.HashAlgorithm(version), c.clusterID)
			generators[version] = gen
		}

		var computed []byte
		hashBuf, computed = gen.Compute(hashBuf, lastHash, hashSlices)

		if !bytes.Equal(computed, entry.GetHash()) {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH,
				fmt.Sprintf("audit hash chain broken at sequence %d: stored=%x computed=%x",
					entry.GetSequence(), entry.GetHash(), computed),
				logSequenceFromAuditEntry(entry), "", "", "",
			))

			// Return the partially populated maps (not nil) so
			// foldBaselineReferences does not panic on a nil-map write.
			// Check() keeps running after a chain break to surface
			// other projection errors.
			return expectedSkippable, nil
		}

		lastHash = entry.GetHash()
		checked++

		// Now that the entry is chain-verified, re-derive the idempotency
		// outcome a keyed proposal would have frozen under it. items carries
		// the serialized orders, reused to recompute the proposal hash.
		if key := entry.GetIdempotency().GetKey(); key != "" {
			if exp, ok := expectedIdempotencyOutcome(entry, items); ok {
				expectedIdem[idemExpectedKey{
					keyHash:   state.HashIdempotencyKey(key),
					createdAt: entry.GetTimestamp().GetData(),
				}] = exp
			}
		}

		// Per-order skippable_reasons whitelist + chain-bound references
		// re-derived from each chain-verified order. Each item carries its
		// own LogSequence (set by buildAuditItems from the fresh
		// CreatedLog or the idempotency replay's ReferenceSequence).
		// Failure-side entries get LogSequence=0 and contribute nothing.
		if entry.GetSuccess() != nil {
			collectExpectedSkippable(items, expectedSkippable, chainBound)
		}
	}

	if checked > 0 {
		c.logger.Infof("Audit hash chain verified: %d entries checked", checked)
	}

	// idemReportFloor is the lowest created_at at/above which `expectedIdem` is
	// complete, so an unmatched stored entry there is tampering rather than an
	// un-re-derivable archived freeze. It is a pointer so a floor of 0 ("the TTL
	// window is unbounded — report every entry") is distinct from "no verified
	// range at all" (nil — report nothing).
	//
	// The post-archive (verified) range always covers [verifiedRangeStartTs, ∞).
	// When the TTL window reaches before that boundary, the still-live archived
	// freezes in [cutoff, boundary) are re-derived from cold storage; if that
	// succeeds the floor drops to cutoff. If cold storage is unavailable the
	// floor stays at the boundary — the residual gap, not a false positive.
	//
	// "now" is verifiedRangeEndTs — the highest hash-chain-verified timestamp —
	// not a Pebble projection, so a tampered lastAppliedTimestamp cannot shrink
	// the window. The TTL itself is still a projection (see idempotencyTTLMicros).
	var idemReportFloor *uint64

	if hasVerifiedRange {
		idemReportFloor = &verifiedRangeStartTs

		if idempotencyTTLMicros == nil {
			// PersistedConfig absent: the window is unknown. Skip the cold pass
			// rather than treat it as never-expire — distinct from a genuine
			// cold-storage read failure below.
			c.logger.Debug("persisted idempotency TTL unavailable; verifying only the post-archive idempotency range")
		} else if cutoff := idempotencyWindowCutoff(verifiedRangeEndTs, *idempotencyTTLMicros); cutoff < verifiedRangeStartTs {
			if c.reDeriveArchivedIdempotency(ctx, chapters, cutoff, expectedIdem) {
				idemReportFloor = &cutoff
			} else {
				c.logger.Info("idempotency TTL window extends before the archive boundary but archived audit entries are not readable; verifying only the post-archive range")
			}
		}
	}

	if err := c.compareIdempotencyOutcomes(reader, expectedIdem, idemReportFloor, callback); err != nil {
		return nil, err
	}

	return expectedSkippable, nil
}

// chainBoundState aggregates the audit-derived state the verifier consults
// to prove each per-reason skip is legitimate. Every entry is populated by
// collectExpectedSkippable during the audit chain scan — projections
// (LedgerLog) never influence any of these maps, so a tampered log cannot
// forge a false "underlying condition was true" answer.
//
//   - references: per-ledger first-seen sequence of every audit-bound
//     CreateTransactionOrder reference (regardless of outcome). Verifies the
//     "prior claim" replay for TRANSACTION_REFERENCE_CONFLICT.
//
//   - reverted: per-ledger first-seen sequence of every audit-bound
//     RevertTransactionOrder that produced a Success outcome (a successful
//     revert; skipped ALREADY_REVERTED reverts are also recorded because
//     they imply a prior success — see collectExpectedSkippable). Verifies
//     "was tx already reverted at seq X?" for TRANSACTION_ALREADY_REVERTED.
//
//   - metadata: per-(ledger, target, key) chronological list of state
//     transitions (SaveMetadata → exists=true, DeleteMetadata → exists=false).
//     The verifier queries "was the key absent just before seq X?" for
//     METADATA_NOT_FOUND via the last transition strictly before X.
//
//   - accountTypes: per-(ledger, name) chronological list of state
//     transitions (AddAccountType → exists=true, RemoveAccountType →
//     exists=false). Verifies presence/absence at seq for
//     ACCOUNT_TYPE_ALREADY_EXISTS / ACCOUNT_TYPE_NOT_FOUND.
type chainBoundState struct {
	references map[string]map[string]uint64
	// referenceTxIDs mirrors references (ledger → reference → owning
	// transaction id) with the SAME first-claim-wins semantic, so the
	// verifier can re-derive and pin OrderSkipped.context["existingTransactionId"]
	// against audit-bound data. Every reference-claim source carries the
	// owning tx id: a live CreateTransaction gets it from the chain-bound
	// counter (allocateChainBoundTxID), a mirror ingest from
	// MirrorCreatedTransaction.transaction_id, and the baseline archive
	// fold from TransactionReferenceValue.transaction_id. An id of 0 means
	// "claim seen but owning tx id not derivable" (e.g. a live claim whose
	// CreateTransaction sits behind an unanchored counter) — the verifier
	// stays permissive in that case, matching the archive-escape philosophy
	// of the seq map. See finding EN-1356 existingTransactionId (invariant #8).
	referenceTxIDs map[string]map[string]uint64
	reverted       map[string]map[uint64]uint64
	metadata       map[string]map[string]map[string][]chainBoundMutation
	accountTypes   map[string]map[string][]chainBoundMutation
	// nextTxID mirrors LedgerBoundaries.NextTransactionId per ledger,
	// bumped on every SUCCESSFUL CreateTransactionOrder / RevertTransactionOrder
	// the chain scan replays (skipped orders don't consume a slot). Used
	// to derive the tx id an order's transaction-scoped metadata
	// (CreateTransaction.metadata, RevertTransaction.metadata) actually
	// targeted, so a later DeleteMetadata(target=<that tx id>) skip
	// check can see the presence. Only anchored when the ledger's
	// CreateLedger order was observed in live (see ledgerCreationSeen);
	// for ledgers whose CreateLedger sits in an archived chapter, the
	// counter is UNRELIABLE and tx-scoped metadata is not recorded.
	nextTxID map[string]uint64
	// ledgerCreationSeen names ledgers whose CreateLedger order was
	// witnessed in the live audit range — the guarantee that nextTxID
	// is anchored to the FSM's true counter. For ledgers absent from
	// this set, tx-scoped metadata mutations (CreateTransaction.metadata,
	// RevertTransaction.metadata) are NOT recorded (a fake tx id would
	// mismatch the client's DeleteMetadata target and either false-
	// positive or false-negative the skip check). The verifier consults
	// this set to stay permissive on tx-id-scoped METADATA_NOT_FOUND
	// skips for archived-history ledgers.
	ledgerCreationSeen map[string]struct{}
}

// chainBoundMutation records one presence-flip observed on the audit
// chain. Order-of-append matches order-of-seq (collectExpectedSkippable
// iterates items in seq order), so the last entry with seq < X gives the
// state as of X's predecessor.
type chainBoundMutation struct {
	seq    uint64
	exists bool
}

// newChainBoundState allocates the four maps with empty top-level entries;
// per-ledger sub-maps are added on demand as collectExpectedSkippable
// observes each ledger.
func newChainBoundState() *chainBoundState {
	return &chainBoundState{
		references:         make(map[string]map[string]uint64),
		referenceTxIDs:     make(map[string]map[string]uint64),
		reverted:           make(map[string]map[uint64]uint64),
		metadata:           make(map[string]map[string]map[string][]chainBoundMutation),
		accountTypes:       make(map[string]map[string][]chainBoundMutation),
		nextTxID:           make(map[string]uint64),
		ledgerCreationSeen: make(map[string]struct{}),
	}
}

// mutationStateAtSeq walks a mutation list backwards and returns the
// exists flag of the latest entry strictly before seq. Returns false
// when the list is empty or every entry is at/after seq (the key was
// never mutated before seq → default absent). Linear scan is fine
// because per-key mutation lists are bounded by the number of user
// operations on that key.
func mutationStateAtSeq(muts []chainBoundMutation, seq uint64) bool {
	state, _ := mutationStateWithWitness(muts, seq)

	return state
}

// mutationStateWithWitness returns both the state at seq AND whether
// the live audit range witnessed any mutation before seq. The witness
// flag lets callers distinguish "positive evidence of absence" (a
// live Delete before seq) from "no live evidence at all" (empty
// timeline may still be a Set-in-archive). The forward verifier
// treats a positive presence witness as authoritative (archives
// cannot undo a live Set). The inverse verifier is more
// conservative: empty-live + archives = ambiguous → permissive.
func mutationStateWithWitness(muts []chainBoundMutation, seq uint64) (state, witnessed bool) {
	for _, v := range slices.Backward(muts) {
		if v.seq < seq {
			return v.exists, true
		}
	}

	return false, false
}

// expectedSkippableOrder captures the chain-verified fields the checker
// needs to confirm a LedgerLogPayload.OrderSkipped projection is legitimate:
// the audit-bound reasons whitelist (any skip must use one of these) plus
// the per-reason correlator the verifier compares against the projection's
// context map. Every field is derived from the chain-bound Order — a
// tampered LedgerLog projection cannot influence any of them.
//
// Each correlator is only populated when the containing LedgerAction is the
// natural producer of the corresponding reason (e.g. transactionID is set
// for a chain-bound RevertTransactionOrder, empty otherwise). The
// per-reason branches in verifySkippedOrder pick the field they need and
// ignore the others.
type expectedSkippableOrder struct {
	reasons []commonpb.ErrorReason
	ledger  string
	// TRANSACTION_REFERENCE_CONFLICT correlator: reference the order declared.
	reference string
	// TRANSACTION_ALREADY_REVERTED correlator: transaction id the revert
	// targeted (chain-bound Order.RevertTransaction.TransactionId).
	transactionID uint64
	// METADATA_NOT_FOUND correlator: same (target, key) tuple the sub-
	// processor stamps on ErrMetadataNotFound.
	metadataTarget string
	metadataKey    string
	// ACCOUNT_TYPE_ALREADY_EXISTS / ACCOUNT_TYPE_NOT_FOUND correlator:
	// account type name from the AddAccountType / RemoveAccountType order.
	accountTypeName string
}

// collectExpectedSkippable populates expectedSkippable and the four
// chainBoundState maps from the chain-verified items of a successful
// audit entry. Every entry consumes item.SerializedOrder — a projection
// (LedgerLog) is never trusted, so a tampered log cannot poison the
// maps the verifier consults.
//
//   - expectedSkippable: per-log skippable_reasons whitelist + correlator
//     for orders that opted into skip. Each item maps to the log at
//     item.LogSequence (which buildAuditItems sets from either the fresh
//     CreatedLog or the idempotency-replay ReferenceSequence — seq + i
//     is the wrong formula because ReferenceSequence items break the
//     sequential adjacency).
//
//   - chainBound.references: per-ledger first-seen sequence of every
//     CreateTransactionOrder reference. Feeds the CONFLICT replay.
//
//   - chainBound.reverted: per-ledger first-seen sequence of every
//     RevertTransactionOrder (regardless of skip). Feeds the
//     ALREADY_REVERTED replay: "was the tx already reverted before seq?".
//     A skipped ALREADY_REVERTED revert implies a prior successful
//     revert by construction, so appending it here is safe — the
//     first-seen wins semantic keeps the earliest seq.
//
//   - chainBound.metadata / accountTypes: chronological presence-flip
//     lists (SetMetadata / AddAccountType → exists=true, Delete /
//     Remove → exists=false). Feeds the NOT_FOUND / ALREADY_EXISTS
//     replay. Conflating skipped-delete with successful-delete is safe
//     because a skipped-delete implies the key was already absent — the
//     transition it records is redundant but correct.
func collectExpectedSkippable(
	items []*auditpb.AuditItem,
	expectedSkippable map[uint64]*expectedSkippableOrder,
	chainBound *chainBoundState,
) {
	for _, item := range items {
		order := &raftcmdpb.Order{}
		if err := order.UnmarshalVT(item.GetSerializedOrder()); err != nil {
			// An item that fails to unmarshal cannot tell us what its order
			// authorised; verifySkippedOrder will see no whitelist and
			// surface an INVALID_SKIP if a skip log claims this sequence.
			continue
		}

		logSeq := item.GetLogSequence()
		if logSeq == 0 {
			// Failure-side audit items get LogSequence=0 (no log was
			// produced); they do not feed either output map.
			continue
		}

		ls := order.GetLedgerScoped()
		if ls == nil {
			continue
		}

		ledger := ls.GetLedger()

		apply := ls.GetApply()
		ct := apply.GetCreateTransaction()

		// Track every chain-bound reference claim on this ledger, not just
		// the ones from orders that opted into skip. Two order shapes can
		// originate a reference write:
		//   - LedgerApplyOrder_CreateTransaction (regular path)
		//   - LedgerScopedOrder_MirrorIngest carrying a
		//     MirrorCreatedTransaction (mirror ingestion after a mirror
		//     promotion replays the source ledger's reference claims)
		// processMirrorCreatedTransaction calls PutTransactionReference
		// the same way processCreateTransaction does, so the verifier
		// would otherwise false-positive on a legitimate skip that
		// conflicts with a mirror-ingested reference.
		if ref := ct.GetReference(); ref != "" {
			rememberReferenceClaim(chainBound.references, ledger, ref, logSeq)
		}

		if mi := ls.GetMirrorIngest(); mi != nil {
			recordMirrorIngestMutations(mi.GetEntry(), ledger, logSeq, chainBound)
		}

		// Audit-derived state timelines for the non-CreateTransaction
		// reasons. Recorded from serialized_order regardless of whether
		// the emitting order opted into skip — a later skip on the same
		// key/id needs the full prior history to answer "was the
		// underlying condition true?".
		recordChainBoundMutations(ls, ledger, logSeq, chainBound)

		reasons := apply.GetSkippableReasons()
		if len(reasons) == 0 {
			continue
		}

		exp := &expectedSkippableOrder{
			reasons: reasons,
			ledger:  ledger,
		}

		if ct != nil {
			exp.reference = ct.GetReference()
		}

		// Per-order correlators for the non-CreateTransaction skip
		// branches. Each is populated only when the chain-bound apply
		// payload matches the natural producer of that reason — the
		// verifier's switch picks the field it needs and ignores the
		// others.
		if rt := apply.GetRevertTransaction(); rt != nil {
			exp.transactionID = rt.GetTransactionId()
		}

		if dm := apply.GetDeleteMetadata(); dm != nil {
			exp.metadataKey = dm.GetKey()
			exp.metadataTarget = formatTargetForSkipContext(dm.GetTarget())
		}

		if aa := apply.GetAddAccountType(); aa != nil {
			exp.accountTypeName = aa.GetAccountType().GetName()
		}

		if ra := apply.GetRemoveAccountType(); ra != nil {
			exp.accountTypeName = ra.GetName()
		}

		expectedSkippable[logSeq] = exp
	}
}

// recordChainBoundMutations extracts every state transition from the
// chain-bound LedgerScoped order and appends it to the matching
// chainBoundState timeline. Only orders that mutate presence/absence
// for currently-annotated skip reasons surface here:
//
//   - CreateLedgerOrder.account_types → chainBound.accountTypes
//     (exists=true) seeded at ledger creation. Without this seed a
//     later skippable AddAccountType of an initial type would falsely
//     surface INVALID_SKIP (the timeline would look empty).
//
//   - RevertTransactionOrder → chainBound.reverted (first-seen wins).
//     Recorded for every occurrence — a skipped ALREADY_REVERTED revert
//     implies a prior successful revert, so its recorded seq is either
//     the first (which is correct) or a later occurrence that
//     first-seen-wins correctly ignores.
//
//   - SetMetadata / SaveMetadata → chainBound.metadata (exists=true).
//     Multi-key SetMetadata expands into one mutation per key.
//
//   - CreateTransactionOrder.account_metadata → chainBound.metadata
//     (exists=true) per (account, key). Client-provided values set
//     account metadata just like an explicit SetMetadata; NOT tracking
//     them lets a later skippable DeleteMetadata on that key falsely
//     pass the "absent" check.
//
//   - DeleteMetadata → chainBound.metadata (exists=false).
//
//   - AddAccountType → chainBound.accountTypes (exists=true).
//
//   - RemoveAccountType → chainBound.accountTypes (exists=false).
//
//   - CreateTransaction.metadata → chainBound.metadata (exists=true)
//     at target=<allocated tx id>. The tx id is derived from the
//     per-ledger nextTxID counter which mirrors the FSM's
//     LedgerBoundaries.NextTransactionId. Skipped CreateTransactions
//     (identified via the chain-bound reference already claimed) do
//     NOT consume a slot — see chainBoundCreateTxSkipped.
//
//   - RevertTransaction.metadata → chainBound.metadata (exists=true)
//     at target=<newly-allocated revert tx id>. The revert produces a
//     NEW transaction (the inverse posting); metadata on the order
//     targets that new tx, not the original one being reverted.
//     Skipped RevertTransactions (via chain-bound prior revert of
//     the same tx) do NOT consume a slot.
//
// All other order shapes are inert.
func recordChainBoundMutations(
	ls *raftcmdpb.LedgerScopedOrder,
	ledger string,
	logSeq uint64,
	chainBound *chainBoundState,
) {
	if ls == nil || ledger == "" {
		return
	}

	if cl := ls.GetCreateLedger(); cl != nil {
		// Initial FSM counter: LedgerBoundaries.NextTransactionId = 1.
		// Only anchor when the ledger's counter has not yet been seeded —
		// a rebuild that replays chain items in-order sees CreateLedger
		// once per ledger. Also mark the ledger as "creation seen" so
		// downstream tx-scoped metadata recording knows the counter is
		// anchored (without this flag, defaulting nextTxID=1 for a
		// ledger whose CreateLedger lives in archive would produce
		// wrong tx-id attribution — see finding cf7f890b).
		if _, seen := chainBound.nextTxID[ledger]; !seen {
			chainBound.nextTxID[ledger] = 1
		}

		chainBound.ledgerCreationSeen[ledger] = struct{}{}

		for name := range cl.GetAccountTypes() {
			if name != "" {
				appendAccountTypeMutation(chainBound.accountTypes, ledger, name, logSeq, true)
			}
		}
	}

	apply := ls.GetApply()
	if apply == nil {
		return
	}

	if am := apply.GetAddMetadata(); am != nil {
		target := formatTargetForSkipContext(am.GetTarget())
		if target != "" {
			for key := range am.GetMetadata() {
				appendMetadataMutation(chainBound.metadata, ledger, target, key, logSeq, true)
			}
		}
	}

	if dm := apply.GetDeleteMetadata(); dm != nil {
		target := formatTargetForSkipContext(dm.GetTarget())
		key := dm.GetKey()
		if target != "" && key != "" {
			appendMetadataMutation(chainBound.metadata, ledger, target, key, logSeq, false)
		}
	}

	if ct := apply.GetCreateTransaction(); ct != nil {
		// account_metadata targets are known independently of the
		// FSM-allocated tx id, so record them regardless of skip
		// state.
		for account, mm := range ct.GetAccountMetadata() {
			if account == "" {
				continue
			}

			for key := range mm.GetValues() {
				appendMetadataMutation(chainBound.metadata, ledger, account, key, logSeq, true)
			}
		}

		// Chain-bound tx id derivation: if the reference was already
		// claimed at an earlier live seq, this order would be skipped
		// by TRANSACTION_REFERENCE_CONFLICT and the counter is NOT
		// bumped. Otherwise the order succeeds and takes the current
		// slot.
		//
		// Tx-scoped metadata is only recorded when the ledger's counter
		// is anchored (CreateLedger observed in live). For archived-
		// history ledgers the counter is defaulted and mislabelling
		// would cause false-negative INVALID_SKIP on future
		// DeleteMetadata(target=<real tx id>). See ledgerCreationSeen.
		if !chainBoundCreateTxSkipped(ledger, ct, logSeq, chainBound) {
			txID := allocateChainBoundTxID(ledger, chainBound)

			// The successful CreateTransaction owns its reference — record
			// the (ref → txID) binding so a later conflicting order's skip
			// can have its context["existingTransactionId"] pinned. Only
			// anchored ledgers get a reliable counter; on unanchored
			// (archived-CreateLedger) ledgers allocateChainBoundTxID
			// defaults the counter and the id would be mis-attributed, so
			// leave the binding absent and let the verifier stay permissive.
			if _, anchored := chainBound.ledgerCreationSeen[ledger]; anchored {
				if ref := ct.GetReference(); ref != "" {
					rememberReferenceTxID(chainBound.referenceTxIDs, ledger, ref, txID)
				}

				target := strconv.FormatUint(txID, 10)
				for key := range ct.GetMetadata() {
					appendMetadataMutation(chainBound.metadata, ledger, target, key, logSeq, true)
				}
			}
		}
	}

	if rt := apply.GetRevertTransaction(); rt != nil {
		if !chainBoundRevertSkipped(ledger, rt, chainBound) {
			// The revert creates a NEW tx (with the inverse
			// postings); RevertTransactionOrder.metadata targets that
			// new tx. Chain-bound.reverted is bumped WITH the
			// ORIGINAL tx id (first-revert-wins), separately from
			// counter allocation.
			rememberFirstRevert(chainBound.reverted, ledger, rt.GetTransactionId(), logSeq)

			newTxID := allocateChainBoundTxID(ledger, chainBound)

			if _, anchored := chainBound.ledgerCreationSeen[ledger]; anchored {
				target := strconv.FormatUint(newTxID, 10)
				for key := range rt.GetMetadata() {
					appendMetadataMutation(chainBound.metadata, ledger, target, key, logSeq, true)
				}
			}
		}
	}

	if aa := apply.GetAddAccountType(); aa != nil {
		if name := aa.GetAccountType().GetName(); name != "" {
			appendAccountTypeMutation(chainBound.accountTypes, ledger, name, logSeq, true)
		}
	}

	if ra := apply.GetRemoveAccountType(); ra != nil {
		if name := ra.GetName(); name != "" {
			appendAccountTypeMutation(chainBound.accountTypes, ledger, name, logSeq, false)
		}
	}
}

// allocateChainBoundTxID mirrors the FSM's boundary bump: returns the
// current nextTxID for the ledger, then increments. Auto-initialises to
// 1 (matching CreateLedger's initial value) when the ledger's counter
// has not been seeded yet — this covers ledgers whose CreateLedger
// order lives in an archived chapter beyond our live scan window.
func allocateChainBoundTxID(ledger string, chainBound *chainBoundState) uint64 {
	current, seen := chainBound.nextTxID[ledger]
	if !seen {
		current = 1
	}

	chainBound.nextTxID[ledger] = current + 1

	return current
}

// chainBoundCreateTxSkipped predicts whether a CreateTransactionOrder
// would have been converted to an OrderSkippedLog by the FSM. Today
// only TRANSACTION_REFERENCE_CONFLICT is skippable on that action, and
// the sub-processor detects the conflict via a chain-bound reference
// claim strictly earlier than the current logSeq. Consulting
// chainBound.references BEFORE the current item's reference is recorded
// answers "was this reference already claimed by an earlier live entry?".
func chainBoundCreateTxSkipped(
	ledger string,
	ct *raftcmdpb.CreateTransactionOrder,
	logSeq uint64,
	chainBound *chainBoundState,
) bool {
	if ct == nil {
		return false
	}

	ref := ct.GetReference()
	if ref == "" {
		return false
	}

	firstSeenSeq, claimed := chainBound.references[ledger][ref]
	if !claimed {
		return false
	}

	return firstSeenSeq < logSeq
}

// chainBoundRevertSkipped predicts whether a RevertTransactionOrder
// would have been converted to an OrderSkippedLog by the FSM. Only
// TRANSACTION_ALREADY_REVERTED is skippable on revert, and the
// sub-processor gates on chain-bound.reverted[ledger][txID] existing
// strictly earlier than this order. Consulting the map BEFORE the
// current revert is recorded answers "was tx already reverted?".
func chainBoundRevertSkipped(
	ledger string,
	rt *raftcmdpb.RevertTransactionOrder,
	chainBound *chainBoundState,
) bool {
	if rt == nil {
		return false
	}

	txID := rt.GetTransactionId()
	if txID == 0 {
		return false
	}

	_, seen := chainBound.reverted[ledger][txID]

	return seen
}

// recordMirrorIngestMutations mirrors recordChainBoundMutations for the
// MirrorLogEntry variants that surface AFTER a mirror-mode promotion.
// Mirror-ingested entries replay the source ledger's state changes on
// this ledger, so a later skippable order on the same target/tx must
// see them in the chain-bound state:
//
//   - MirrorRevertedTransaction → chainBound.reverted (first-seen wins).
//     Fixes the false-positive where a promoted ledger's later
//     RevertTransaction with ALREADY_REVERTED opt-in couldn't find its
//     prior mirror-ingested revert.
//
//   - MirrorSavedMetadata → chainBound.metadata (exists=true).
//     Multi-key save expands into one mutation per key. Mirror-ingested
//     metadata is a real presence; a later skippable DeleteMetadata
//     must see it to answer "was absent" correctly.
//
//   - MirrorDeletedMetadata → chainBound.metadata (exists=false).
//
//   - MirrorCreatedTransaction: reference already handled by the caller
//     via rememberReferenceClaim; no metadata correlator to track here
//     (transaction-scoped metadata is the same tx-id gap documented in
//     recordChainBoundMutations).
func recordMirrorIngestMutations(
	entry *raftcmdpb.MirrorLogEntry,
	ledger string,
	logSeq uint64,
	chainBound *chainBoundState,
) {
	if entry == nil {
		return
	}

	if mct := entry.GetCreatedTransaction(); mct != nil {
		if ref := mct.GetReference(); ref != "" {
			rememberReferenceClaim(chainBound.references, ledger, ref, logSeq)
			// Mirror ingestion preserves the source ledger's tx id, so the
			// owning tx id for a mirror-claimed reference is carried on the
			// entry — record it for existingTransactionId verification.
			rememberReferenceTxID(chainBound.referenceTxIDs, ledger, ref, mct.GetTransactionId())
		}
	}

	if mrt := entry.GetRevertedTransaction(); mrt != nil {
		rememberFirstRevert(chainBound.reverted, ledger, mrt.GetRevertedTransactionId(), logSeq)
	}

	if msm := entry.GetSavedMetadata(); msm != nil {
		target := formatTargetForSkipContext(msm.GetTarget())
		if target != "" {
			for key := range msm.GetMetadata() {
				appendMetadataMutation(chainBound.metadata, ledger, target, key, logSeq, true)
			}
		}
	}

	if mdm := entry.GetDeletedMetadata(); mdm != nil {
		target := formatTargetForSkipContext(mdm.GetTarget())
		key := mdm.GetKey()
		if target != "" && key != "" {
			appendMetadataMutation(chainBound.metadata, ledger, target, key, logSeq, false)
		}
	}
}

// rememberFirstRevert records the FIRST chain-bound revert of a tx —
// second and later occurrences are ignored so verifySkippedOrder's
// "firstRevertSeq < seq" query answers "was tx already reverted before
// seq". This mirrors rememberReferenceClaim's first-claim-wins.
func rememberFirstRevert(
	reverted map[string]map[uint64]uint64,
	ledger string,
	txID uint64,
	logSeq uint64,
) {
	// logSeq=0 is the baseline-fold sentinel (foldBaselineReverted).
	// The live-audit path never reaches here with logSeq=0.
	if txID == 0 {
		return
	}

	per, ok := reverted[ledger]
	if !ok {
		per = make(map[uint64]uint64)
		reverted[ledger] = per
	}

	if _, seen := per[txID]; seen {
		return
	}

	per[txID] = logSeq
}

// appendMetadataMutation appends a presence-flip to the per-(ledger,
// target, key) chronological list. collectExpectedSkippable iterates in
// seq order, so the append order matches seq order — the verifier's
// backward scan finds the latest transition strictly before the skip
// sequence.
func appendMetadataMutation(
	metadata map[string]map[string]map[string][]chainBoundMutation,
	ledger, target, key string,
	logSeq uint64,
	exists bool,
) {
	// NOTE: logSeq=0 IS a valid sentinel value used by the baseline folds
	// (foldBaselineMetadata / foldBaselineChainState) to record entries
	// that predate the live audit range. The caller in
	// collectExpectedSkippable never reaches this helper with logSeq=0
	// (failure-side audit items early-continue at the loop top), so no
	// guard is needed here.

	perLedger, ok := metadata[ledger]
	if !ok {
		perLedger = make(map[string]map[string][]chainBoundMutation)
		metadata[ledger] = perLedger
	}

	perTarget, ok := perLedger[target]
	if !ok {
		perTarget = make(map[string][]chainBoundMutation)
		perLedger[target] = perTarget
	}

	perTarget[key] = append(perTarget[key], chainBoundMutation{seq: logSeq, exists: exists})
}

// appendAccountTypeMutation is appendMetadataMutation's account-type
// twin (one dimension less: no target).
func appendAccountTypeMutation(
	accountTypes map[string]map[string][]chainBoundMutation,
	ledger, name string,
	logSeq uint64,
	exists bool,
) {
	// logSeq=0 is the baseline-fold sentinel (see foldBaselineLedgers).
	// The live-audit path never reaches here with logSeq=0.

	perLedger, ok := accountTypes[ledger]
	if !ok {
		perLedger = make(map[string][]chainBoundMutation)
		accountTypes[ledger] = perLedger
	}

	perLedger[name] = append(perLedger[name], chainBoundMutation{seq: logSeq, exists: exists})
}

// isNumericTxIDTarget reports whether a formatTargetForSkipContext
// output looks like a Target_TransactionId (a decimal uint64), i.e. NOT
// an account address. Used to gate tx-id-scoped metadata verification
// on the ledgerCreationSeen anchor — account-address targets are
// unaffected because their recording bypasses the nextTxID counter.
func isNumericTxIDTarget(target string) bool {
	if target == "" {
		return false
	}

	_, err := strconv.ParseUint(target, 10, 64)

	return err == nil
}

// formatTargetForSkipContext mirrors the string ErrMetadataNotFound stamps
// into its Metadata()["target"] — account targets carry the raw address,
// transaction targets carry the id as a decimal string. Kept in lockstep
// with the sub-processor's formatting so the checker's context-tampering
// check compares apples-to-apples.
func formatTargetForSkipContext(target *commonpb.Target) string {
	switch t := target.GetTarget().(type) {
	case *commonpb.Target_Account:
		return t.Account.GetAddr()
	case *commonpb.Target_TransactionId:
		return strconv.FormatUint(t.TransactionId, 10)
	default:
		return ""
	}
}

// rememberReferenceClaim records the first chain-bound claim of a
// (ledger, reference) pair. Used by collectExpectedSkippable across the
// two order shapes that produce references (CreateTransactionOrder and
// MirrorIngestOrder.MirrorCreatedTransaction) to keep the
// first-claim-wins semantic the verifier relies on (`firstSeenSeq < seq`).
func rememberReferenceClaim(
	chainBoundReferences map[string]map[string]uint64,
	ledger, reference string,
	logSeq uint64,
) {
	if ledger == "" || reference == "" {
		return
	}

	perLedger := chainBoundReferences[ledger]
	if perLedger == nil {
		perLedger = make(map[string]uint64)
		chainBoundReferences[ledger] = perLedger
	}

	if _, claimed := perLedger[reference]; claimed {
		return
	}

	perLedger[reference] = logSeq
}

// rememberReferenceTxID records the owning transaction id of the FIRST
// successful claim of a reference (ledger → reference → txID), mirroring
// rememberReferenceClaim's first-claim-wins semantic. Only successful
// claimants call this (a skipped CreateTransaction consumes no tx slot),
// so the recorded id is the transaction that actually owns the reference
// and is therefore the value a later TRANSACTION_REFERENCE_CONFLICT skip
// surfaces as context["existingTransactionId"]. A txID of 0 is treated as
// "not derivable" and not recorded — the verifier stays permissive rather
// than pinning a bogus id.
func rememberReferenceTxID(
	chainBoundReferenceTxIDs map[string]map[string]uint64,
	ledger, reference string,
	txID uint64,
) {
	if ledger == "" || reference == "" || txID == 0 {
		return
	}

	perLedger := chainBoundReferenceTxIDs[ledger]
	if perLedger == nil {
		perLedger = make(map[string]uint64)
		chainBoundReferenceTxIDs[ledger] = perLedger
	}

	if _, claimed := perLedger[reference]; claimed {
		return
	}

	perLedger[reference] = txID
}

// foldBaselineReferences merges the TransactionReference attribute from
// the baseline checkpoint into chainBoundReferences with a sentinel
// sequence of 0 (always precedes any live log seq). The baseline is the
// pre-archive Pebble snapshot the checker already consults for
// volumes/metadata/transactions; without this fold, references claimed
// in purged chapters would be invisible to verifySkippedOrder and the
// archive escape hatch would have to ignore the entire live skip range.
//
// Returns true when baseline references were successfully loaded. The
// caller scopes the archive escape to (hasArchivedChapters && !baselineLoaded)
// so a fabricated skip on a fresh live reference still surfaces as
// INVALID_SKIP whenever a baseline is available.
//
// baselineDB=nil short-circuits the load (no archived data, or baseline
// unavailable — the caller already paid for the open if applicable).
func (c *Checker) foldBaselineReferences(
	baselineDB *pebble.DB,
	chainBoundReferences map[string]map[string]uint64,
	chainBoundReferenceTxIDs map[string]map[string]uint64,
) (bool, error) {
	if baselineDB == nil {
		return false, nil
	}

	iter, err := c.attrs.References.NewStreamingIter(baselineDB, nil)
	if err != nil {
		return false, fmt.Errorf("iterating baseline references: %w", err)
	}

	for iter.Next() {
		entry := iter.Entry()

		var trk domain.TransactionReferenceKey
		if err := trk.Unmarshal(entry.CanonicalKey); err != nil {
			continue
		}

		if trk.LedgerName == "" || trk.Reference == "" {
			continue
		}

		// The baseline TransactionReferenceValue carries the owning tx id
		// of the archived claim — fold it so a live conflicting skip that
		// resolves against an archived reference can still pin its
		// context["existingTransactionId"]. First-claim-wins inside the map;
		// the baseline is the earliest claim by construction (sentinel 0).
		rememberReferenceTxID(chainBoundReferenceTxIDs, trk.LedgerName, trk.Reference, entry.Value.GetTransactionId())

		perLedger := chainBoundReferences[trk.LedgerName]
		if perLedger == nil {
			perLedger = make(map[string]uint64)
			chainBoundReferences[trk.LedgerName] = perLedger
		}

		// Baseline references always win over any live-audit entry for the
		// same reference. Two cases motivate the override:
		//
		//   1. A skipped order's OWN reference is recorded by
		//      collectExpectedSkippable at its skip-log seq (AuditItem.
		//      LogSequence carries the skip log's seq, not 0). Without the
		//      override, verifySkippedOrder sees firstSeenSeq >= seq and
		//      false-positives INVALID_SKIP on a legitimate archive-scoped
		//      skip that a live baseline could otherwise confirm.
		//   2. If the reference conflict enforcement is correct, only ONE
		//      successful claim of a given reference can exist across the
		//      ledger's lifetime. So a baseline entry ⇒ no successful live
		//      claim ⇒ any live-audit record for this reference is either
		//      the skipped order itself (case 1) or a mirror-ingest replay
		//      of the archived source claim — both cases want the archived
		//      claim (sentinel 0) as the effective firstSeenSeq.
		//
		// The check verifySkippedOrder runs is `firstSeenSeq < seq`, so 0 is
		// the safest value: it always beats any live seq without changing
		// the truth of the comparison.
		perLedger[trk.Reference] = 0 // sentinel: precedes every live log seq
	}

	if err := iter.Close(); err != nil {
		return false, fmt.Errorf("closing baseline references iterator: %w", err)
	}

	if err := iter.Err(); err != nil {
		return false, fmt.Errorf("baseline references iterator error: %w", err)
	}

	return true, nil
}

// foldBaselineChainState seeds chainBound.reverted / metadata /
// accountTypes / ledgerCreationSeen / nextTxID from the baseline
// snapshot. Sibling of foldBaselineReferences — closes the archive-
// boundary integrity gap for non-CONFLICT skip reasons on ledgers whose
// relevant history predates the archive boundary.
//
// Returns (true, nil) when the baseline was actually loaded — the
// caller (verifySkippedOrder via a plumbed flag) uses this to scope
// its archive escape: with baseline chain state available, an "empty
// live evidence" no longer excuses a forged skip because the baseline
// carries the missing evidence.
//
// baselineDB=nil short-circuits every fold and returns (false, nil).
//
// Every seeded entry uses sentinel seq=0 so mutationStateAtSeq's
// backward walk finds it below any live log seq — baseline claims
// win on empty live but never override a live mutation.
func (c *Checker) foldBaselineChainState(
	baselineDB *pebble.DB,
	chainBound *chainBoundState,
) (bool, error) {
	if baselineDB == nil {
		return false, nil
	}

	if err := c.foldBaselineLedgers(baselineDB, chainBound); err != nil {
		return false, err
	}

	if err := c.foldBaselineBoundaries(baselineDB, chainBound); err != nil {
		return false, err
	}

	if err := c.foldBaselineReverted(baselineDB, chainBound); err != nil {
		return false, err
	}

	if err := c.foldBaselineMetadata(baselineDB, chainBound); err != nil {
		return false, err
	}

	if err := c.foldBaselineTxMetadata(baselineDB, chainBound); err != nil {
		return false, err
	}

	return true, nil
}

// foldBaselineTxMetadata seeds transaction-scoped metadata from the
// baseline's TransactionState.Metadata field. Closes the gap where a
// live DeleteMetadata(target=<archived tx id>, key=X) skip falsely
// passes because the tx-scoped metadata's Set lived in an archived
// chapter's CreateTransaction / RevertTransaction order.
func (c *Checker) foldBaselineTxMetadata(
	baselineDB *pebble.DB,
	chainBound *chainBoundState,
) error {
	iter, err := c.attrs.Transaction.NewStreamingIter(baselineDB, nil)
	if err != nil {
		return fmt.Errorf("iterating baseline transactions for tx metadata: %w", err)
	}

	for iter.Next() {
		entry := iter.Entry()

		var tk domain.TransactionKey
		if err := tk.Unmarshal(entry.CanonicalKey); err != nil {
			continue
		}

		if tk.LedgerName == "" || entry.Value == nil {
			continue
		}

		md := entry.Value.GetMetadata()
		if len(md) == 0 {
			continue
		}

		target := strconv.FormatUint(tk.ID, 10)
		for key := range md {
			if key == "" {
				continue
			}

			appendMetadataMutation(chainBound.metadata, tk.LedgerName, target, key, 0, true)
		}
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf("closing baseline transactions iterator for tx metadata: %w", err)
	}

	return iter.Err()
}

// foldBaselineLedgers marks every ledger present in the baseline as
// creation-seen and seeds its account-type set from LedgerInfo.
// account_types. Fixes two false-negatives on archived-history ledgers:
//
//   - The tx-id-scoped metadata permissive fallback (see
//     isNumericTxIDTarget) no longer applies for ledgers whose
//     CreateLedger sits in an archive but whose LedgerInfo IS in the
//     baseline — the counter can be seeded via foldBaselineBoundaries.
//
//   - A later skippable AddAccountType of a type present at ledger
//     creation (but archived away) is legitimately valid; the checker
//     would previously flag it INVALID_SKIP.
func (c *Checker) foldBaselineLedgers(
	baselineDB *pebble.DB,
	chainBound *chainBoundState,
) error {
	iter, err := c.attrs.Ledger.NewStreamingIter(baselineDB, nil)
	if err != nil {
		return fmt.Errorf("iterating baseline ledgers: %w", err)
	}

	for iter.Next() {
		entry := iter.Entry()

		var lk domain.LedgerKey
		if err := lk.Unmarshal(entry.CanonicalKey); err != nil {
			continue
		}

		if lk.Name == "" || entry.Value == nil {
			continue
		}

		chainBound.ledgerCreationSeen[lk.Name] = struct{}{}

		for name := range entry.Value.GetAccountTypes() {
			if name == "" {
				continue
			}

			appendAccountTypeMutation(chainBound.accountTypes, lk.Name, name, 0, true)
		}
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf("closing baseline ledgers iterator: %w", err)
	}

	return iter.Err()
}

// foldBaselineBoundaries seeds nextTxID per ledger from the baseline's
// LedgerBoundaries.NextTransactionId. Without this, ledgers whose
// CreateLedger sits in an archive default nextTxID to 1 and mislabel
// live CreateTransaction / RevertTransaction metadata under wrong
// tx-id targets (see finding cf7f890b).
func (c *Checker) foldBaselineBoundaries(
	baselineDB *pebble.DB,
	chainBound *chainBoundState,
) error {
	iter, err := c.attrs.Boundary.NewStreamingIter(baselineDB, nil)
	if err != nil {
		return fmt.Errorf("iterating baseline boundaries: %w", err)
	}

	for iter.Next() {
		entry := iter.Entry()

		var lk domain.LedgerKey
		if err := lk.Unmarshal(entry.CanonicalKey); err != nil {
			continue
		}

		if lk.Name == "" || entry.Value == nil {
			continue
		}

		next := entry.Value.GetNextTransactionId()
		if next == 0 {
			continue
		}

		// Existing seed (from live CreateLedger) wins if it was already
		// observed — live is authoritative on the current counter.
		if existing, seen := chainBound.nextTxID[lk.Name]; !seen || existing < next {
			chainBound.nextTxID[lk.Name] = next
		}
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf("closing baseline boundaries iterator: %w", err)
	}

	return iter.Err()
}

// foldBaselineReverted seeds chainBound.reverted from the baseline's
// TransactionState.RevertedByTransaction markers. Every tx already
// reverted at the archive boundary lands with sentinel seq=0 so a
// later legitimate skippable RevertTransaction targeting that tx sees
// the prior revert regardless of whether it lives in an archived
// chapter.
func (c *Checker) foldBaselineReverted(
	baselineDB *pebble.DB,
	chainBound *chainBoundState,
) error {
	iter, err := c.attrs.Transaction.NewStreamingIter(baselineDB, nil)
	if err != nil {
		return fmt.Errorf("iterating baseline transactions: %w", err)
	}

	for iter.Next() {
		entry := iter.Entry()

		var tk domain.TransactionKey
		if err := tk.Unmarshal(entry.CanonicalKey); err != nil {
			continue
		}

		if tk.LedgerName == "" || entry.Value == nil {
			continue
		}

		if entry.Value.GetRevertedByTransaction() == 0 {
			continue
		}

		// First-revert-wins semantic: sentinel 0 beats any live seq.
		perLedger, ok := chainBound.reverted[tk.LedgerName]
		if !ok {
			perLedger = make(map[uint64]uint64)
			chainBound.reverted[tk.LedgerName] = perLedger
		}

		if _, seen := perLedger[tk.ID]; seen {
			continue
		}

		perLedger[tk.ID] = 0
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf("closing baseline transactions iterator: %w", err)
	}

	return iter.Err()
}

// foldBaselineMetadata seeds chainBound.metadata (exists=true, seq=0)
// for every account-metadata entry present in the baseline. Closes
// the log-only tampering vector where a live DeleteMetadata skip
// falsely passes because the metadata's Set lived in an archived
// chapter — the baseline still holds the key.
//
// Only account-metadata targets are folded here; transaction-scoped
// metadata is not persisted as a separate attribute in the baseline
// (it lives on TransactionState). Tx-scoped metadata verification
// stays under the ledgerCreationSeen / nextTxID anchoring path.
func (c *Checker) foldBaselineMetadata(
	baselineDB *pebble.DB,
	chainBound *chainBoundState,
) error {
	iter, err := c.attrs.Metadata.NewStreamingIter(baselineDB, nil)
	if err != nil {
		return fmt.Errorf("iterating baseline metadata: %w", err)
	}

	for iter.Next() {
		entry := iter.Entry()

		var mk domain.MetadataKey
		if err := mk.Unmarshal(entry.CanonicalKey); err != nil {
			continue
		}

		if mk.LedgerName == "" || mk.Account == "" || mk.Key == "" {
			continue
		}

		appendMetadataMutation(chainBound.metadata, mk.LedgerName, mk.Account, mk.Key, 0, true)
	}

	if err := iter.Close(); err != nil {
		return fmt.Errorf("closing baseline metadata iterator: %w", err)
	}

	return iter.Err()
}

// verifySkippedOrder flags an OrderSkippedLog projection whose reason was
// not authorised by the chain-bound Order.skippable_reasons whitelist (or
// is a structural KindInternal reason — defense in depth mirroring the
// gate in matchOrderSkip). It then replays the reason-specific condition
// against the AUDIT chain (not the LedgerLog projection) so a tampered
// CreatedTransaction log cannot forge a "prior claim" for a fake skip.
//
// chainBoundReferences was built in verifyAuditHashChain from
// chain-verified `serialized_order` payloads, so any reference appearing
// in it was the subject of a real audit-bound write at the recorded
// sequence. hasArchivedChapters acts as a presence escape hatch:
// references claimed in archived chapters have their audit entries purged
// and therefore are NOT in chainBoundReferences. To avoid false positives
// on legitimate skips against archived references, the per-reason
// presence check is downgraded to a silent pass when archived chapters
// exist. This trade-off matches the same archive boundary
// compareIdempotencyOutcomes uses.
func verifySkippedOrder(
	ledger string,
	seq uint64,
	payload *commonpb.LedgerLogPayload,
	expectedSkippable map[uint64]*expectedSkippableOrder,
	chainBound *chainBoundState,
	hasArchivedChapters bool,
	baselineReferencesAvailable bool,
	baselineChainStateAvailable bool,
	callback func(*servicepb.CheckStoreEvent),
) {
	skipped, ok := payload.GetPayload().(*commonpb.LedgerLogPayload_OrderSkipped)
	if !ok {
		// The projection at `seq` is NOT an OrderSkippedLog. The elision
		// check (inverse direction) is dispatched at the outer log
		// iteration scope by dispatchElisionCheck so it also fires for
		// tampered projections that never reach this well-formed Apply-log
		// path (non-Apply payload, nil Apply.Log, nil Data). Return
		// silently here — the outer dispatch owns the alert.
		return
	}

	if skipped.OrderSkipped == nil {
		// OrderSkipped oneof is set but the inner OrderSkippedLog is nil.
		// dispatchElisionCheck classifies "oneof set" as a valid skip
		// projection and defers to this forward verifier, so a silent
		// return here would let a malformed skip evade both checks. Flag
		// it directly as invalid — a legitimate skip always carries a
		// populated inner message (assignSkipLogIDAndDate + reason).
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
			fmt.Sprintf("log %d on ledger %q sets the OrderSkipped oneof but the inner OrderSkippedLog message is nil", seq, ledger),
			seq, ledger, "", "",
		))

		return
	}

	reason := skipped.OrderSkipped.GetReason()

	if reason == commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
			fmt.Sprintf("log %d records an OrderSkipped projection with UNSPECIFIED reason in ledger %q", seq, ledger),
			seq, ledger, "", "",
		))

		return
	}

	if domain.KindForReason(reason) == domain.KindInternal {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
			fmt.Sprintf("log %d records an OrderSkipped projection with KindInternal reason %s in ledger %q (structural failures must never skip)", seq, reason, ledger),
			seq, ledger, "", "",
		))

		return
	}

	expected, ok := expectedSkippable[seq]
	if !ok {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
			fmt.Sprintf("log %d records an OrderSkipped projection (reason %s) but the originating order did not authorise any skippable reason", seq, reason),
			seq, ledger, "", "",
		))

		return
	}

	if !slices.Contains(expected.reasons, reason) {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
			fmt.Sprintf("log %d records an OrderSkipped projection with reason %s that is not in the originating order's skippable_reasons whitelist", seq, reason),
			seq, ledger, "", "",
		))

		return
	}

	// Ledger-envelope pinning: catch a tampered ApplyLedgerLog envelope
	// that transplants a skip from its chain-bound ledger onto a
	// different one. LogSequence is proposal-global, so exactly one
	// chain-bound order sits at this seq — a projection claiming a
	// different ledger identifies the tampering directly. Kept before
	// the per-reason switch so every reason inherits the check, not
	// just CONFLICT.
	if expected.ledger != ledger {
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
			fmt.Sprintf("log %d records OrderSkipped on ledger %q but the chain-bound order targets ledger %q", seq, ledger, expected.ledger),
			seq, ledger, "", "",
		))

		return
	}

	// Reason-specific replay: confirm the underlying condition was
	// plausible at this sequence given the chain-bound order.
	//
	// Fail-closed: every whitelisted reason MUST have an explicit case
	// here. Adding a reason to admission's allowedSkippableReasons (see
	// internal/application/admission/skippable.go) without also adding a
	// case below would let a projection sail through with only the
	// whitelist-membership check — the underlying condition would never
	// be replayed against the audit chain. The `default` branch is the
	// tripwire.
	switch reason {
	case commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT:
		// Empty reference means the original order had no reference set —
		// TRANSACTION_REFERENCE_CONFLICT is structurally impossible.
		if expected.reference == "" {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records TRANSACTION_REFERENCE_CONFLICT skip but the audited order on ledger %q has no reference set", seq, ledger),
				seq, ledger, "", "",
			))

			return
		}

		// Verify the persisted OrderSkipped.context fields against the
		// chain-bound order BEFORE the reference-claim lookup. Both
		// `reference` and `ledger` come from expected (chain-bound), so
		// their check is independent of whether the prior claim lives in
		// the live audit range or an archived chapter — running them
		// before the archive escape below closes a tampering vector where
		// the context alone is edited on a legitimate archive-scoped skip.
		//
		// The context surfaces to clients via the REST response and gRPC
		// log payload, so tampering only the context (without flipping
		// the reason or rewriting an unrelated log) would otherwise pass
		// Check() with no other tripwire — the LedgerLog projection is
		// not hash-bound.
		//
		// ErrTransactionReferenceConflict.Metadata() ALWAYS writes both
		// `ledger` and `reference`; when the FSM detected the collision it
		// also writes `existingTransactionId`. All three are re-derivable
		// from audit-bound data (existingTransactionId via the owning tx id
		// the checker recorded when it replayed the first successful claim —
		// see chainBound.referenceTxIDs), so all three are pinned. Missing
		// or mismatched values are tampering — the LedgerLog projection is
		// not hash-bound, so the checker is the only guard for these
		// client-facing fields (invariant #8).
		ctx := skipped.OrderSkipped.GetContext()

		if got := ctx["reference"]; got != expected.reference {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records TRANSACTION_REFERENCE_CONFLICT skip with context.reference=%q but the chain-bound order references %q", seq, got, expected.reference),
				seq, ledger, "", "",
			))

			return
		}

		if got := ctx["ledger"]; got != expected.ledger {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records TRANSACTION_REFERENCE_CONFLICT skip with context.ledger=%q but the chain-bound order targets ledger %q", seq, got, expected.ledger),
				seq, ledger, "", "",
			))

			return
		}

		// Pin context["existingTransactionId"] against the owning tx id the
		// checker recorded for the first successful claim of this reference
		// (chainBound.referenceTxIDs, populated from the live CreateTransaction
		// counter, mirror ingestion, or the baseline archive fold). A
		// tamperer who rewrites this field to misattribute the conflicting
		// transaction is caught here. Stays permissive only when the owning
		// id is not derivable (ownerTxID==0): a live claim on an unanchored
		// (archived-CreateLedger) ledger, or a claim whose CreateTransaction
		// lives entirely in a purged chapter with no baseline. In that case
		// the field cannot be re-derived, so pinning it would false-positive;
		// this matches the archive-escape philosophy of the seq lookup below.
		if ownerTxID := chainBound.referenceTxIDs[ledger][expected.reference]; ownerTxID != 0 {
			want := strconv.FormatUint(ownerTxID, 10)
			// Only enforce when the projection actually carries the field —
			// ErrTransactionReferenceConflict omits it when the FSM could not
			// resolve the owner (ExistingTransactionID==0), so an absent
			// field is not tampering.
			if got, present := ctx["existingTransactionId"]; present && got != want {
				callback(errorEvent(
					servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
					fmt.Sprintf("log %d records TRANSACTION_REFERENCE_CONFLICT skip with context.existingTransactionId=%q but the reference %q on ledger %q is owned by transaction %s", seq, got, expected.reference, ledger, want),
					seq, ledger, "", "",
				))

				return
			}
		}

		// Look up the reference in the audit-derived map. firstSeenSeq is
		// strictly less than seq because chainBound.references only retains
		// the first claim for each reference (re-claims on the same ref
		// later — including the very order producing this skip log — do
		// not move it).
		firstSeenSeq, claimed := chainBound.references[ledger][expected.reference]
		if !claimed || firstSeenSeq >= seq {
			// No earlier claim visible in the live audit range. If the
			// baseline TransactionReference attribute did NOT cover the
			// archived range (baselineReferencesAvailable=false), the
			// claim may live in a purged chapter we cannot re-verify.
			// Stay permissive under that specific condition; when the
			// baseline IS available (foldBaselineReferences folded
			// archived refs into chainBound.references with sentinel
			// seq=0), the missing claim proves fabrication.
			if hasArchivedChapters && !baselineReferencesAvailable {
				return
			}

			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records TRANSACTION_REFERENCE_CONFLICT skip but reference %q was not claimed on ledger %q before this sequence", seq, expected.reference, ledger),
				seq, ledger, "", "",
			))

			return
		}

	case commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED:
		// Chain-bound Order was a RevertTransactionOrder targeting
		// transactionID. Two-layer verification:
		//
		//   1. context["transactionId"] == chain-bound tx id (closes the
		//      correlator-tampering vector — projection swapped to a
		//      different tx to lie to clients).
		//
		//   2. chainBound.reverted[ledger][txID] < seq (closes the
		//      log-only tampering vector — projection swapped from a
		//      genuine successful revert to a fabricated skip). The
		//      chainBound.reverted map records the first successful
		//      revert of each tx from the chain's serialized_order, so
		//      a "skip because already reverted" is only legitimate
		//      when a strictly-earlier revert exists.
		if expected.transactionID == 0 {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records TRANSACTION_ALREADY_REVERTED skip but the audited order on ledger %q is not a RevertTransactionOrder", seq, ledger),
				seq, ledger, "", "",
			))

			return
		}

		ctx := skipped.OrderSkipped.GetContext()
		want := strconv.FormatUint(expected.transactionID, 10)
		if got := ctx["transactionId"]; got != want {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records TRANSACTION_ALREADY_REVERTED skip with context.transactionId=%q but the chain-bound order targets %s", seq, got, want),
				seq, ledger, "", "",
			))

			return
		}

		firstRevertSeq, seen := chainBound.reverted[ledger][expected.transactionID]
		if !seen || firstRevertSeq >= seq {
			// No earlier successful revert visible in the live audit
			// range. If archived chapters exist AND the baseline chain
			// state fold did NOT succeed (foldBaselineReverted couldn't
			// seed reverted-tx claims from LedgerBoundaries), the prior
			// revert may live in a purged range we cannot re-verify.
			// Stay permissive under that specific condition; when the
			// baseline IS available, foldBaselineReverted already
			// folded any archived reverts as sentinel seq=0 entries, so
			// a missing claim proves fabrication.
			if hasArchivedChapters && !baselineChainStateAvailable {
				return
			}

			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records TRANSACTION_ALREADY_REVERTED skip but tx %d was not reverted on ledger %q before this sequence", seq, expected.transactionID, ledger),
				seq, ledger, "", "",
			))

			return
		}

	case commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND:
		// Chain-bound Order was a DeleteMetadataOrder with (target, key).
		// Two-layer verification:
		//
		//   1. context["target"] + context["key"] match chain-bound
		//      values (correlator-tampering vector).
		//
		//   2. mutationStateAtSeq(chainBound.metadata[l][t][k], seq) ==
		//      false — the key was actually absent just before seq.
		//      Closes the log-only tampering vector where the projection
		//      was swapped from a genuine successful delete to a
		//      fabricated skip. The metadata timeline is built from
		//      serialized_order SetMetadata / DeleteMetadata mutations,
		//      so a tampered log cannot influence it.
		if expected.metadataKey == "" {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records METADATA_NOT_FOUND skip but the audited order on ledger %q is not a DeleteMetadataOrder", seq, ledger),
				seq, ledger, "", "",
			))

			return
		}

		ctx := skipped.OrderSkipped.GetContext()
		if got := ctx["key"]; got != expected.metadataKey {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records METADATA_NOT_FOUND skip with context.key=%q but the chain-bound order targets key %q", seq, got, expected.metadataKey),
				seq, ledger, "", "",
			))

			return
		}

		if got := ctx["target"]; got != expected.metadataTarget {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records METADATA_NOT_FOUND skip with context.target=%q but the chain-bound order targets %q", seq, got, expected.metadataTarget),
				seq, ledger, "", "",
			))

			return
		}

		// Tx-id-scoped metadata on a ledger whose CreateLedger sits in
		// an archived chapter: the chain-bound nextTxID counter was
		// defaulted (no CreateLedger observed → wrong tx-id
		// attribution), so we did NOT record any CreateTransaction/
		// RevertTransaction metadata for this ledger. The timeline is
		// UNRELIABLE for tx-id targets on such ledgers — treat the
		// verification as inconclusive rather than false-negative or
		// false-positive. Account-address targets are not affected
		// (recorded directly from the order without counter derivation).
		if isNumericTxIDTarget(expected.metadataTarget) {
			if _, anchored := chainBound.ledgerCreationSeen[ledger]; !anchored && hasArchivedChapters {
				return
			}
		}

		if mutationStateAtSeq(chainBound.metadata[ledger][expected.metadataTarget][expected.metadataKey], seq) {
			// Chain-bound state says the key was PRESENT just before
			// seq — a DeleteMetadata that ran on that state would have
			// succeeded, not skipped. Live-audit evidence of presence
			// is authoritative: archived chapters live BEFORE the
			// current audit range, so they cannot undo a live Set.
			// Unlike CONFLICT (where "no live claim" leaves the archive
			// as a plausible source), a live Set at some earlier seq
			// is a positive fact — no archive escape applies.
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records METADATA_NOT_FOUND skip on (target=%q, key=%q) but the audit chain shows the key was present at this sequence on ledger %q", seq, expected.metadataTarget, expected.metadataKey, ledger),
				seq, ledger, "", "",
			))

			return
		}

	case commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS,
		commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND:
		// Chain-bound Order was an AddAccountType (for ALREADY_EXISTS)
		// or RemoveAccountType (for NOT_FOUND) targeting `name`.
		// Two-layer verification:
		//
		//   1. context["name"] matches chain-bound value.
		//
		//   2. The chain-bound state at seq matches the reason:
		//      ALREADY_EXISTS ⇒ name was present just before seq,
		//      NOT_FOUND     ⇒ name was absent just before seq.
		//      Anything else would have produced a successful log
		//      (AddedAccountTypeLog / RemovedAccountTypeLog), not a
		//      skip. A tampered log that swaps success → skip fails
		//      this check because the timeline reflects the true
		//      chain-bound mutations.
		if expected.accountTypeName == "" {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records %s skip but the audited order on ledger %q is not an AccountType order", seq, reason, ledger),
				seq, ledger, "", "",
			))

			return
		}

		ctx := skipped.OrderSkipped.GetContext()
		if got := ctx["name"]; got != expected.accountTypeName {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records %s skip with context.name=%q but the chain-bound order targets %q", seq, reason, got, expected.accountTypeName),
				seq, ledger, "", "",
			))

			return
		}

		present, witnessed := mutationStateWithWitness(chainBound.accountTypes[ledger][expected.accountTypeName], seq)

		mustBePresent := reason == commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS
		if present != mustBePresent {
			// Archive escape is direction-aware AND witness-aware:
			//
			//   - ALREADY_EXISTS + live shows ABSENT + no witness:
			//     archives may contain an AddAccountType that was
			//     never Removed in live — permissive.
			//
			//   - ALREADY_EXISTS + live shows ABSENT + witnessed
			//     (a live RemoveAccountType before seq): live has
			//     positive proof the name is absent regardless of
			//     archives — a legitimate Add would have succeeded,
			//     not conflicted. Archive escape SHOULD NOT apply.
			//
			//   - NOT_FOUND + live shows PRESENT: archives lie BEFORE
			//     the current live range and cannot undo a live
			//     AddAccountType. Live-presence is authoritative — no
			//     archive escape.
			if mustBePresent && hasArchivedChapters && !baselineChainStateAvailable && !witnessed {
				return
			}

			condition := "absent"
			if mustBePresent {
				condition = "present"
			}

			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records %s skip on account type %q but the audit chain shows the name was not %s at this sequence on ledger %q", seq, reason, expected.accountTypeName, condition, ledger),
				seq, ledger, "", "",
			))

			return
		}

	default:
		// Fail-loud fallback: the whitelist admitted this reason but no
		// verification branch replayed it against the audit chain. Any new
		// entry in admission's allowedSkippableReasons must land a matching
		// case above; until then, treat the projection as unverifiable.
		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
			fmt.Sprintf("log %d records an OrderSkipped projection with reason %s on ledger %q, but the checker has no reason-specific replay branch — the reason was added to the whitelist without extending verifySkippedOrder", seq, reason, ledger),
			seq, ledger, "", "",
		))
	}
}

// resolveIdempotencyTTLMicros picks the TTL (in microseconds) that bounds the
// cold re-derivation window. The boot-validated runtime config is preferred
// because it is not read from the audited store; the persisted projection is
// the fallback for paths with no runtime config (CLI / restore backup
// validation). Returns nil when neither is available — the window is then
// unknown and the cold pass is skipped.
func resolveIdempotencyTTLMicros(runtime *time.Duration, persisted *commonpb.PersistedConfig) *uint64 {
	if runtime != nil {
		micros := uint64(runtime.Microseconds())

		return &micros
	}

	if persisted != nil {
		micros := persisted.GetIdempotencyTtlSeconds() * 1_000_000

		return &micros
	}

	return nil
}

// idempotencyWindowCutoff returns the lower bound of the idempotency TTL window
// given the hash-chain-verified "now" and the configured TTL. A ttlMicros of 0
// (idempotency-ttl=0, never expire) or a ledger younger than its TTL yields 0 —
// an unbounded window that re-derives the whole archived history.
func idempotencyWindowCutoff(now, ttlMicros uint64) uint64 {
	if ttlMicros != 0 && now > ttlMicros {
		return now - ttlMicros
	}

	return 0
}

// dispatchElisionCheck runs the elision guard at every seq where the audit
// chain expected a skip, using expected.ledger (chain-bound) as the ledger
// context. A "valid skip projection" is a well-formed Apply payload with a
// non-nil Log/Data whose payload discriminant is LedgerLogPayload_OrderSkipped;
// anything else — a non-Apply payload (e.g. LogPayload_DeleteLedger), nil
// Apply.Log, nil Log.Data, or an Apply payload discriminant other than
// OrderSkipped — is treated as an elision candidate and forwarded to
// verifyExpectedSkipNotElided.
//
// Dispatching from the outer iteration (rather than from inside
// verifySkippedOrder) closes a bypass path: the Apply case's inline call
// only fires for well-formed Apply payloads with non-nil Log.Data, so a
// tampered projection stored as any other shape would previously escape
// the elision check entirely.
func dispatchElisionCheck(
	seq uint64,
	log *commonpb.Log,
	expectedSkippable map[uint64]*expectedSkippableOrder,
	chainBound *chainBoundState,
	hasArchivedChapters bool,
	callback func(*servicepb.CheckStoreEvent),
) {
	expected, isExpected := expectedSkippable[seq]
	if !isExpected {
		return
	}

	if apl, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply); ok &&
		apl.Apply != nil &&
		apl.Apply.GetLog() != nil &&
		apl.Apply.GetLog().GetData() != nil {
		if _, isSkip := apl.Apply.GetLog().GetData().GetPayload().(*commonpb.LedgerLogPayload_OrderSkipped); isSkip {
			// Well-formed OrderSkipped projection: verifySkippedOrder
			// (called from the Apply branch of the iteration) already
			// validated the forward direction.
			return
		}
	}

	verifyExpectedSkipNotElided(expected.ledger, seq, expectedSkippable, chainBound, hasArchivedChapters, callback)
}

// verifyExpectedSkipNotElided is the inverse-direction sibling of
// verifySkippedOrder: it fires when the audit chain says the FSM MUST have
// emitted an OrderSkipped log at this sequence (the originating order opted
// into skip AND the chain shows the skip condition was met) but the stored
// LedgerLog projection says otherwise (typically a forged CreatedTransaction).
//
// Without this check a tamperer could elide a skip and convince downstream
// readers a transaction landed when the FSM actually rolled it back: the
// LedgerLog is a projection of the audit chain, not hash-bound itself.
//
// The `!claimed || firstSeenSeq >= seq` guard below already covers the
// archive-boundary case: references claimed in purged chapters legitimately
// fail to appear in chainBoundReferences (so `claimed=false`), and the
// verifier stays permissive without any archive-flag branch. Once we DO see
// a prior live claim (`firstSeenSeq < seq`), the elision is positively
// proven by the hash-chained audit range — we must NOT downgrade this to a
// silent pass just because archived chapters exist elsewhere in the log.
func verifyExpectedSkipNotElided(
	ledger string,
	seq uint64,
	expectedSkippable map[uint64]*expectedSkippableOrder,
	chainBound *chainBoundState,
	hasArchivedChapters bool,
	callback func(*servicepb.CheckStoreEvent),
) {
	expected, ok := expectedSkippable[seq]
	if !ok {
		// The originating order did not authorise any skippable reason; any
		// projection is fine (the FSM never had the option to skip).
		return
	}

	if expected.ledger != ledger {
		// Cross-ledger desync: the projection at this sequence is on a
		// different ledger than the chain-bound order targets. Either the
		// per-ledger LedgerLog routing is broken (a separate Check pass
		// catches that) or the projection was tampered to land elsewhere;
		// either way it is not the legitimate landing site of the audited
		// order, so leave the call to the other verifier.
		return
	}

	// Per-reason elision proof: we fire only when the chain-bound audit
	// state shows the skip condition WAS met at seq — otherwise the
	// projection could legitimately be a non-skip outcome. Reasons whose
	// audit-derived state cannot definitively prove "should have
	// skipped" (no correlator on expected, no state timeline) are left
	// to the forward direction, which catches forged OrderSkipped logs.
	switch {
	case slices.Contains(expected.reasons, commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT) && expected.reference != "":
		firstSeenSeq, claimed := chainBound.references[ledger][expected.reference]
		if !claimed || firstSeenSeq >= seq {
			return
		}

		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
			fmt.Sprintf("log %d on ledger %q is not an OrderSkipped projection but the chain-bound order opted into TRANSACTION_REFERENCE_CONFLICT skip and reference %q was already claimed at sequence %d", seq, ledger, expected.reference, firstSeenSeq),
			seq, ledger, "", "",
		))

	case slices.Contains(expected.reasons, commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED) && expected.transactionID != 0:
		firstRevertSeq, seen := chainBound.reverted[ledger][expected.transactionID]
		if !seen || firstRevertSeq >= seq {
			return
		}

		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
			fmt.Sprintf("log %d on ledger %q is not an OrderSkipped projection but the chain-bound order opted into TRANSACTION_ALREADY_REVERTED skip and tx %d was already reverted at sequence %d", seq, ledger, expected.transactionID, firstRevertSeq),
			seq, ledger, "", "",
		))

	case slices.Contains(expected.reasons, commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND) && expected.metadataKey != "":
		present, witnessed := mutationStateWithWitness(chainBound.metadata[ledger][expected.metadataTarget][expected.metadataKey], seq)
		if present {
			// Live shows PRESENT → a Delete would have succeeded, not
			// skipped. Any non-skip projection is a legitimate landing.
			return
		}

		if !witnessed && hasArchivedChapters {
			// Empty live timeline under archives is ambiguous — an
			// archived Set could make the delete succeed, so the
			// projection may legitimately be a non-skip. Be permissive.
			return
		}

		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
			fmt.Sprintf("log %d on ledger %q is not an OrderSkipped projection but the chain-bound order opted into METADATA_NOT_FOUND skip and (target=%q, key=%q) was absent at this sequence", seq, ledger, expected.metadataTarget, expected.metadataKey),
			seq, ledger, "", "",
		))

	case (slices.Contains(expected.reasons, commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS) ||
		slices.Contains(expected.reasons, commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND)) && expected.accountTypeName != "":
		present, witnessed := mutationStateWithWitness(chainBound.accountTypes[ledger][expected.accountTypeName], seq)

		mustBePresent := slices.Contains(expected.reasons, commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS)
		if present != mustBePresent {
			// Chain-bound state contradicts the reason that would
			// have been skipped: no elision to prove.
			return
		}

		if !witnessed && hasArchivedChapters {
			// Empty live timeline under archives is ambiguous. The
			// archived history could make either outcome legitimate.
			return
		}

		callback(errorEvent(
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
			fmt.Sprintf("log %d on ledger %q is not an OrderSkipped projection but the chain-bound order opted into %s skip and account type %q was in the expected state at this sequence", seq, ledger, elidedReasonForAccountType(mustBePresent), expected.accountTypeName),
			seq, ledger, "", "",
		))
	}
}

// elidedReasonForAccountType returns the reason string that would have
// been surfaced by an OrderSkipped log on the elided account-type
// operation. Used only in the inverse-direction error message.
func elidedReasonForAccountType(present bool) string {
	if present {
		return "ACCOUNT_TYPE_ALREADY_EXISTS"
	}

	return "ACCOUNT_TYPE_NOT_FOUND"
}

// idemExpectedKey identifies the frozen idempotency entry an audit outcome
// should have produced. created_at equals the freezing audit entry's timestamp,
// so it pins a stored entry to exactly one outcome even when a key is re-frozen
// after a prior outcome expired.
type idemExpectedKey struct {
	keyHash   attributes.U128
	createdAt uint64
}

// expectedIdempotency is the SubIdempKeys value re-derived from a hash-chained
// audit outcome.
type expectedIdempotency struct {
	proposalHash []byte
	failure      bool
	reason       commonpb.ErrorReason
	message      string
	metadata     map[string]string
	firstLog     uint64
	logCount     uint32
}

// expectedIdempotencyOutcome re-derives the frozen idempotency value a keyed
// proposal would have written, from its audit entry + items. ok is false when
// the proposal froze nothing under its key: an all-replay success (no log
// produced) or a non-freezable failure (retryable/internal) — neither writes
// SubIdempKeys.
func expectedIdempotencyOutcome(entry *auditpb.AuditEntry, items []*auditpb.AuditItem) (expectedIdempotency, bool) {
	switch out := entry.GetOutcome().(type) {
	case *auditpb.AuditEntry_Failure:
		reason := out.Failure.GetReason()
		if !domain.IsFreezableFailure(domain.KindForReason(reason)) {
			return expectedIdempotency{}, false
		}

		return expectedIdempotency{
			proposalHash: recomputeProposalHash(items),
			failure:      true,
			reason:       reason,
			message:      out.Failure.GetMessage(),
			metadata:     out.Failure.GetContext(),
		}, true
	case *auditpb.AuditEntry_Success:
		maxSeq := out.Success.GetMaxLogSequence()
		if maxSeq == 0 {
			return expectedIdempotency{}, false
		}

		minSeq := out.Success.GetMinLogSequence()

		return expectedIdempotency{
			proposalHash: recomputeProposalHash(items),
			firstLog:     minSeq,
			logCount:     uint32(maxSeq - minSeq + 1),
		}, true
	default:
		return expectedIdempotency{}, false
	}
}

// recomputeProposalHash re-derives a proposal's idempotency hash from its
// persisted audit orders, reusing the FSM's hashing so the result is
// byte-identical to what was frozen. The orders round-trip from the chain-bound
// serialized_order bytes; a corrupt order would already have broken the audit
// chain above, so a nil here only forces a loud hash mismatch.
func recomputeProposalHash(items []*auditpb.AuditItem) []byte {
	orders := make([]*raftcmdpb.Order, 0, len(items))

	for _, item := range items {
		order := &raftcmdpb.Order{}
		if err := order.UnmarshalVT(item.GetSerializedOrder()); err != nil {
			return nil
		}

		orders = append(orders, order)
	}

	return processing.HashOrders(orders)
}

// reDeriveArchivedIdempotency extends `expected` with the frozen idempotency
// outcomes re-derived from ARCHIVED audit entries whose timestamp is within the
// TTL window [ttlCutoff, ∞) — the only archived freezes that can still be live.
// It returns true when every archived chapter that could hold such a freeze was
// read, so the caller may lower its report floor to ttlCutoff; false when cold
// storage is unavailable or a read failed, in which case the caller keeps the
// post-archive boundary and leaves the residual gap rather than risk a false
// positive.
//
// Unlike the post-archive range, these cold entries are NOT re-verified against
// the hash chain here. Cold storage sits outside the follower-disk threat model
// this pass targets (see compareIdempotencyOutcomes), so the archived entry is
// taken as the trusted source and only the live SubIdempKeys projection is
// checked against it. Widening the threat model to cover cold-storage tampering
// would require re-walking the chain over this same bounded window.
//
// The read is bounded by the TTL window, not by history: chapters are visited
// newest-first and the scan stops at the first one whose newest entry predates
// ttlCutoff.
func (c *Checker) reDeriveArchivedIdempotency(
	ctx context.Context,
	chapters []*commonpb.Chapter,
	ttlCutoff uint64,
	expected map[idemExpectedKey]expectedIdempotency,
) bool {
	archived := make([]*commonpb.Chapter, 0, len(chapters))

	for _, ch := range chapters {
		if ch.GetStatus() == commonpb.ChapterStatus_CHAPTER_ARCHIVED {
			archived = append(archived, ch)
		}
	}

	// No archived data: the verified (hot) range already spans the whole audit
	// history, so coverage extends down to ttlCutoff without any cold read.
	if len(archived) == 0 {
		return true
	}

	if c.coldReader == nil {
		return false
	}

	// ttlCutoff == 0 means an unbounded window (idempotency-ttl=0 never-expire,
	// or a ledger younger than its TTL): the whole archived history is read.
	// Flag it — the O(history) read is by configuration, not a bug.
	if ttlCutoff == 0 {
		c.logger.Infof("idempotency TTL window is unbounded; scanning all %d archived chapters to verify frozen outcomes", len(archived))
	}

	// Newest first, so the scan can stop at the first chapter whose newest entry
	// predates the cutoff.
	slices.SortFunc(archived, func(a, b *commonpb.Chapter) int {
		return cmp.Compare(b.GetCloseAuditSequence(), a.GetCloseAuditSequence())
	})

	for _, ch := range archived {
		coldPebble, err := c.coldReader.GetReader(ctx, ch.GetId())
		if err != nil {
			c.logger.Infof("reading archived chapter %d for idempotency window failed: %v", ch.GetId(), err)

			return false
		}

		last, err := query.ReadLastAuditEntry(coldPebble)
		if err != nil {
			c.logger.Infof("reading last audit entry of archived chapter %d failed: %v", ch.GetId(), err)

			return false
		}

		// This chapter (and, by audit-sequence order, every older one) is
		// entirely below the TTL window — nothing here can still be live.
		if last == nil || last.GetTimestamp().GetData() < ttlCutoff {
			break
		}

		windowStartsHere, err := c.collectChapterIdempotency(ctx, coldPebble, ttlCutoff, expected)
		if err != nil {
			c.logger.Infof("re-deriving idempotency from archived chapter %d failed: %v", ch.GetId(), err)

			return false
		}

		if windowStartsHere {
			break
		}
	}

	return true
}

// collectChapterIdempotency scans one archived chapter's audit entries (read
// from cold storage), re-derives the frozen idempotency outcome for every keyed
// entry at/after ttlCutoff, and merges it into expected. It returns true when
// the chapter's oldest entry predates ttlCutoff — the window starts inside this
// chapter, so older chapters need not be read.
//
// Scan order does not matter here: every in-window keyed entry is added
// regardless of direction, and windowStartsHere only needs to observe whether
// any entry predates the cutoff — so this is correct even if ReadAuditEntries'
// ordering ever changes.
func (c *Checker) collectChapterIdempotency(
	ctx context.Context,
	coldPebble dal.PebbleReader,
	ttlCutoff uint64,
	expected map[idemExpectedKey]expectedIdempotency,
) (bool, error) {
	cur, err := query.ReadAuditEntries(ctx, coldPebble, nil)
	if err != nil {
		return false, fmt.Errorf("reading archived audit entries: %w", err)
	}

	defer func() { _ = cur.Close() }()

	windowStartsHere := false

	for {
		entry, err := cur.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return false, fmt.Errorf("reading archived audit entry: %w", err)
		}

		ts := entry.GetTimestamp().GetData()
		if ts < ttlCutoff {
			windowStartsHere = true

			continue
		}

		key := entry.GetIdempotency().GetKey()
		if key == "" {
			continue
		}

		items, err := query.ReadAuditItems(ctx, coldPebble, entry.GetSequence())
		if err != nil {
			return false, fmt.Errorf("reading archived audit items for sequence %d: %w", entry.GetSequence(), err)
		}

		if exp, ok := expectedIdempotencyOutcome(entry, items); ok {
			expected[idemExpectedKey{
				keyHash:   state.HashIdempotencyKey(key),
				createdAt: ts,
			}] = exp
		}
	}

	return windowStartsHere, nil
}

// compareIdempotencyOutcomes scans the frozen idempotency entries
// (SubIdempKeys) and verifies each against the outcome re-derived from the
// hash-chained audit entry that wrote it. A divergence is a tampered replay
// cache — left unchecked, a duplicate caller would replay an arbitrary error or
// wrong log range while Check() passed.
//
// Entries are matched by (key hash, created_at). `expected` is built to be
// complete at/above idemReportFloor: the post-archive range is always
// re-derived, and the still-live slice frozen by archived entries within the
// idempotency TTL window ([ttlCutoff, boundary)) is re-derived from cold storage
// when available (see reDeriveArchivedIdempotency). A stored entry whose
// created_at is at/above the floor but matches no freeze is therefore a tampered
// created_at or a fabricated entry and is reported. Below the floor the freezing
// audit entry is older than the TTL window — no longer live, and not
// re-derived — so the entry is skipped. A nil floor means nothing was
// re-derivable (no verified range), so all entries are skipped; a non-nil floor
// of 0 means the window is unbounded (never-expire), so every entry is checked.
//
// This pass verifies the INTEGRITY of the entries that are stored — it does not
// detect a DELETED entry. A frozen outcome that is simply absent cannot be
// distinguished from one legitimately evicted at its TTL: eviction is applied by
// IdempotencyEviction, which writes no audit record (see applyIdempotencyEviction),
// so the audit log cannot prove an entry "should still be there". Detecting a
// deleted entry (which would let a retry re-execute instead of replay) is a
// separate concern out of scope here.
//
// Threat model: the check targets an actor with direct disk/Pebble write access
// to a follower's store, which is where SubIdempKeys lives. The post-archive
// audit entries that anchor the live range are hash-chain-verified above; the
// archived entries used for the TTL window are trusted as-is because cold
// storage is outside that follower-disk reach (see reDeriveArchivedIdempotency).
//
// Coverage frontier — the report floor depends on two inputs beyond the audit
// chain:
//   - "now" (verifiedRangeEndTs) is the highest hash-chain-verified timestamp,
//     NOT a projection — a tampered lastAppliedTimestamp cannot move the floor.
//   - the idempotency TTL is taken from the boot-validated runtime config when
//     available (in process memory, off the audited disk), falling back to the
//     PersistedConfig projection only where no runtime config exists (CLI /
//     restore backup validation). On those fallback paths a disk-tampered TTL
//     could move the floor up until the next boot revalidates config; the TTL
//     is boot config, not an audit projection, so the checker cannot re-derive
//     it from the chain.
func (c *Checker) compareIdempotencyOutcomes(
	reader dal.PebbleReader,
	expected map[idemExpectedKey]expectedIdempotency,
	idemReportFloor *uint64,
	callback func(*servicepb.CheckStoreEvent),
) error {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.ZoneIdempotency, dal.SubIdempKeys},
		UpperBound: []byte{dal.ZoneIdempotency, dal.SubIdempKeys + 1},
	})
	if err != nil {
		return fmt.Errorf("scanning idempotency keys: %w", err)
	}

	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) != 2+16 {
			continue
		}

		var stored commonpb.IdempotencyKeyValue
		if err := stored.UnmarshalVT(iter.Value()); err != nil {
			return fmt.Errorf("unmarshalling idempotency value: %w", err)
		}

		exp, ok := expected[idemExpectedKey{
			keyHash:   attributes.U128FromBytes(key[2:18]),
			createdAt: stored.GetCreatedAt(),
		}]
		if !ok {
			// No matching freeze. `expected` is complete at/above the report
			// floor, so a miss there is a tampered created_at or a fabricated
			// entry. Below the floor the freezing entry is older than the TTL
			// window (or pre-archive when cold storage was unavailable) and is
			// not re-derived, so the entry is skipped rather than flagged.
			if idemReportFloor != nil && stored.GetCreatedAt() >= *idemReportFloor {
				callback(errorEvent(
					servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH,
					fmt.Sprintf("frozen idempotency outcome (created_at=%d) has no matching audit entry — tampered created_at or fabricated entry",
						stored.GetCreatedAt()),
					0, "", "", "",
				))
			}

			continue
		}

		if msg := idempotencyMismatch(&stored, exp); msg != "" {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH,
				fmt.Sprintf("frozen idempotency outcome (created_at=%d) diverges from its audit entry: %s",
					stored.GetCreatedAt(), msg),
				0, "", "", "",
			))
		}
	}

	return nil
}

// idempotencyMismatch returns a human-readable reason the stored frozen outcome
// diverges from the audit-derived expectation, or "" when they agree.
func idempotencyMismatch(stored *commonpb.IdempotencyKeyValue, exp expectedIdempotency) string {
	if !bytes.Equal(stored.GetHash(), exp.proposalHash) {
		return fmt.Sprintf("proposal hash %x does not match audit-derived %x", stored.GetHash(), exp.proposalHash)
	}

	if exp.failure {
		f := stored.GetFailure()
		switch {
		case f == nil:
			return "stored a success outcome where the audit recorded a failure"
		case f.GetReason() != exp.reason:
			return fmt.Sprintf("failure reason %s does not match audit %s", f.GetReason(), exp.reason)
		case f.GetMessage() != exp.message:
			return "failure message does not match the audit"
		case !metadataEqual(f.GetMetadata(), exp.metadata):
			return "failure metadata does not match the audit"
		default:
			return ""
		}
	}

	switch {
	case stored.GetFailure() != nil:
		return "stored a failure outcome where the audit recorded a success"
	case stored.GetFirstLogSequence() != exp.firstLog || stored.GetLogCount() != exp.logCount:
		return fmt.Sprintf("log range (first=%d count=%d) does not match audit (first=%d count=%d)",
			stored.GetFirstLogSequence(), stored.GetLogCount(), exp.firstLog, exp.logCount)
	default:
		return ""
	}
}

// metadataEqual compares two metadata maps treating nil and empty as equal:
// buildAuditFailure stores an empty (non-nil) context map while
// recordIdempotencyFailure may store a nil metadata map for the same error.
func metadataEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if b[k] != v {
			return false
		}
	}

	return true
}

type proposalBoundaryReader struct {
	auditCursor cursor.Cursor[*auditpb.AuditEntry]
	tracker     *domainreplay.ProposalBoundaryTracker
}

func (c *Checker) newProposalBoundaryReader(
	ctx context.Context,
	reader dal.PebbleReader,
	chapters []*commonpb.Chapter,
	archiveEndSeq uint64,
) (*proposalBoundaryReader, error) {
	var afterAuditSeq *uint64

	for _, p := range chapters {
		if p.GetStatus() == commonpb.ChapterStatus_CHAPTER_ARCHIVED {
			closeAuditSeq := p.GetCloseAuditSequence()
			if afterAuditSeq == nil || closeAuditSeq > *afterAuditSeq {
				afterAuditSeq = &closeAuditSeq
			}
		}
	}

	auditCursor, err := query.ReadAuditEntries(ctx, reader, afterAuditSeq)
	if err != nil {
		return nil, fmt.Errorf("reading audit entries: %w", err)
	}

	return &proposalBoundaryReader{
		auditCursor: auditCursor,
		tracker:     domainreplay.NewProposalBoundaryTracker(archiveEndSeq),
	}, nil
}

func (r *proposalBoundaryReader) Next() (uint64, bool, error) {
	for {
		entry, err := r.auditCursor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0, false, nil
			}

			return 0, false, fmt.Errorf("reading audit entry: %w", err)
		}

		success := entry.GetSuccess()
		if success == nil || success.GetMaxLogSequence() == 0 {
			continue
		}

		if boundary, ok := r.tracker.Accept(success.GetMaxLogSequence()); ok {
			return boundary, true, nil
		}
	}
}

func (r *proposalBoundaryReader) Close() error {
	if r == nil || r.auditCursor == nil {
		return nil
	}

	return r.auditCursor.Close()
}

// logSequenceFromAuditEntry extracts a representative log sequence from an
// audit entry for error reporting. Returns 0 for failure entries.
func logSequenceFromAuditEntry(entry *auditpb.AuditEntry) uint64 {
	if success := entry.GetSuccess(); success != nil {
		return success.GetMinLogSequence()
	}

	return 0
}

// verifySealingHash checks that the sealing hash of an archived chapter matches
// the expected decomposition: BLAKE3(chapter_id || close_sequence || last_log_hash || state_hash).
func verifySealingHash(p *commonpb.Chapter, callback func(*servicepb.CheckStoreEvent)) {
	hasher := blake3.New()
	buf := make([]byte, 8)

	binary.BigEndian.PutUint64(buf, p.GetId())
	_, _ = hasher.Write(buf)

	binary.BigEndian.PutUint64(buf, p.GetCloseSequence())
	_, _ = hasher.Write(buf)

	if len(p.GetLastAuditHash()) > 0 {
		_, _ = hasher.Write(p.GetLastAuditHash())
	}

	_, _ = hasher.Write(p.GetStateHash())

	expected := hasher.Sum(nil)
	if !bytes.Equal(expected, p.GetSealingHash()) {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH,
			fmt.Sprintf("sealing hash mismatch for archived chapter %d: expected %x, got %x",
				p.GetId(), expected, p.GetSealingHash()),
			p.GetCloseSequence(), "", "", ""))
	}
}
func errorEvent(errorType servicepb.CheckStoreErrorType, message string, logSequence uint64, ledger, account, asset string) *servicepb.CheckStoreEvent {
	return &servicepb.CheckStoreEvent{
		Type: &servicepb.CheckStoreEvent_Error{
			Error: &servicepb.CheckStoreError{
				ErrorType:   errorType,
				Message:     message,
				LogSequence: logSequence,
				Ledger:      ledger,
				Account:     account,
				Asset:       asset,
			},
		},
	}
}

func errorEventWithTx(errorType servicepb.CheckStoreErrorType, message, ledger string, txID uint64) *servicepb.CheckStoreEvent {
	return &servicepb.CheckStoreEvent{
		Type: &servicepb.CheckStoreEvent_Error{
			Error: &servicepb.CheckStoreError{
				ErrorType:     errorType,
				Message:       message,
				Ledger:        ledger,
				TransactionId: txID,
			},
		},
	}
}

// checkReversionInvariants tracks transaction IDs and validates reversion invariants
// during log replay.
func checkReversionInvariants(
	ledgerName string,
	seq uint64,
	payload *commonpb.LedgerLogPayload,
	knownTxIDs map[string]*bitset.Bitset,
	revertedTxIDs map[string]*bitset.Bitset,
	callback func(*servicepb.CheckStoreEvent),
) {
	switch p := payload.GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if p.CreatedTransaction != nil && p.CreatedTransaction.GetTransaction() != nil {
			trackTxID(knownTxIDs, ledgerName, p.CreatedTransaction.GetTransaction().GetId())
		}

	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if p.RevertedTransaction == nil {
			return
		}

		revertedID := p.RevertedTransaction.GetRevertedTransactionId()

		// Check that the target transaction exists
		bs := knownTxIDs[ledgerName]
		if bs == nil || !bs.Test(revertedID) {
			callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERTED_MISMATCH,
				fmt.Sprintf("log %d reverts non-existent transaction %d in ledger %q", seq, revertedID, ledgerName),
				ledgerName, revertedID))
		}

		// Check that the transaction is not already reverted
		rbs := revertedTxIDs[ledgerName]
		if rbs != nil && rbs.Test(revertedID) {
			callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERTED_MISMATCH,
				fmt.Sprintf("log %d double-reverts transaction %d in ledger %q", seq, revertedID, ledgerName),
				ledgerName, revertedID))
		}

		// Mark the transaction as reverted
		trackTxID(revertedTxIDs, ledgerName, revertedID)

		// Track the revert transaction's own ID
		if p.RevertedTransaction.GetRevertTransaction() != nil {
			trackTxID(knownTxIDs, ledgerName, p.RevertedTransaction.GetRevertTransaction().GetId())
		}
	}
}

// normalizeTransactionState replaces an empty metadata map with nil so that
// proto.Equal treats both representations as equivalent.
func normalizeTransactionState(s *commonpb.TransactionState) {
	if s.GetMetadata() != nil && len(s.GetMetadata()) == 0 {
		s.Metadata = nil
	}
}

func trackTxID(m map[string]*bitset.Bitset, ledgerName string, txID uint64) {
	bs := m[ledgerName]
	if bs == nil {
		bs = &bitset.Bitset{}
		m[ledgerName] = bs
	}

	bs.Set(txID)
}
