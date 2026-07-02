package check

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"slices"

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
}

// NewChecker creates a new Checker. clusterID is used to derive the
// per-cluster key for verifying audit-hash chain entries — it must match
// the value the FSM used when writing those entries (enforced via
// PersistedConfig immutability).
func NewChecker(store *dal.Store, attrs *attributes.Attributes, clusterID string, logger logging.Logger) *Checker {
	return &Checker{
		store:     store,
		attrs:     attrs,
		logger:    logger,
		clusterID: clusterID,
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

	// Verify the audit hash chain before log replay. This iterates all
	// non-archived audit entries and recomputes each hash from the stored
	// orders, chaining from archiveLastAuditHash. Returns:
	//   - expectedSkippable: per-log-seq skippable_reasons + correlator
	//   - chainBoundReferences: per-ledger references claimed in the audit
	// Both feed verifySkippedOrder during the log iteration loop below.
	expectedSkippable, chainBoundReferences, err := c.verifyAuditHashChain(ctx, snap, chapters, archiveLastAuditHash, callback)
	if err != nil {
		return fmt.Errorf("verifying audit hash chain: %w", err)
	}

	// Open baseline checkpoint for archived state comparison. The baseline
	// is the pre-archive Pebble snapshot the checker consults to verify
	// archived data (volumes, metadata, transactions, and now references
	// to scope the OrderSkipped archive escape). Opening it here — before
	// log iteration — lets foldBaselineReferences populate
	// chainBoundReferences in time for verifySkippedOrder, while the
	// later compareVolumes / compareMetadata / compareTransactions pass
	// reuses the same handle.
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

	// Fold the baseline TransactionReference attribute into
	// chainBoundReferences with a sentinel sequence of 0 (precedes every
	// live log seq). This is what lets verifySkippedOrder validate skips
	// that conflict with a reference claimed in a purged chapter without
	// falling back to the broad archive escape hatch — the escape now
	// scopes only to the "no baseline available" case.
	baselineReferencesAvailable, err := c.foldBaselineReferences(baselineDB, chainBoundReferences)
	if err != nil {
		return fmt.Errorf("loading baseline references: %w", err)
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

						verifySkippedOrder(ledgerName, seq, payload.Apply.GetLog().GetData(), expectedSkippable, chainBoundReferences, hasArchivedChapters && !baselineReferencesAvailable, callback)

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
		dispatchElisionCheck(seq, log, expectedSkippable, chainBoundReferences, callback)

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
// Returns:
//
//   - expectedSkippable: per-log-seq skippable_reasons whitelist + reason
//     correlator re-derived from the chain-bound Orders. Consumed by
//     verifySkippedOrder.
//   - chainBoundReferences: per-ledger map of every CreateTransactionOrder
//     reference seen in the audit chain, with the first sequence that
//     claimed it. Built from chain-bound `serialized_order` so a tampered
//     CreatedTransaction LedgerLog cannot forge a "prior claim" for a fake
//     skip.
//
// The LedgerLog projection is not hash-chain bound, so without these
// checks a tampered skip log could let a fabricated outcome slip past
// Check().
func (c *Checker) verifyAuditHashChain(
	ctx context.Context,
	reader dal.PebbleReader,
	chapters []*commonpb.Chapter,
	archiveLastAuditHash []byte,
	callback func(*servicepb.CheckStoreEvent),
) (map[uint64]*expectedSkippableOrder, map[string]map[string]uint64, error) {
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
		return nil, nil, fmt.Errorf("reading audit entries: %w", err)
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
		// Per-ledger map of every CreateTransactionOrder reference seen in
		// the audit chain (regardless of outcome), with the first log
		// sequence that claimed it. Built from chain-bound serialized_order
		// so the verifier never trusts a LedgerLog projection for the
		// "prior claim" replay — a tampered CreatedTransaction log cannot
		// inject a fake reference here. Limitation: references claimed by
		// archived orders (audit purged) are NOT in this map; the caller
		// pairs the lookup with hasArchivedChapters to avoid false
		// positives on legitimate skips against archived references.
		chainBoundReferences = make(map[string]map[string]uint64)
		// Timestamp of the first (lowest-sequence) verified entry — the archive
		// boundary. HLC timestamps are monotonic with sequence, so every freeze
		// in the verified range is at/after it; a stored idempotency entry that
		// claims a created_at >= this but matches no expected freeze is tampered
		// or fabricated (see compareIdempotencyOutcomes). 0 = no verified entry.
		verifiedRangeStartTs uint64
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
			return nil, nil, fmt.Errorf("reading audit entry for hash chain verification: %w", err)
		}

		if verifiedRangeStartTs == 0 {
			verifiedRangeStartTs = entry.GetTimestamp().GetData()
		}

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
			return expectedSkippable, chainBoundReferences, nil
		}

		// Read the audit items for this entry, then rebuild the canonical
		// per-item bytes that fed the hash chain at apply time. Combined
		// with the rebuilt header payload (which binds every other
		// AuditEntry field via state.BuildHashedHeaderPayload), the hash
		// pre-image is reconstructed from the stored fields — no proto
		// re-marshalling, immune to vtprotobuf and Order schema drift.
		items, itemsErr := query.ReadAuditItems(ctx, reader, entry.GetSequence())
		if itemsErr != nil {
			return nil, nil, fmt.Errorf("reading audit items for sequence %d: %w", entry.GetSequence(), itemsErr)
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
			return expectedSkippable, chainBoundReferences, nil
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
			return expectedSkippable, chainBoundReferences, nil // Stop on first mismatch — chain is broken from here.
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
			collectExpectedSkippable(items, expectedSkippable, chainBoundReferences)
		}
	}

	if checked > 0 {
		c.logger.Infof("Audit hash chain verified: %d entries checked", checked)
	}

	if err := c.compareIdempotencyOutcomes(reader, expectedIdem, verifiedRangeStartTs, callback); err != nil {
		return nil, nil, err
	}

	return expectedSkippable, chainBoundReferences, nil
}

// expectedSkippableOrder captures the chain-verified fields the checker
// needs to confirm a LedgerLogPayload.OrderSkipped projection is legitimate:
// the audit-bound reasons whitelist (any skip must use one of these) and
// the reason-specific correlator the verifier replays against the projection
// stream. Today only TRANSACTION_REFERENCE_CONFLICT carries a correlator
// (Reference + LedgerName); other reasons leave both empty.
type expectedSkippableOrder struct {
	reasons   []commonpb.ErrorReason
	ledger    string
	reference string
}

// collectExpectedSkippable populates two outputs from the chain-verified
// items of a successful audit entry:
//
//   - expectedSkippable: per-log skippable_reasons whitelist + correlator for
//     orders that opted into skip. Each item maps to the log at
//     item.LogSequence (which buildAuditItems sets from either the fresh
//     CreatedLog or the idempotency-replay ReferenceSequence). MinLogSequence
//
//   - i is the wrong formula when an item is a ReferenceSequence —
//     succeeding indexes do not line up with seq + i.
//
//   - chainBoundReferences: per-ledger map of every CreateTransactionOrder
//     reference seen in the audit chain, with the first sequence that
//     claimed it. Built from chain-bound `serialized_order` (NOT the
//     LedgerLog projection) so a tampered CreatedTransaction log cannot
//     forge a "prior claim" for a fake skip — see
//     verifySkippedOrder's TRANSACTION_REFERENCE_CONFLICT branch.
func collectExpectedSkippable(
	items []*auditpb.AuditItem,
	expectedSkippable map[uint64]*expectedSkippableOrder,
	chainBoundReferences map[string]map[string]uint64,
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
			rememberReferenceClaim(chainBoundReferences, ledger, ref, logSeq)
		}

		if mi := ls.GetMirrorIngest(); mi != nil {
			if mct := mi.GetEntry().GetCreatedTransaction(); mct != nil {
				if ref := mct.GetReference(); ref != "" {
					rememberReferenceClaim(chainBoundReferences, ledger, ref, logSeq)
				}
			}
		}

		reasons := order.GetSkippableReasons()
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

		expectedSkippable[logSeq] = exp
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
	chainBoundReferences map[string]map[string]uint64,
	hasArchivedChapters bool,
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

	// Reason-specific replay: confirm the underlying condition was
	// plausible at this sequence given the chain-bound order.
	if reason == commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT {
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

		// expected.ledger comes from the chain-bound order; cross-check it
		// against the log's ledger to catch a tampered ApplyLedgerLog
		// envelope that points the skip at a different ledger.
		if expected.ledger != ledger {
			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records OrderSkipped on ledger %q but the chain-bound order targets ledger %q", seq, ledger, expected.ledger),
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
		// `ledger` and `reference`, so missing or empty values are also
		// tampering — flag them. ExistingTransactionId is not in the
		// audit chain (tx ids are FSM-allocated and not re-derivable
		// from the chain alone), so the verifier only pins the two
		// fields it can re-derive.
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

		// Look up the reference in the audit-derived map. firstSeenSeq is
		// strictly less than seq because chainBoundReferences only retains
		// the first claim for each reference (re-claims on the same ref
		// later — including the very order producing this skip log — do
		// not move it).
		firstSeenSeq, claimed := chainBoundReferences[ledger][expected.reference]
		if !claimed || firstSeenSeq >= seq {
			// No earlier claim visible in the live audit range. If
			// archived chapters exist, the claim may live in a purged
			// range we cannot re-verify here; stay permissive to avoid
			// flagging legitimate skips on archived references. If no
			// archive boundary applies, the missing claim IS the
			// fabrication we want to catch — fail loudly.
			if hasArchivedChapters {
				return
			}

			callback(errorEvent(
				servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
				fmt.Sprintf("log %d records TRANSACTION_REFERENCE_CONFLICT skip but reference %q was not claimed on ledger %q before this sequence", seq, expected.reference, ledger),
				seq, ledger, "", "",
			))

			return
		}
	}
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
	chainBoundReferences map[string]map[string]uint64,
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

	verifyExpectedSkipNotElided(expected.ledger, seq, expectedSkippable, chainBoundReferences, callback)
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
	chainBoundReferences map[string]map[string]uint64,
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

	// Only TRANSACTION_REFERENCE_CONFLICT is whitelisted today. Reasons added
	// later need their own re-derivation here — fail closed: skip the inverse
	// check rather than guess, the forward direction still catches a forged
	// OrderSkipped.
	if !slices.Contains(expected.reasons, commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT) || expected.reference == "" {
		return
	}

	firstSeenSeq, claimed := chainBoundReferences[ledger][expected.reference]
	if !claimed || firstSeenSeq >= seq {
		// No earlier claim visible in the live audit range. Either the
		// reference is genuinely new at this sequence (legitimate non-skip)
		// or the prior claim lives in a purged archive we cannot re-verify.
		// Either way, stay permissive — the forward direction still catches
		// a forged OrderSkipped if the projection is a skip.
		return
	}

	callback(errorEvent(
		servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_INVALID_SKIP,
		fmt.Sprintf("log %d on ledger %q is not an OrderSkipped projection but the chain-bound order opted into TRANSACTION_REFERENCE_CONFLICT skip and reference %q was already claimed at sequence %d", seq, ledger, expected.reference, firstSeenSeq),
		seq, ledger, "", "",
	))
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

// compareIdempotencyOutcomes scans the frozen idempotency entries
// (SubIdempKeys) and verifies each against the outcome re-derived from the
// hash-chained audit entry that wrote it. A divergence is a tampered replay
// cache — left unchecked, a duplicate caller would replay an arbitrary error or
// wrong log range while Check() passed.
//
// Entries are matched by (key hash, created_at). A miss is classified by
// verifiedRangeStartTs (the archive boundary): an entry whose created_at is
// at/after the boundary must have its freeze in the verified range, so a miss
// there is tampered created_at or a fabricated entry and is reported; an entry
// before the boundary was frozen by an already-archived entry that is no longer
// re-derivable here, so it is skipped — the same scoping the hash-chain
// verification uses. This closes the gap where tampering the created_at of a
// live, post-boundary entry would otherwise dodge the lookup and skip
// verification entirely. (When verifiedRangeStartTs is 0 — no verified entries —
// nothing is re-derivable, so all entries are skipped.)
//
// Residual limitation: a stored entry created before the boundary is skipped
// even when it is still live — reachable only when the idempotency TTL outlives
// the age at which chapters are archived. In that window a tampered outcome on
// such an entry goes undetected here; fully closing it requires re-deriving the
// archived freezes from cold storage (the same boundary that already scopes the
// hash-chain verification).
func (c *Checker) compareIdempotencyOutcomes(
	reader dal.PebbleReader,
	expected map[idemExpectedKey]expectedIdempotency,
	verifiedRangeStartTs uint64,
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
			// No matching freeze. If the entry claims a created_at at/after the
			// archive boundary, its freeze must be in the verified range — its
			// absence is tampering or fabrication. Older entries are archived
			// (not re-derivable here) and are skipped.
			if verifiedRangeStartTs != 0 && stored.GetCreatedAt() >= verifiedRangeStartTs {
				callback(errorEvent(
					servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_IDEMPOTENCY_MISMATCH,
					fmt.Sprintf("frozen idempotency outcome (created_at=%d) has no matching audit entry in the verified range — tampered created_at or fabricated entry",
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
