package check

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"

	"github.com/cockroachdb/pebble/v2"
	"github.com/zeebo/blake3"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
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

	// Verify the audit hash chain before log replay.
	// This iterates all non-archived audit entries and recomputes each hash
	// from the stored orders, chaining from archiveLastAuditHash.
	if err := c.verifyAuditHashChain(ctx, snap, chapters, archiveLastAuditHash, callback); err != nil {
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
					delete(knownLedgers, payload.DeleteLedger.GetName())
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

						checkReversionInvariants(ledgerName, seq, payload.Apply.GetLog().GetData(), ledgerKnownTxIDs, ledgerRevertedTxIDs, callback)

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

	// Open baseline checkpoint for archived state comparison.
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

	// If archived chapters exist but no baseline is available, we can't do
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
func (c *Checker) verifyAuditHashChain(
	ctx context.Context,
	reader dal.PebbleReader,
	chapters []*commonpb.Chapter,
	archiveLastAuditHash []byte,
	callback func(*servicepb.CheckStoreEvent),
) error {
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
		return fmt.Errorf("reading audit entries: %w", err)
	}

	defer func() { _ = auditCursor.Close() }()

	var (
		lastHash   = archiveLastAuditHash
		hashBuf    []byte
		checked    uint64
		generators = make(map[uint32]processing.HashGenerator, 2)
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
			return fmt.Errorf("reading audit entry for hash chain verification: %w", err)
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

			return nil
		}

		// Read the audit items for this entry, then rebuild the canonical
		// per-item bytes that fed the hash chain at apply time. Combined
		// with the rebuilt header payload (which binds every other
		// AuditEntry field via state.BuildHashedHeaderPayload), the hash
		// pre-image is reconstructed from the stored fields — no proto
		// re-marshalling, immune to vtprotobuf and Order schema drift.
		items, itemsErr := query.ReadAuditItems(ctx, reader, entry.GetSequence())
		if itemsErr != nil {
			return fmt.Errorf("reading audit items for sequence %d: %w", entry.GetSequence(), itemsErr)
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

			return nil
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

			return nil // Stop on first mismatch — chain is broken from here.
		}

		lastHash = entry.GetHash()
		checked++
	}

	if checked > 0 {
		c.logger.Infof("Audit hash chain verified: %d entries checked", checked)
	}

	return nil
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
