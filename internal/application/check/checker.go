package check

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/cockroachdb/pebble"
	"github.com/zeebo/blake3"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

const progressInterval = 100

// Checker verifies store integrity by replaying logs and comparing derived state.
type Checker struct {
	store *dal.Store
	attrs *attributes.Attributes
}

// NewChecker creates a new Checker.
func NewChecker(store *dal.Store, attrs *attributes.Attributes) *Checker {
	return &Checker{
		store: store,
		attrs: attrs,
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
// 7. Archived period sealing hash decomposition
// 8. Archived state via baseline checkpoint + 3-way merge comparison.
func (c *Checker) Check(ctx context.Context, callback func(*servicepb.CheckStoreEvent)) error {
	lastSequence, err := query.ReadLastSequence(c.store)
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

	// Read archived periods to adjust the starting point for log replay.
	periodsCursor, err := query.ReadPeriods(ctx, c.store)
	if err != nil {
		return fmt.Errorf("reading periods: %w", err)
	}

	periods, err := dal.Collect(periodsCursor)
	if err != nil {
		return fmt.Errorf("collecting periods: %w", err)
	}

	var (
		hasArchivedPeriods bool
		archiveEndSeq      uint64 // max close_sequence among archived periods
		archiveLastHash    []byte // last_log_hash from the latest archived period
	)

	for _, p := range periods {
		if p.GetStatus() == commonpb.PeriodStatus_PERIOD_ARCHIVED {
			hasArchivedPeriods = true

			if len(p.GetSealingHash()) == 0 {
				callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH,
					fmt.Sprintf("archived period %d has no sealing hash (unsealed before archive)", p.GetId()),
					p.GetCloseSequence(), "", "", ""))
			} else {
				verifySealingHash(p, callback)
			}

			if p.GetCloseSequence() > archiveEndSeq {
				archiveEndSeq = p.GetCloseSequence()
				archiveLastHash = p.GetLastLogHash()
			}
		}
	}

	// Create replay store (replaces in-memory maps + txStateStore)
	replay, err := newReplayStore()
	if err != nil {
		return fmt.Errorf("creating replay store: %w", err)
	}

	defer func() { _ = replay.Close() }()

	// State tracked during log replay
	var (
		hasher       = blake3.New()
		lastHash     []byte
		knownLedgers = make(map[string]struct{})
		// Per-ledger reversion tracking using bitsets (1 bit per tx ID)
		ledgerKnownTxIDs    = make(map[string]*domain.ReversionBitset)
		ledgerRevertedTxIDs = make(map[string]*domain.ReversionBitset)
		// Per-ledger account types for ephemeral purge simulation
		ledgerAccountTypes = make(map[string]map[string]*commonpb.AccountType)
	)

	// If periods were archived, pre-populate knownLedgers from Pebble
	// since the CreateLedger logs have been purged.
	if hasArchivedPeriods {
		ledgerCursor, err := query.ReadLedgers(ctx, c.store)
		if err != nil {
			return fmt.Errorf("reading ledgers for archive recovery: %w", err)
		}

		ledgers, err := dal.Collect(ledgerCursor)
		if err != nil {
			return fmt.Errorf("collecting ledgers: %w", err)
		}

		for _, info := range ledgers {
			if info.GetDeletedAt() == nil {
				knownLedgers[info.GetName()] = struct{}{}

				if types := info.GetAccountTypes(); len(types) > 0 {
					ledgerAccountTypes[info.GetName()] = types
				}
			}
		}

		lastHash = archiveLastHash

		// Pre-populate knownTxIDs from archived transaction states so that
		// reversion invariant checks work correctly for non-archived logs.
		txIter, err := c.attrs.Transaction.NewStreamingIter(c.store, nil)
		if err != nil {
			return fmt.Errorf("creating tx streaming iter for archive recovery: %w", err)
		}

		for txIter.Next() {
			entry := txIter.Entry()

			var tk domain.TransactionKey
			if err := tk.Unmarshal(entry.CanonicalKey); err != nil {
				continue // skip unparsable keys
			}

			trackTxID(ledgerKnownTxIDs, tk.Ledger, tk.ID)

			if entry.Value.GetRevertedByTransaction() != 0 {
				trackTxID(ledgerRevertedTxIDs, tk.Ledger, tk.ID)
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
	logIter, err := c.store.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.KeyPrefixLog},
		UpperBound: []byte{dal.KeyPrefixLog, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
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

		// Extract sequence from key: [KeyPrefixLog(1)][sequence(8)]
		seq := binary.BigEndian.Uint64(logIter.Key()[1:9])

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

		// 2. Verify hash chain
		expectedHash := processing.ComputeLogHash(hasher, lastHash, log)
		if !bytes.Equal(expectedHash, log.GetHash()) {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH,
				fmt.Sprintf("hash mismatch at sequence %d: expected %x, got %x", seq, expectedHash, log.GetHash()),
				seq, "", "", ""))
		}

		lastHash = log.GetHash()

		// 3. Replay log to update expected state
		if log.GetPayload() != nil {
			switch payload := log.GetPayload().GetType().(type) {
			case *commonpb.LogPayload_CreateLedger:
				if payload.CreateLedger != nil && payload.CreateLedger.GetInfo() != nil {
					knownLedgers[payload.CreateLedger.GetInfo().GetName()] = struct{}{}
				}
			case *commonpb.LogPayload_DeleteLedger:
				if payload.DeleteLedger != nil && payload.DeleteLedger.GetInfo() != nil {
					delete(knownLedgers, payload.DeleteLedger.GetInfo().GetName())
				}
			case *commonpb.LogPayload_Apply:
				if payload.Apply != nil {
					ledgerName := payload.Apply.GetLedgerName()

					_, ok := knownLedgers[ledgerName]
					if !ok {
						callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_UNKNOWN_LEDGER,
							fmt.Sprintf("log %d references unknown ledger %q", seq, ledgerName),
							seq, ledgerName, "", ""))

						continue
					}

					if payload.Apply.GetLog() != nil && payload.Apply.GetLog().GetData() != nil {
						if err := replayLedgerLog(ledgerName, seq, payload.Apply.GetLog().GetData(), replay, ledgerAccountTypes); err != nil {
							return fmt.Errorf("replaying log %d: %w", seq, err)
						}

						checkReversionInvariants(ledgerName, seq, payload.Apply.GetLog().GetData(), ledgerKnownTxIDs, ledgerRevertedTxIDs, callback)
					}
				}
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

	// Open baseline checkpoint for archived state comparison.
	var baselineDB *pebble.DB

	if hasArchivedPeriods {
		baselinePath, exists := c.store.BaselineCheckpointPath()
		if exists {
			db, openErr := pebble.Open(baselinePath, &pebble.Options{ReadOnly: true})
			if openErr != nil {
				logging.FromContext(ctx).Infof("failed to open baseline checkpoint: %v (skipping entry-by-entry comparison)", openErr)
			} else {
				baselineDB = db

				defer func() { _ = baselineDB.Close() }()
			}
		}
	}

	// If archived periods exist but no baseline is available, we can't do
	// entry-by-entry comparison (the replay only covers non-archived logs).
	// This is expected after a restore. Warn and skip comparison passes.
	if hasArchivedPeriods && baselineDB == nil {
		logging.FromContext(ctx).Info("no baseline checkpoint available for archived state comparison; skipping entry-by-entry verification")

		return nil
	}

	// Comparison passes: 3-way merge (baseline + replay + live).
	// When no archived periods exist, baseline is nil and expected = replay delta only.
	c.compareVolumes(ctx, baselineDB, replay, callback)
	c.compareMetadata(ctx, baselineDB, replay, callback)
	c.compareTransactions(ctx, baselineDB, replay, callback)

	return nil
}

// compareVolumes performs a 3-way merge comparison for volumes.
// expected = baseline + replay delta; compare with live (actual).
func (c *Checker) compareVolumes(ctx context.Context, baselineDB *pebble.DB, replay *replayStore, callback func(*servicepb.CheckStoreEvent)) int {
	errorCount := 0

	// Collect live volumes
	liveVolumes := make(map[string]*raftcmdpb.VolumePair)

	liveIter, err := c.attrs.Volume.NewStreamingIter(c.store, nil)
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

		if expectedInput.Cmp(actualInput) != 0 {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
				fmt.Sprintf("input mismatch for %s/%s: expected %s, got %s",
					vk.Account, vk.Asset, expectedInput.String(), actualInput.String()),
				0, vk.Ledger, vk.Account, vk.Asset))

			errorCount++
		}

		if expectedOutput.Cmp(actualOutput) != 0 {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
				fmt.Sprintf("output mismatch for %s/%s: expected %s, got %s",
					vk.Account, vk.Asset, expectedOutput.String(), actualOutput.String()),
				0, vk.Ledger, vk.Account, vk.Asset))

			errorCount++
		}
	}

	return errorCount
}

// compareMetadata performs a 3-way merge comparison for account metadata.
// Replay entries encode SET (flag 0x00 + value) or DELETED (flag 0x01).
// expected = replay override if present, else baseline; compare with live.
func (c *Checker) compareMetadata(ctx context.Context, baselineDB *pebble.DB, replay *replayStore, callback func(*servicepb.CheckStoreEvent)) int {
	errorCount := 0

	// Collect live metadata
	liveMetadata := make(map[string]string)

	liveIter, err := c.attrs.Metadata.NewStreamingIter(c.store, nil)
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
					0, mk.Ledger, mk.Account, ""))
			} else {
				callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
					fmt.Sprintf("unexpected metadata for %s/%s: got %q",
						mk.Account, mk.Key, actualValue),
					0, mk.Ledger, mk.Account, ""))
			}

			errorCount++
		} else if expectedExists && expectedValue != actualValue {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
				fmt.Sprintf("metadata mismatch for %s/%s: expected %q, got %q",
					mk.Account, mk.Key, expectedValue, actualValue),
				0, mk.Ledger, mk.Account, ""))

			errorCount++
		}
	}

	return errorCount
}

// compareTransactions performs a 3-way merge comparison for transaction states.
// expected = replay override if present, else baseline; compare with live.
func (c *Checker) compareTransactions(ctx context.Context, baselineDB *pebble.DB, replay *replayStore, callback func(*servicepb.CheckStoreEvent)) int {
	errorCount := 0

	// Collect replay transaction states
	replayTx := make(map[string]*commonpb.TransactionState)

	replayIter, err := replay.newPrefixIter(replayPrefixTransaction)
	if err != nil {
		callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH,
			fmt.Sprintf("failed to create replay transaction iterator: %v", err), "", 0))

		return 1
	}

	for replayIter.First(); replayIter.Valid(); replayIter.Next() {
		canonicalKey := replayIter.Key()[1:]

		valBytes, valErr := replayIter.ValueAndErr()
		if valErr != nil {
			_ = replayIter.Close()

			return 1
		}

		// Values are prefixed with txOpFinalized tag from the merger's Finish output.
		if len(valBytes) == 0 || valBytes[0] != 0x00 {
			_ = replayIter.Close()

			return 1
		}

		state := &commonpb.TransactionState{}
		if err := state.UnmarshalVT(valBytes[1:]); err != nil {
			_ = replayIter.Close()

			return 1
		}

		replayTx[string(canonicalKey)] = state
	}

	if err := replayIter.Close(); err != nil {
		return 1
	}

	// Collect baseline transaction states (if available)
	baselineTx := make(map[string]*commonpb.TransactionState)

	if baselineDB != nil {
		baselineIter, err := c.attrs.Transaction.NewStreamingIter(baselineDB, nil)
		if err != nil {
			return 1
		}

		for baselineIter.Next() {
			e := baselineIter.Entry()
			baselineTx[string(e.CanonicalKey)] = e.Value
		}

		_ = baselineIter.Close()

		if baselineIter.Err() != nil {
			return 1
		}
	}

	// Collect all keys to check
	allKeys := make(map[string]struct{})
	for k := range replayTx {
		allKeys[k] = struct{}{}
	}

	for k := range baselineTx {
		allKeys[k] = struct{}{}
	}

	// Compare each expected transaction against the live store
	for key := range allKeys {
		if ctx.Err() != nil {
			return errorCount
		}

		var tk domain.TransactionKey
		if err := tk.Unmarshal([]byte(key)); err != nil {
			continue
		}

		// Expected: replay overrides baseline
		var expected *commonpb.TransactionState
		if rs, ok := replayTx[key]; ok {
			expected = rs
		} else if bs, ok := baselineTx[key]; ok {
			expected = bs
		}

		if expected == nil {
			continue
		}

		// Read actual from live store
		actualState, err := query.ReadTransactionState(context.Background(), c.store, c.attrs.Transaction, tk.Ledger, tk.ID)
		if err != nil {
			return errorCount
		}

		if actualState == nil {
			callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH,
				fmt.Sprintf("transaction state missing for tx %d", tk.ID),
				tk.Ledger, tk.ID))

			errorCount++

			continue
		}

		// Normalize empty MetadataSet to nil so that proto.Equal does not
		// treat nil vs &MetadataSet{} as a mismatch.
		// todo: this should be handled at source
		normalizeTransactionState(expected)
		normalizeTransactionState(actualState)

		if !proto.Equal(expected, actualState) {
			callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH,
				fmt.Sprintf("transaction state mismatch for tx %d: expected %s, got %s",
					tk.ID, expected.String(), actualState.String()),
				tk.Ledger, tk.ID))

			errorCount++
		}
	}

	return errorCount
}

// verifySealingHash checks that the sealing hash of an archived period matches
// the expected decomposition: BLAKE3(period_id || close_sequence || last_log_hash || state_hash).
func verifySealingHash(p *commonpb.Period, callback func(*servicepb.CheckStoreEvent)) {
	hasher := blake3.New()
	buf := make([]byte, 8)

	binary.BigEndian.PutUint64(buf, p.GetId())
	_, _ = hasher.Write(buf)

	binary.BigEndian.PutUint64(buf, p.GetCloseSequence())
	_, _ = hasher.Write(buf)

	if len(p.GetLastLogHash()) > 0 {
		_, _ = hasher.Write(p.GetLastLogHash())
	}

	_, _ = hasher.Write(p.GetStateHash())

	expected := hasher.Sum(nil)
	if !bytes.Equal(expected, p.GetSealingHash()) {
		callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH,
			fmt.Sprintf("sealing hash mismatch for archived period %d: expected %x, got %x",
				p.GetId(), expected, p.GetSealingHash()),
			p.GetCloseSequence(), "", "", ""))
	}
}

// replayLedgerLog updates expected state in the replay store based on a ledger log payload.
// ledgerAccountTypes tracks account types per ledger for ephemeral purge simulation.
func replayLedgerLog(
	ledger string,
	seq uint64,
	payload *commonpb.LedgerLogPayload,
	replay *replayStore,
	ledgerAccountTypes map[string]map[string]*commonpb.AccountType,
) error {
	switch p := payload.GetPayload().(type) {
	case *commonpb.LedgerLogPayload_AddedAccountType:
		if p.AddedAccountType != nil && p.AddedAccountType.GetAccountType() != nil {
			at := p.AddedAccountType.GetAccountType()
			types := ledgerAccountTypes[ledger]
			if types == nil {
				types = make(map[string]*commonpb.AccountType)
				ledgerAccountTypes[ledger] = types
			}

			types[at.GetName()] = at
		}

	case *commonpb.LedgerLogPayload_RemovedAccountType:
		if p.RemovedAccountType != nil {
			if types := ledgerAccountTypes[ledger]; types != nil {
				delete(types, p.RemovedAccountType.GetName())
			}
		}

	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if p.CreatedTransaction == nil || p.CreatedTransaction.GetTransaction() == nil {
			return nil
		}

		tx := p.CreatedTransaction.GetTransaction()
		if err := applyPostings(ledger, tx.GetPostings(), replay); err != nil {
			return err
		}

		// Simulate ephemeral purge: delete volumes that reached zero balance on ephemeral accounts.
		if err := simulateEphemeralPurge(ledger, tx.GetPostings(), replay, ledgerAccountTypes); err != nil {
			return err
		}

		// Track TransactionState
		txCanonical := domain.TransactionKey{Ledger: ledger, ID: tx.GetId()}.Bytes()

		if err := replay.createTransaction(txCanonical, seq, tx.GetMetadata()); err != nil {
			return fmt.Errorf("putting tx state for created tx %d: %w", tx.GetId(), err)
		}

		// Apply account metadata from the transaction
		for account, metaSet := range p.CreatedTransaction.GetAccountMetadata() {
			if metaSet != nil {
				for _, m := range metaSet.GetMetadata() {
					mk := domain.MetadataKey{
						AccountKey: domain.AccountKey{
							Ledger:  ledger,
							Account: account,
						},
						Key: m.GetKey(),
					}

					if m.GetValue() != nil {
						if err := replay.setMetadata(mk.Bytes(), commonpb.MetadataValueToString(m.GetValue())); err != nil {
							return fmt.Errorf("setting account metadata: %w", err)
						}
					}
				}
			}
		}

	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if p.RevertedTransaction == nil || p.RevertedTransaction.GetRevertTransaction() == nil {
			return nil
		}

		revertTx := p.RevertedTransaction.GetRevertTransaction()
		if err := applyPostings(ledger, revertTx.GetPostings(), replay); err != nil {
			return err
		}

		// Simulate ephemeral purge after revert postings.
		if err := simulateEphemeralPurge(ledger, revertTx.GetPostings(), replay, ledgerAccountTypes); err != nil {
			return err
		}

		// Mark original transaction as reverted
		origTxCanonical := domain.TransactionKey{Ledger: ledger, ID: p.RevertedTransaction.GetRevertedTransactionId()}.Bytes()

		if err := replay.setRevertedBy(origTxCanonical, revertTx.GetId()); err != nil {
			return fmt.Errorf("putting revert marker for tx %d: %w", p.RevertedTransaction.GetRevertedTransactionId(), err)
		}

		// Track TransactionState for the revert transaction
		revertTxCanonical := domain.TransactionKey{Ledger: ledger, ID: revertTx.GetId()}.Bytes()

		if err := replay.createTransaction(revertTxCanonical, seq, revertTx.GetMetadata()); err != nil {
			return fmt.Errorf("putting tx state for revert tx %d: %w", revertTx.GetId(), err)
		}

	case *commonpb.LedgerLogPayload_SavedMetadata:
		if p.SavedMetadata == nil || p.SavedMetadata.GetTarget() == nil {
			return nil
		}

		switch target := p.SavedMetadata.GetTarget().GetTarget().(type) {
		case *commonpb.Target_Account:
			if p.SavedMetadata.GetMetadata() != nil {
				for _, m := range p.SavedMetadata.GetMetadata().GetMetadata() {
					mk := domain.MetadataKey{
						AccountKey: domain.AccountKey{
							Ledger:  ledger,
							Account: target.Account.GetAddr(),
						},
						Key: m.GetKey(),
					}

					if m.GetValue() != nil {
						if err := replay.setMetadata(mk.Bytes(), commonpb.MetadataValueToString(m.GetValue())); err != nil {
							return fmt.Errorf("setting metadata: %w", err)
						}
					}
				}
			}
		case *commonpb.Target_Transaction:
			if p.SavedMetadata.GetMetadata() != nil {
				txCanonical := domain.TransactionKey{Ledger: ledger, ID: target.Transaction.GetId()}.Bytes()

				if err := replay.saveTxMetadata(txCanonical, p.SavedMetadata.GetMetadata().GetMetadata()); err != nil {
					return fmt.Errorf("saving tx metadata for tx %d: %w", target.Transaction.GetId(), err)
				}
			}
		}

	case *commonpb.LedgerLogPayload_DeletedMetadata:
		if p.DeletedMetadata == nil || p.DeletedMetadata.GetTarget() == nil {
			return nil
		}

		switch target := p.DeletedMetadata.GetTarget().GetTarget().(type) {
		case *commonpb.Target_Account:
			mk := domain.MetadataKey{
				AccountKey: domain.AccountKey{
					Ledger:  ledger,
					Account: target.Account.GetAddr(),
				},
				Key: p.DeletedMetadata.GetKey(),
			}

			if err := replay.deleteMetadata(mk.Bytes()); err != nil {
				return fmt.Errorf("deleting metadata: %w", err)
			}
		case *commonpb.Target_Transaction:
			txCanonical := domain.TransactionKey{Ledger: ledger, ID: target.Transaction.GetId()}.Bytes()

			if err := replay.deleteTxMetadata(txCanonical, p.DeletedMetadata.GetKey()); err != nil {
				return fmt.Errorf("deleting tx metadata for tx %d: %w", target.Transaction.GetId(), err)
			}
		}

	case *commonpb.LedgerLogPayload_SetMetadataFieldType:
		// Schema operations — no state to track for integrity checks
	case *commonpb.LedgerLogPayload_RemovedMetadataFieldType:
		// Schema operations — no state to track for integrity checks
	case *commonpb.LedgerLogPayload_ConvertMetadataBatch:
		// Background conversion — no state to track for integrity checks
	case *commonpb.LedgerLogPayload_MetadataConversionComplete:
		// Background conversion — no state to track for integrity checks
	}

	return nil
}

// simulateEphemeralPurge checks if any account volumes affected by the postings
// have reached zero balance (input == output) on an ephemeral account type.
// If so, it deletes the volume from the replay store, mirroring the real purge
// that happens during Buffered.Merge().
func simulateEphemeralPurge(
	ledger string,
	postings []*commonpb.Posting,
	replay *replayStore,
	ledgerAccountTypes map[string]map[string]*commonpb.AccountType,
) error {
	types := ledgerAccountTypes[ledger]
	if len(types) == 0 {
		return nil
	}

	// Collect unique accounts affected by these postings.
	seen := make(map[string]struct{})

	for _, posting := range postings {
		for _, addr := range []string{posting.GetSource(), posting.GetDestination()} {
			if addr == "world" {
				continue
			}

			if _, ok := seen[addr]; ok {
				continue
			}

			seen[addr] = struct{}{}

			matched := accounttype.FindMatchingType(addr, types)
			if matched == nil || !matched.GetEphemeral() {
				continue
			}

			// Check all assets for this account by reading the volume.
			// We only know which assets were affected by the postings, so check those.
			for _, p := range postings {
				if p.GetSource() != addr && p.GetDestination() != addr {
					continue
				}

				vk := domain.VolumeKey{
					AccountKey: domain.AccountKey{Ledger: ledger, Account: addr},
					Asset:      p.GetAsset(),
				}

				pair, err := replay.getVolume(vk.Bytes())
				if err != nil {
					return fmt.Errorf("reading volume for ephemeral check: %w", err)
				}

				if pair == nil {
					continue
				}

				// Check if input == output (zero balance).
				inBig := pair.GetInput().ToBigInt()
				outBig := pair.GetOutput().ToBigInt()

				if inBig.Cmp(outBig) == 0 {
					if err := replay.deleteVolume(vk.Bytes()); err != nil {
						return fmt.Errorf("deleting ephemeral volume: %w", err)
					}
				}
			}
		}
	}

	return nil
}

// applyPostings applies postings to the replay store as volume deltas.
func applyPostings(
	ledger string,
	postings []*commonpb.Posting,
	replay *replayStore,
) error {
	for _, posting := range postings {
		amount := posting.GetAmount().ToBigInt()

		// Source: increase output
		sourceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{
				Ledger:  ledger,
				Account: posting.GetSource(),
			},
			Asset: posting.GetAsset(),
		}

		if err := replay.addVolumeDelta(sourceKey.Bytes(), big.NewInt(0), amount); err != nil {
			return fmt.Errorf("adding source volume delta: %w", err)
		}

		// Destination: increase input
		destKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{
				Ledger:  ledger,
				Account: posting.GetDestination(),
			},
			Asset: posting.GetAsset(),
		}

		if err := replay.addVolumeDelta(destKey.Bytes(), amount, big.NewInt(0)); err != nil {
			return fmt.Errorf("adding dest volume delta: %w", err)
		}
	}

	return nil
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
	ledger string,
	seq uint64,
	payload *commonpb.LedgerLogPayload,
	knownTxIDs map[string]*domain.ReversionBitset,
	revertedTxIDs map[string]*domain.ReversionBitset,
	callback func(*servicepb.CheckStoreEvent),
) {
	switch p := payload.GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if p.CreatedTransaction != nil && p.CreatedTransaction.GetTransaction() != nil {
			trackTxID(knownTxIDs, ledger, p.CreatedTransaction.GetTransaction().GetId())
		}

	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if p.RevertedTransaction == nil {
			return
		}

		revertedID := p.RevertedTransaction.GetRevertedTransactionId()

		// Check that the target transaction exists
		bs := knownTxIDs[ledger]
		if bs == nil || !bs.IsReverted(revertedID) {
			callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERTED_MISMATCH,
				fmt.Sprintf("log %d reverts non-existent transaction %d in ledger %q", seq, revertedID, ledger),
				ledger, revertedID))
		}

		// Check that the transaction is not already reverted
		rbs := revertedTxIDs[ledger]
		if rbs != nil && rbs.IsReverted(revertedID) {
			callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERTED_MISMATCH,
				fmt.Sprintf("log %d double-reverts transaction %d in ledger %q", seq, revertedID, ledger),
				ledger, revertedID))
		}

		// Mark the transaction as reverted
		trackTxID(revertedTxIDs, ledger, revertedID)

		// Track the revert transaction's own ID
		if p.RevertedTransaction.GetRevertTransaction() != nil {
			trackTxID(knownTxIDs, ledger, p.RevertedTransaction.GetRevertTransaction().GetId())
		}
	}
}

// normalizeTransactionState replaces an empty MetadataSet with nil so that
// proto.Equal treats both representations as equivalent.
func normalizeTransactionState(s *commonpb.TransactionState) {
	if s.GetMetadata() != nil && len(s.GetMetadata().GetMetadata()) == 0 {
		s.Metadata = nil
	}
}

func trackTxID(m map[string]*domain.ReversionBitset, ledger string, txID uint64) {
	bs := m[ledger]
	if bs == nil {
		bs = &domain.ReversionBitset{}
		m[ledger] = bs
	}

	bs.SetReverted(txID)
}
