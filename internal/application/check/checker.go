package check

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/cockroachdb/pebble/v2"
	"github.com/zeebo/blake3"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing"
	domainreplay "github.com/formancehq/ledger-v3-poc/internal/domain/replay"
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
	store  *dal.Store
	attrs  *attributes.Attributes
	logger logging.Logger
}

// NewChecker creates a new Checker.
func NewChecker(store *dal.Store, attrs *attributes.Attributes, logger logging.Logger) *Checker {
	return &Checker{
		store:  store,
		attrs:  attrs,
		logger: logger,
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

	// Read archived periods to adjust the starting point for log replay.
	periodsCursor, err := query.ReadPeriods(ctx, snap)
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
		hashBuf      = make([]byte, 0, 1024)
		lastHash     []byte
		knownLedgers = make(map[string]struct{})
		// Per-ledger reversion tracking using bitsets (1 bit per tx ID)
		ledgerKnownTxIDs    = make(map[string]*domain.ReversionBitset)
		ledgerRevertedTxIDs = make(map[string]*domain.ReversionBitset)
		// Per-ledger account types for ephemeral purge simulation
		rawLedgerTypes     = make(map[string]map[string]*commonpb.AccountType)
		ledgerAccountTypes = make(map[string][]accounttype.CompiledType)
	)

	// If periods were archived, pre-populate knownLedgers from Pebble
	// since the CreateLedger logs have been purged.
	if hasArchivedPeriods {
		ledgerCursor, err := query.ReadLedgers(ctx, snap)
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
					rawLedgerTypes[info.GetName()] = types
					ledgerAccountTypes[info.GetName()] = accounttype.CompileTypes(types)
				}
			}
		}

		lastHash = archiveLastHash

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
	logIter, err := snap.NewIter(&pebble.IterOptions{
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
		var expectedHash []byte
		hashBuf, expectedHash = processing.ComputeLogHash(hashBuf, lastHash, log)
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

					_, ok := knownLedgers[ledgerName]
					if !ok {
						callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_UNKNOWN_LEDGER,
							fmt.Sprintf("log %d references unknown ledger %q", seq, ledgerName),
							seq, ledgerName, "", ""))

						continue
					}

					if payload.Apply.GetLog() != nil && payload.Apply.GetLog().GetData() != nil {
						if err := domainreplay.ReplayLedgerLog(ledgerName, seq, payload.Apply.GetLog().GetData(), replay, rawLedgerTypes, ledgerAccountTypes); err != nil {
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

	// If archived periods exist but no baseline is available, we can't do
	// entry-by-entry comparison (the replay only covers non-archived logs).
	// This is expected after a restore. Warn and skip comparison passes.
	if hasArchivedPeriods && baselineDB == nil {
		c.logger.Info("no baseline checkpoint available for archived state comparison; skipping entry-by-entry verification")

		return nil
	}

	// Comparison passes: 3-way merge (baseline + replay + live).
	// When no archived periods exist, baseline is nil and expected = replay delta only.
	c.compareVolumes(ctx, snap, baselineDB, replay, callback)
	c.compareMetadata(ctx, snap, baselineDB, replay, callback)
	c.compareTransactions(ctx, snap, baselineDB, replay, callback)

	return nil
}

// compareVolumes performs a 3-way merge comparison for volumes.
// expected = baseline + replay delta; compare with live (actual).
func (c *Checker) compareVolumes(ctx context.Context, reader dal.PebbleReader, baselineDB *pebble.DB, replay *replayStore, callback func(*servicepb.CheckStoreEvent)) int {
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

		// A zero-balanced volume (input == output) is semantically equivalent
		// to absent. This matters for transient accounts: the real system may
		// keep pre-existing zero-balanced volumes in Pebble while the replay
		// store purges them (or vice versa). Both represent "no net balance".
		expectedZeroBal := expectedInput.Cmp(expectedOutput) == 0
		actualZeroBal := actualInput.Cmp(actualOutput) == 0
		if expectedZeroBal && actualZeroBal {
			continue
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
func (c *Checker) compareMetadata(ctx context.Context, reader dal.PebbleReader, baselineDB *pebble.DB, replay *replayStore, callback func(*servicepb.CheckStoreEvent)) int {
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
func (c *Checker) compareTransactions(ctx context.Context, reader dal.PebbleReader, baselineDB *pebble.DB, replay *replayStore, callback func(*servicepb.CheckStoreEvent)) int {
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
		actualState, err := query.ReadTransactionState(context.Background(), reader, c.attrs.Transaction, tk.Ledger, tk.ID)
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

		// Normalize empty metadata map to nil so that proto.Equal does not
		// treat nil vs empty map as a mismatch.
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

// normalizeTransactionState replaces an empty metadata map with nil so that
// proto.Equal treats both representations as equivalent.
func normalizeTransactionState(s *commonpb.TransactionState) {
	if s.GetMetadata() != nil && len(s.GetMetadata()) == 0 {
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
