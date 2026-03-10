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

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

const progressInterval = 100

// expectedTxState tracks the expected transaction state derived from replaying logs.
type expectedTxState struct {
	state *commonpb.TransactionState
}

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
// 6. Transaction update consistency.
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

	// State tracked during log replay
	var (
		hasher           = blake3.New()
		lastHash         []byte
		expectedInputs   = make(map[string]*big.Int)         // volumeKey -> cumulative input
		expectedOutputs  = make(map[string]*big.Int)         // volumeKey -> cumulative output
		expectedMetadata = make(map[string]string)           // metadataKey -> value
		deletedMetadata  = make(map[string]struct{})         // metadataKey -> deleted
		expectedTxStates = make(map[string]*expectedTxState) // txKey -> expected state
		knownLedgers     = make(map[string]struct{})         // ledgerName -> exists
		// Per-ledger reversion tracking (verified during log replay)
		ledgerKnownTxIDs    = make(map[string]map[uint64]struct{}) // ledger -> set of created tx IDs
		ledgerRevertedTxIDs = make(map[string]map[uint64]struct{}) // ledger -> set of already-reverted tx IDs
		errorCount          int
	)

	// Pass 1: Single forward iterator over all logs (avoids N point reads).
	// Uses UnmarshalVT directly (avoids reflect.New + proto.Unmarshal overhead).
	logIter, err := c.store.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.KeyPrefixLog},
		UpperBound: []byte{dal.KeyPrefixLog, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
	})
	if err != nil {
		return fmt.Errorf("creating log iterator: %w", err)
	}

	defer func() { _ = logIter.Close() }()

	expectedSeq := uint64(1)

	for logIter.First(); logIter.Valid(); logIter.Next() {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Extract sequence from key: [KeyPrefixLog(1)][sequence(8)]
		seq := binary.BigEndian.Uint64(logIter.Key()[1:9])

		// 1. Detect gaps: any missing sequences between expectedSeq and seq
		for expectedSeq < seq {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP,
				fmt.Sprintf("log sequence %d is missing", expectedSeq), expectedSeq, "", "", ""))

			errorCount++
			expectedSeq++
		}

		expectedSeq = seq + 1

		// Unmarshal using vtprotobuf (avoids reflect.New and standard proto.Unmarshal)
		value, err := logIter.ValueAndErr()
		if err != nil {
			return fmt.Errorf("reading log %d value: %w", seq, err)
		}

		log := &commonpb.Log{}
		if err := log.UnmarshalVT(value); err != nil {
			return fmt.Errorf("unmarshaling log %d: %w", seq, err)
		}

		// 2. Verify hash chain
		// ComputeLogHash only hashes sequence, payload, and idempotency fields,
		// skipping the hash field by design, so we can pass the stored log directly.
		expectedHash := processing.ComputeLogHash(hasher, lastHash, log)
		if !bytes.Equal(expectedHash, log.GetHash()) {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH,
				fmt.Sprintf("hash mismatch at sequence %d: expected %x, got %x", seq, expectedHash, log.GetHash()),
				seq, "", "", ""))

			errorCount++
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
				// Nothing to track for delete
			case *commonpb.LogPayload_Apply:
				if payload.Apply != nil {
					ledgerName := payload.Apply.GetLedgerName()

					_, ok := knownLedgers[ledgerName]
					if !ok {
						callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_UNKNOWN_LEDGER,
							fmt.Sprintf("log %d references unknown ledger %q", seq, ledgerName),
							seq, ledgerName, "", ""))

						errorCount++

						continue
					}

					if payload.Apply.GetLog() != nil && payload.Apply.GetLog().GetData() != nil {
						replayLedgerLog(ledgerName, seq, payload.Apply.GetLog().GetData(), expectedInputs, expectedOutputs, expectedMetadata, deletedMetadata, expectedTxStates)

						// Track transaction IDs and verify reversion invariants
						errorCount += checkReversionInvariants(ledgerName, seq, payload.Apply.GetLog().GetData(), ledgerKnownTxIDs, ledgerRevertedTxIDs, callback)
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

	// Pass 2: Compare expected volumes against actual stored values.
	// Single ForEachInPrefix scan instead of N separate ComputeValue iterators.
	actualVolumes := make(map[string]*raftcmdpb.VolumePair)

	err = c.attrs.Volume.ForEachInPrefix(c.store, nil, func(entry attributes.ComputedEntry[*raftcmdpb.VolumePair]) error {
		actualVolumes[string(entry.CanonicalKey)] = entry.Value

		return nil
	})
	if err != nil {
		return fmt.Errorf("scanning volumes: %w", err)
	}

	// Collect all volume keys from both expected maps
	allVolumeKeys := make(map[string]struct{}, len(expectedInputs)+len(expectedOutputs))
	for key := range expectedInputs {
		allVolumeKeys[key] = struct{}{}
	}

	for key := range expectedOutputs {
		allVolumeKeys[key] = struct{}{}
	}

	for key := range allVolumeKeys {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var vk domain.VolumeKey

		err := vk.Unmarshal([]byte(key))
		if err != nil {
			continue
		}

		pair := actualVolumes[key]
		actualInputVal := big.NewInt(0)
		actualOutputVal := big.NewInt(0)

		if pair != nil {
			actualInputVal = pair.GetInput().ToBigInt()
			actualOutputVal = pair.GetOutput().ToBigInt()
		}

		if expectedInput, ok := expectedInputs[key]; ok {
			if expectedInput.Cmp(actualInputVal) != 0 {
				callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
					fmt.Sprintf("input mismatch for %s/%s: expected %s, got %s",
						vk.Account, vk.Asset, expectedInput.String(), actualInputVal.String()),
					0, vk.Ledger, vk.Account, vk.Asset))

				errorCount++
			}
		}

		if expectedOutput, ok := expectedOutputs[key]; ok {
			if expectedOutput.Cmp(actualOutputVal) != 0 {
				callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
					fmt.Sprintf("output mismatch for %s/%s: expected %s, got %s",
						vk.Account, vk.Asset, expectedOutput.String(), actualOutputVal.String()),
					0, vk.Ledger, vk.Account, vk.Asset))

				errorCount++
			}
		}
	}

	// Pass 3: Compare expected metadata against actual stored values.
	// Single ForEachInPrefix scan instead of N separate ComputeValue iterators.
	actualMetadata := make(map[string]*commonpb.MetadataValue)

	err = c.attrs.Metadata.ForEachInPrefix(c.store, nil, func(entry attributes.ComputedEntry[*commonpb.MetadataValue]) error {
		actualMetadata[string(entry.CanonicalKey)] = entry.Value

		return nil
	})
	if err != nil {
		return fmt.Errorf("scanning metadata: %w", err)
	}

	for key, expectedValue := range expectedMetadata {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Skip deleted metadata
		if _, deleted := deletedMetadata[key]; deleted {
			continue
		}

		var mk domain.MetadataKey

		err := mk.Unmarshal([]byte(key))
		if err != nil {
			continue
		}

		actualValue := actualMetadata[key]

		actualStr := ""
		if actualValue != nil {
			actualStr = commonpb.MetadataValueToString(actualValue)
		}

		if expectedValue != actualStr {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
				fmt.Sprintf("metadata mismatch for %s/%s: expected %q, got %q",
					mk.Account, mk.Key, expectedValue, actualStr),
				0, mk.Ledger, mk.Account, ""))

			errorCount++
		}
	}

	// Pass 4: Compare expected transaction states against actual stored values
	for key, expected := range expectedTxStates {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var tk domain.TransactionKey
		if err := tk.Unmarshal([]byte(key)); err != nil {
			continue
		}

		actualState, err := query.ReadTransactionState(context.Background(), c.store, c.attrs.Transaction, tk.Ledger, tk.ID)
		if err != nil {
			return fmt.Errorf("getting transaction state for ledger %s tx %d: %w", tk.Ledger, tk.ID, err)
		}

		if actualState == nil {
			callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH,
				fmt.Sprintf("transaction state missing for tx %d", tk.ID),
				tk.Ledger, tk.ID))

			errorCount++

			continue
		}

		if !proto.Equal(expected.state, actualState) {
			callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH,
				fmt.Sprintf("transaction state mismatch for tx %d: expected created_by_log=%d got %d, expected reverted_by=%d got %d",
					tk.ID, expected.state.GetCreatedByLog(), actualState.GetCreatedByLog(),
					expected.state.GetRevertedByTransaction(), actualState.GetRevertedByTransaction()),
				tk.Ledger, tk.ID))

			errorCount++
		}
	}

	return nil
}

// replayLedgerLog updates expected state based on a ledger log payload.
func replayLedgerLog(
	ledger string,
	seq uint64,
	payload *commonpb.LedgerLogPayload,
	expectedInputs map[string]*big.Int,
	expectedOutputs map[string]*big.Int,
	expectedMetadata map[string]string,
	deletedMetadata map[string]struct{},
	expectedTxStates map[string]*expectedTxState,
) {
	switch p := payload.GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if p.CreatedTransaction == nil || p.CreatedTransaction.GetTransaction() == nil {
			return
		}

		tx := p.CreatedTransaction.GetTransaction()
		applyPostings(ledger, tx.GetPostings(), expectedInputs, expectedOutputs)

		// Track TransactionState for the created transaction
		txKey := string(domain.TransactionKey{Ledger: ledger, ID: tx.GetId()}.Bytes())
		txState := getOrCreateTxState(expectedTxStates, txKey)
		txState.state.CreatedByLog = seq
		txState.state.Metadata = tx.GetMetadata()

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

					key := string(mk.Bytes())
					if m.GetValue() != nil {
						expectedMetadata[key] = commonpb.MetadataValueToString(m.GetValue())
						delete(deletedMetadata, key)
					}
				}
			}
		}

	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if p.RevertedTransaction == nil || p.RevertedTransaction.GetRevertTransaction() == nil {
			return
		}

		revertTx := p.RevertedTransaction.GetRevertTransaction()
		applyPostings(ledger, revertTx.GetPostings(), expectedInputs, expectedOutputs)

		// Mark original transaction as reverted
		origTxKey := string(domain.TransactionKey{Ledger: ledger, ID: p.RevertedTransaction.GetRevertedTransactionId()}.Bytes())
		origState := getOrCreateTxState(expectedTxStates, origTxKey)
		origState.state.RevertedByTransaction = revertTx.GetId()

		// Track TransactionState for the revert transaction
		revertTxKey := string(domain.TransactionKey{Ledger: ledger, ID: revertTx.GetId()}.Bytes())
		revertState := getOrCreateTxState(expectedTxStates, revertTxKey)
		revertState.state.CreatedByLog = seq
		revertState.state.Metadata = revertTx.GetMetadata()

	case *commonpb.LedgerLogPayload_SavedMetadata:
		if p.SavedMetadata == nil || p.SavedMetadata.GetTarget() == nil {
			return
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

					key := string(mk.Bytes())
					if m.GetValue() != nil {
						expectedMetadata[key] = commonpb.MetadataValueToString(m.GetValue())
						delete(deletedMetadata, key)
					}
				}
			}
		case *commonpb.Target_Transaction:
			if p.SavedMetadata.GetMetadata() != nil {
				txKey := string(domain.TransactionKey{Ledger: ledger, ID: target.Transaction.GetId()}.Bytes())
				txState := getOrCreateTxState(expectedTxStates, txKey)

				if txState.state.GetMetadata() == nil {
					txState.state.Metadata = &commonpb.MetadataSet{}
				}

				for _, m := range p.SavedMetadata.GetMetadata().GetMetadata() {
					found := false

					for i, existing := range txState.state.GetMetadata().GetMetadata() {
						if existing.GetKey() == m.GetKey() {
							txState.state.Metadata.Metadata[i] = m
							found = true

							break
						}
					}

					if !found {
						txState.state.Metadata.Metadata = append(txState.state.Metadata.Metadata, m)
					}
				}
			}
		}

	case *commonpb.LedgerLogPayload_DeletedMetadata:
		if p.DeletedMetadata == nil || p.DeletedMetadata.GetTarget() == nil {
			return
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
			key := string(mk.Bytes())
			delete(expectedMetadata, key)
			deletedMetadata[key] = struct{}{}
		case *commonpb.Target_Transaction:
			txKey := string(domain.TransactionKey{Ledger: ledger, ID: target.Transaction.GetId()}.Bytes())
			txState := getOrCreateTxState(expectedTxStates, txKey)

			if txState.state.GetMetadata() != nil {
				filtered := make([]*commonpb.Metadata, 0, len(txState.state.GetMetadata().GetMetadata()))
				for _, m := range txState.state.GetMetadata().GetMetadata() {
					if m.GetKey() != p.DeletedMetadata.GetKey() {
						filtered = append(filtered, m)
					}
				}

				txState.state.Metadata.Metadata = filtered
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
}

// applyPostings applies postings to the expected volume maps.
func applyPostings(
	ledger string,
	postings []*commonpb.Posting,
	expectedInputs map[string]*big.Int,
	expectedOutputs map[string]*big.Int,
) {
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

		sk := string(sourceKey.Bytes())
		if expectedOutputs[sk] == nil {
			expectedOutputs[sk] = big.NewInt(0)
		}

		expectedOutputs[sk].Add(expectedOutputs[sk], amount)

		// Destination: increase input
		destKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{
				Ledger:  ledger,
				Account: posting.GetDestination(),
			},
			Asset: posting.GetAsset(),
		}

		dk := string(destKey.Bytes())
		if expectedInputs[dk] == nil {
			expectedInputs[dk] = big.NewInt(0)
		}

		expectedInputs[dk].Add(expectedInputs[dk], amount)
	}
}

func getOrCreateTxState(m map[string]*expectedTxState, key string) *expectedTxState {
	if state, ok := m[key]; ok {
		return state
	}

	state := &expectedTxState{state: &commonpb.TransactionState{}}
	m[key] = state

	return state
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
// during log replay. It returns the number of errors found.
func checkReversionInvariants(
	ledger string,
	seq uint64,
	payload *commonpb.LedgerLogPayload,
	knownTxIDs map[string]map[uint64]struct{},
	revertedTxIDs map[string]map[uint64]struct{},
	callback func(*servicepb.CheckStoreEvent),
) int {
	errors := 0

	switch p := payload.GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if p.CreatedTransaction != nil && p.CreatedTransaction.GetTransaction() != nil {
			trackTxID(knownTxIDs, ledger, p.CreatedTransaction.GetTransaction().GetId())
		}

	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if p.RevertedTransaction == nil {
			return 0
		}

		revertedID := p.RevertedTransaction.GetRevertedTransactionId()

		// Check that the target transaction exists
		if txs, ok := knownTxIDs[ledger]; !ok || !containsUint64(txs, revertedID) {
			callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERTED_MISMATCH,
				fmt.Sprintf("log %d reverts non-existent transaction %d in ledger %q", seq, revertedID, ledger),
				ledger, revertedID))

			errors++
		}

		// Check that the transaction is not already reverted
		if containsUint64(revertedTxIDs[ledger], revertedID) {
			callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERTED_MISMATCH,
				fmt.Sprintf("log %d double-reverts transaction %d in ledger %q", seq, revertedID, ledger),
				ledger, revertedID))

			errors++
		}

		// Mark the transaction as reverted
		trackTxID(revertedTxIDs, ledger, revertedID)

		// Track the revert transaction's own ID
		if p.RevertedTransaction.GetRevertTransaction() != nil {
			trackTxID(knownTxIDs, ledger, p.RevertedTransaction.GetRevertTransaction().GetId())
		}
	}

	return errors
}

func trackTxID(m map[string]map[uint64]struct{}, ledger string, txID uint64) {
	txs, ok := m[ledger]
	if !ok {
		txs = make(map[uint64]struct{})
		m[ledger] = txs
	}

	txs[txID] = struct{}{}
}

func containsUint64(m map[uint64]struct{}, id uint64) bool {
	if m == nil {
		return false
	}

	_, ok := m[id]

	return ok
}
