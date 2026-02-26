package check

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/zeebo/blake3"
	"google.golang.org/protobuf/proto"
)

const progressInterval = 100

// expectedTxState tracks the expected transaction updates and reverted status
// derived from replaying logs.
type expectedTxState struct {
	updates  []*commonpb.TransactionUpdate
	reverted bool
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
// 3. Volume consistency (input/output per account/asset)
// 4. Account metadata consistency
// 5. Transaction update consistency
// 6. Reverted status consistency
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
		knownLedgers     = make(map[string]struct{})          // ledgerName -> exists
		errorCount       int
	)

	// Pass 1: Iterate all logs, verify hash chain, and replay state
	for seq := uint64(1); seq <= lastSequence; seq++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		log, err := query.ReadLogBySequence(c.store, seq)
		if err != nil {
			return fmt.Errorf("getting log %d: %w", seq, err)
		}

		// 1. Verify sequence continuity
		if log == nil {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_SEQUENCE_GAP,
				fmt.Sprintf("log sequence %d is missing", seq), seq, "", "", ""))
			errorCount++
			continue
		}

		// 2. Verify hash chain
		// ComputeLogHash only hashes sequence, payload, and idempotency fields,
		// skipping the hash field by design, so we can pass the stored log directly.
		expectedHash := processing.ComputeLogHash(hasher, lastHash, log)
		if !bytes.Equal(expectedHash, log.Hash) {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH,
				fmt.Sprintf("hash mismatch at sequence %d: expected %x, got %x", seq, expectedHash, log.Hash),
				seq, "", "", ""))
			errorCount++
		}
		lastHash = log.Hash

		// 3. Replay log to update expected state
		if log.Payload != nil {
			switch payload := log.Payload.Type.(type) {
			case *commonpb.LogPayload_CreateLedger:
				if payload.CreateLedger != nil && payload.CreateLedger.Info != nil {
					knownLedgers[payload.CreateLedger.Info.Name] = struct{}{}
				}
			case *commonpb.LogPayload_DeleteLedger:
				// Nothing to track for delete
			case *commonpb.LogPayload_Apply:
				if payload.Apply != nil {
					ledgerName := payload.Apply.LedgerName
					_, ok := knownLedgers[ledgerName]
					if !ok {
						callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_UNKNOWN_LEDGER,
							fmt.Sprintf("log %d references unknown ledger %q", seq, ledgerName),
							seq, ledgerName, "", ""))
						errorCount++
						continue
					}
					if payload.Apply.Log != nil && payload.Apply.Log.Data != nil {
						replayLedgerLog(ledgerName, seq, payload.Apply.Log.Data, expectedInputs, expectedOutputs, expectedMetadata, deletedMetadata, expectedTxStates)
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

	// Pass 2: Compare expected volumes against actual stored values
	const maxIndex uint64 = 1 << 62

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
		if err := vk.Unmarshal([]byte(key)); err != nil {
			continue
		}

		pair, err := c.attrs.Volume.ComputeValue(c.store, maxIndex, []byte(key))
		if err != nil {
			return fmt.Errorf("computing volume for %s: %w", key, err)
		}

		actualInputVal := big.NewInt(0)
		actualOutputVal := big.NewInt(0)
		if pair != nil {
			if pair.InputKnown != nil {
				actualInputVal = pair.InputKnown.ToBigInt()
			}
			if pair.OutputKnown != nil {
				actualOutputVal = pair.OutputKnown.ToBigInt()
			}
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

	// Pass 3: Compare expected metadata against actual stored values
	for key, expectedValue := range expectedMetadata {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Skip deleted metadata
		if _, deleted := deletedMetadata[key]; deleted {
			continue
		}

		var mk domain.MetadataKey
		if err := mk.Unmarshal([]byte(key)); err != nil {
			continue
		}

		actualValue, err := c.attrs.Metadata.ComputeValue(c.store, maxIndex, []byte(key))
		if err != nil {
			return fmt.Errorf("computing metadata for %s: %w", key, err)
		}

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

	// Pass 4: Compare expected transaction updates against actual stored values
	for key, expected := range expectedTxStates {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var tk domain.TransactionKey
		if err := tk.Unmarshal([]byte(key)); err != nil {
			continue
		}

		actualUpdates, err := query.ReadTransactionUpdates(c.store, tk.Ledger, tk.ID)
		if err != nil {
			return fmt.Errorf("getting transaction updates for ledger %s tx %d: %w", tk.Ledger, tk.ID, err)
		}

		if len(expected.updates) != len(actualUpdates) {
			callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH,
				fmt.Sprintf("transaction update count mismatch for tx %d: expected %d, got %d",
					tk.ID, len(expected.updates), len(actualUpdates)),
				tk.Ledger, tk.ID))
			errorCount++
			continue
		}

		for i, expectedUpdate := range expected.updates {
			actualUpdate := actualUpdates[i]
			if !proto.Equal(expectedUpdate, actualUpdate) {
				callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_TRANSACTION_UPDATE_MISMATCH,
					fmt.Sprintf("transaction update mismatch for tx %d at index %d: ByLog expected %d got %d",
						tk.ID, i, expectedUpdate.ByLog, actualUpdate.ByLog),
					tk.Ledger, tk.ID))
				errorCount++
			}
		}
	}

	// Pass 5: Compare expected reverted status against actual stored values
	for key, expected := range expectedTxStates {
		if !expected.reverted {
			continue
		}

		if ctx.Err() != nil {
			return ctx.Err()
		}

		var tk domain.TransactionKey
		if err := tk.Unmarshal([]byte(key)); err != nil {
			continue
		}

		actual, err := c.attrs.Reverted.ComputeValue(c.store, maxIndex, []byte(key))
		if err != nil {
			return fmt.Errorf("computing reverted status for ledger %s tx %d: %w", tk.Ledger, tk.ID, err)
		}

		actualReverted := false
		if actual != nil {
			actualReverted = actual.Reverted
		}

		if expected.reverted != actualReverted {
			callback(errorEventWithTx(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_REVERTED_MISMATCH,
				fmt.Sprintf("reverted mismatch for tx %d: expected %v, got %v",
					tk.ID, expected.reverted, actualReverted),
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
	switch p := payload.Payload.(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if p.CreatedTransaction == nil || p.CreatedTransaction.Transaction == nil {
			return
		}
		tx := p.CreatedTransaction.Transaction
		applyPostings(ledger, tx.Postings, expectedInputs, expectedOutputs)

		// Track TransactionInit update
		txKey := string(domain.TransactionKey{Ledger: ledger, ID: tx.Id}.Bytes())
		state := getOrCreateTxState(expectedTxStates, txKey)
		state.updates = append(state.updates, &commonpb.TransactionUpdate{
			ByLog: seq,
			Updates: []*commonpb.TransactionUpdateType{{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
					TransactionInit: &commonpb.TransactionInit{},
				},
			}},
		})

		// Apply account metadata from the transaction
		for account, metaSet := range p.CreatedTransaction.AccountMetadata {
			if metaSet != nil {
				for _, m := range metaSet.Metadata {
					mk := domain.MetadataKey{
						AccountKey: domain.AccountKey{
							Ledger: ledger,
							Account:  account,
						},
						Key: m.Key,
					}
					key := string(mk.Bytes())
					if m.Value != nil {
						expectedMetadata[key] = commonpb.MetadataValueToString(m.Value)
						delete(deletedMetadata, key)
					}
				}
			}
		}

	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if p.RevertedTransaction == nil || p.RevertedTransaction.RevertTransaction == nil {
			return
		}
		revertTx := p.RevertedTransaction.RevertTransaction
		applyPostings(ledger, revertTx.Postings, expectedInputs, expectedOutputs)

		// Track revert update on the original transaction
		origTxKey := string(domain.TransactionKey{Ledger: ledger, ID: p.RevertedTransaction.RevertedTransactionId}.Bytes())
		origState := getOrCreateTxState(expectedTxStates, origTxKey)
		origState.reverted = true
		origState.updates = append(origState.updates, &commonpb.TransactionUpdate{
			ByLog: seq,
			Updates: []*commonpb.TransactionUpdateType{{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationRevert{
					TransactionModificationRevert: &commonpb.TransactionUpdateRevert{
						ByTransaction: revertTx.Id,
					},
				},
			}},
		})

		// Track TransactionInit for the revert transaction
		revertTxKey := string(domain.TransactionKey{Ledger: ledger, ID: revertTx.Id}.Bytes())
		revertState := getOrCreateTxState(expectedTxStates, revertTxKey)
		revertState.updates = append(revertState.updates, &commonpb.TransactionUpdate{
			ByLog: seq,
			Updates: []*commonpb.TransactionUpdateType{{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
					TransactionInit: &commonpb.TransactionInit{},
				},
			}},
		})

	case *commonpb.LedgerLogPayload_SavedMetadata:
		if p.SavedMetadata == nil || p.SavedMetadata.Target == nil {
			return
		}
		switch target := p.SavedMetadata.Target.Target.(type) {
		case *commonpb.Target_Account:
			if p.SavedMetadata.Metadata != nil {
				for _, m := range p.SavedMetadata.Metadata.Metadata {
					mk := domain.MetadataKey{
						AccountKey: domain.AccountKey{
							Ledger: ledger,
							Account:  target.Account.Addr,
						},
						Key: m.Key,
					}
					key := string(mk.Bytes())
					if m.Value != nil {
						expectedMetadata[key] = commonpb.MetadataValueToString(m.Value)
						delete(deletedMetadata, key)
					}
				}
			}
		case *commonpb.Target_Transaction:
			if p.SavedMetadata.Metadata != nil {
				txKey := string(domain.TransactionKey{Ledger: ledger, ID: target.Transaction.Id}.Bytes())
				state := getOrCreateTxState(expectedTxStates, txKey)
				updates := make([]*commonpb.TransactionUpdateType, len(p.SavedMetadata.Metadata.Metadata))
				for i, m := range p.SavedMetadata.Metadata.Metadata {
					updates[i] = &commonpb.TransactionUpdateType{
						TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationAddMetadata{
							TransactionModificationAddMetadata: &commonpb.TransactionUpdateAddMetadata{
								Metadata: m,
							},
						},
					}
				}
				state.updates = append(state.updates, &commonpb.TransactionUpdate{
					ByLog:   seq,
					Updates: updates,
				})
			}
		}

	case *commonpb.LedgerLogPayload_DeletedMetadata:
		if p.DeletedMetadata == nil || p.DeletedMetadata.Target == nil {
			return
		}
		switch target := p.DeletedMetadata.Target.Target.(type) {
		case *commonpb.Target_Account:
			mk := domain.MetadataKey{
				AccountKey: domain.AccountKey{
					Ledger: ledger,
					Account:  target.Account.Addr,
				},
				Key: p.DeletedMetadata.Key,
			}
			key := string(mk.Bytes())
			delete(expectedMetadata, key)
			deletedMetadata[key] = struct{}{}
		case *commonpb.Target_Transaction:
			txKey := string(domain.TransactionKey{Ledger: ledger, ID: target.Transaction.Id}.Bytes())
			state := getOrCreateTxState(expectedTxStates, txKey)
			state.updates = append(state.updates, &commonpb.TransactionUpdate{
				ByLog: seq,
				Updates: []*commonpb.TransactionUpdateType{{
					TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationDeleteMetadata{
						TransactionModificationDeleteMetadata: &commonpb.TransactionUpdateDeleteMetadata{
							Key: p.DeletedMetadata.Key,
						},
					},
				}},
			})
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
		amount := posting.Amount.ToBigInt()

		// Source: increase output
		sourceKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{
				Ledger: ledger,
				Account:  posting.Source,
			},
			Asset: posting.Asset,
		}
		sk := string(sourceKey.Bytes())
		if expectedOutputs[sk] == nil {
			expectedOutputs[sk] = big.NewInt(0)
		}
		expectedOutputs[sk].Add(expectedOutputs[sk], amount)

		// Destination: increase input
		destKey := domain.VolumeKey{
			AccountKey: domain.AccountKey{
				Ledger: ledger,
				Account:  posting.Destination,
			},
			Asset: posting.Asset,
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
	state := &expectedTxState{}
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

