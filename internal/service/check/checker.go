package check

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/zeebo/blake3"
)

const progressInterval = 100

// Checker verifies store integrity by replaying logs and comparing derived state.
type Checker struct {
	store *data.Store
	attrs *attributes.Attributes
}

// NewChecker creates a new Checker.
func NewChecker(store *data.Store, attrs *attributes.Attributes) *Checker {
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
func (c *Checker) Check(ctx context.Context, callback func(*servicepb.CheckStoreEvent)) error {
	lastSequence, err := c.store.GetLastSequence()
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
		expectedInputs   = make(map[string]*big.Int) // volumeKey -> cumulative input
		expectedOutputs  = make(map[string]*big.Int) // volumeKey -> cumulative output
		expectedMetadata = make(map[string]string)   // metadataKey -> value
		deletedMetadata  = make(map[string]struct{}) // metadataKey -> deleted
		ledgerIDs        = make(map[string]uint32)   // ledgerName -> ledgerID
		errorCount       int
	)

	// Pass 1: Iterate all logs, verify hash chain, and replay state
	for seq := uint64(1); seq <= lastSequence; seq++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		log, err := c.store.GetLogBySequence(seq)
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
		// Recreate the log without the Hash field, as the production code computes
		// the hash before setting it on the log.
		logForHash := &commonpb.Log{
			Sequence:    log.Sequence,
			Payload:     log.Payload,
			Idempotency: log.Idempotency,
		}
		expectedHash := processing.ComputeLogHash(hasher, lastHash, logForHash)
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
					ledgerIDs[payload.CreateLedger.Info.Name] = payload.CreateLedger.Info.Id
				}
			case *commonpb.LogPayload_DeleteLedger:
				// Nothing to track for delete
			case *commonpb.LogPayload_Apply:
				if payload.Apply != nil {
					ledgerName := payload.Apply.LedgerName
					ledgerID, ok := ledgerIDs[ledgerName]
					if !ok {
						callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_UNKNOWN_LEDGER,
							fmt.Sprintf("log %d references unknown ledger %q", seq, ledgerName),
							seq, ledgerName, "", ""))
						errorCount++
						continue
					}
					if payload.Apply.Log != nil && payload.Apply.Log.Data != nil {
						replayLedgerLog(ledgerID, payload.Apply.Log.Data, expectedInputs, expectedOutputs, expectedMetadata, deletedMetadata)
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

	for key, expectedInput := range expectedInputs {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var vk data.VolumeKey
		if err := vk.Unmarshal([]byte(key)); err != nil {
			continue
		}

		actualInput, err := c.attrs.Input.ComputeValue(c.store, maxIndex, []byte(key))
		if err != nil {
			return fmt.Errorf("computing input for %s: %w", key, err)
		}

		actualInputVal := big.NewInt(0)
		if actualInput != nil {
			actualInputVal = actualInput.Value()
		}

		if expectedInput.Cmp(actualInputVal) != 0 {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
				fmt.Sprintf("input mismatch for %s/%s: expected %s, got %s",
					vk.Account, vk.Asset, expectedInput.String(), actualInputVal.String()),
				0, ledgerNameByID(ledgerIDs, vk.LedgerID), vk.Account, vk.Asset))
			errorCount++
		}
	}

	for key, expectedOutput := range expectedOutputs {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		var vk data.VolumeKey
		if err := vk.Unmarshal([]byte(key)); err != nil {
			continue
		}

		actualOutput, err := c.attrs.Output.ComputeValue(c.store, maxIndex, []byte(key))
		if err != nil {
			return fmt.Errorf("computing output for %s: %w", key, err)
		}

		actualOutputVal := big.NewInt(0)
		if actualOutput != nil {
			actualOutputVal = actualOutput.Value()
		}

		if expectedOutput.Cmp(actualOutputVal) != 0 {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_VOLUME_MISMATCH,
				fmt.Sprintf("output mismatch for %s/%s: expected %s, got %s",
					vk.Account, vk.Asset, expectedOutput.String(), actualOutputVal.String()),
				0, ledgerNameByID(ledgerIDs, vk.LedgerID), vk.Account, vk.Asset))
			errorCount++
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

		var mk data.MetadataKey
		if err := mk.Unmarshal([]byte(key)); err != nil {
			continue
		}

		actualValue, err := c.attrs.Metadata.ComputeValue(c.store, maxIndex, []byte(key))
		if err != nil {
			return fmt.Errorf("computing metadata for %s: %w", key, err)
		}

		actualStr := ""
		if actualValue != nil {
			actualStr = actualValue.Value
		}

		if expectedValue != actualStr {
			callback(errorEvent(servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_METADATA_MISMATCH,
				fmt.Sprintf("metadata mismatch for %s/%s: expected %q, got %q",
					mk.Account, mk.Key, expectedValue, actualStr),
				0, ledgerNameByID(ledgerIDs, mk.LedgerID), mk.Account, ""))
			errorCount++
		}
	}

	return nil
}

// replayLedgerLog updates expected state based on a ledger log payload.
func replayLedgerLog(
	ledgerID uint32,
	payload *commonpb.LedgerLogPayload,
	expectedInputs map[string]*big.Int,
	expectedOutputs map[string]*big.Int,
	expectedMetadata map[string]string,
	deletedMetadata map[string]struct{},
) {
	switch p := payload.Payload.(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		if p.CreatedTransaction == nil || p.CreatedTransaction.Transaction == nil {
			return
		}
		applyPostings(ledgerID, p.CreatedTransaction.Transaction.Postings, expectedInputs, expectedOutputs)
		// Apply account metadata from the transaction
		for account, metaSet := range p.CreatedTransaction.AccountMetadata {
			if metaSet != nil {
				for _, m := range metaSet.Metadata {
					mk := data.MetadataKey{
						AccountKey: data.AccountKey{
							LedgerID: ledgerID,
							Account:  account,
						},
						Key: m.Key,
					}
					key := string(mk.Bytes())
					if m.Value != nil {
						expectedMetadata[key] = m.Value.Value
						delete(deletedMetadata, key)
					}
				}
			}
		}

	case *commonpb.LedgerLogPayload_RevertedTransaction:
		if p.RevertedTransaction == nil || p.RevertedTransaction.RevertTransaction == nil {
			return
		}
		applyPostings(ledgerID, p.RevertedTransaction.RevertTransaction.Postings, expectedInputs, expectedOutputs)

	case *commonpb.LedgerLogPayload_SavedMetadata:
		if p.SavedMetadata == nil || p.SavedMetadata.Target == nil {
			return
		}
		switch target := p.SavedMetadata.Target.Target.(type) {
		case *commonpb.Target_Account:
			if p.SavedMetadata.Metadata != nil {
				for _, m := range p.SavedMetadata.Metadata.Metadata {
					mk := data.MetadataKey{
						AccountKey: data.AccountKey{
							LedgerID: ledgerID,
							Account:  target.Account.Addr,
						},
						Key: m.Key,
					}
					key := string(mk.Bytes())
					if m.Value != nil {
						expectedMetadata[key] = m.Value.Value
						delete(deletedMetadata, key)
					}
				}
			}
		}

	case *commonpb.LedgerLogPayload_DeletedMetadata:
		if p.DeletedMetadata == nil || p.DeletedMetadata.Target == nil {
			return
		}
		switch target := p.DeletedMetadata.Target.Target.(type) {
		case *commonpb.Target_Account:
			mk := data.MetadataKey{
				AccountKey: data.AccountKey{
					LedgerID: ledgerID,
					Account:  target.Account.Addr,
				},
				Key: p.DeletedMetadata.Key,
			}
			key := string(mk.Bytes())
			delete(expectedMetadata, key)
			deletedMetadata[key] = struct{}{}
		}
	}
}

// applyPostings applies postings to the expected volume maps.
func applyPostings(
	ledgerID uint32,
	postings []*commonpb.Posting,
	expectedInputs map[string]*big.Int,
	expectedOutputs map[string]*big.Int,
) {
	for _, posting := range postings {
		amount := posting.Amount.Value()

		// Source: increase output
		sourceKey := data.VolumeKey{
			AccountKey: data.AccountKey{
				LedgerID: ledgerID,
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
		destKey := data.VolumeKey{
			AccountKey: data.AccountKey{
				LedgerID: ledgerID,
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

func ledgerNameByID(ledgerIDs map[string]uint32, id uint32) string {
	for name, lid := range ledgerIDs {
		if lid == id {
			return name
		}
	}
	return fmt.Sprintf("unknown-ledger-id-%d", id)
}
