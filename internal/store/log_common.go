package store

import (
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// ============================================================================
// Utility Functions (shared between store implementations)
// ============================================================================

// accumulateBalanceDiffs accumulates balance differences from postings into a map
// balanceDiffs: map[string]map[string]map[asset]*big.Int (positive = add, negative = subtract)
func accumulateBalanceDiffs(balanceDiffs map[string]map[string]*big.Int, postings []*ledgerpb.Posting) {
	for _, posting := range postings {
		if posting == nil {
			continue
		}
		amount := posting.Amount.Value()

		// Subtract from source
		if posting.Source != "" && posting.Source != posting.Destination {
			sourceKey := posting.Source
			if balanceDiffs[sourceKey] == nil {
				balanceDiffs[sourceKey] = make(map[string]*big.Int)
			}
			if balanceDiffs[sourceKey][posting.Asset] == nil {
				balanceDiffs[sourceKey][posting.Asset] = big.NewInt(0)
			}
			balanceDiffs[sourceKey][posting.Asset] = new(big.Int).Sub(balanceDiffs[sourceKey][posting.Asset], amount)
		}

		// Add to destination
		if posting.Destination != "" {
			destKey := posting.Destination
			if balanceDiffs[destKey] == nil {
				balanceDiffs[destKey] = make(map[string]*big.Int)
			}
			if balanceDiffs[destKey][posting.Asset] == nil {
				balanceDiffs[destKey][posting.Asset] = big.NewInt(0)
			}
			balanceDiffs[destKey][posting.Asset] = new(big.Int).Add(balanceDiffs[destKey][posting.Asset], amount)
		}
	}
}

// accumulateAccountsFromTransaction accumulates account and metadata updates from a transaction
func accumulateAccountsFromTransaction(
	accountMetadataBatch map[string]map[string]string,
	transaction *ledgerpb.CreatedTransaction,
) {
	postings := transaction.Transaction.Postings

	// Get unique accounts from postings
	accounts := make(map[string]bool)
	for _, posting := range postings {
		if posting == nil {
			continue
		}
		if posting.Source != "" {
			accounts[posting.Source] = true
		}
		if posting.Destination != "" {
			accounts[posting.Destination] = true
		}
	}

	// Accumulate account metadata from accountMetadata in transaction
	if len(transaction.AccountMetadata) > 0 {
		// Accumulate account metadata for batch processing
		for accountAddr, accountMetaStruct := range transaction.AccountMetadata {
			if accountMetadataBatch[accountAddr] == nil {
				accountMetadataBatch[accountAddr] = make(map[string]string)
			}
			for k, v := range accountMetaStruct.Entries {
				accountMetadataBatch[accountAddr][k] = v
			}
		}
	}
}

// accumulateMetadataFromSetMetadata accumulates metadata updates from SET_METADATA log
func accumulateMetadataFromSetMetadata(
	accountMetadataBatch map[string]map[string]string,
	savedMetadata *ledgerpb.SavedMetadata,
) {
	if savedMetadata.Target.GetAccount() == nil {
		return
	}

	accountAddr := savedMetadata.Target.GetAccount().Addr
	if accountAddr == "" {
		return
	}

	// Accumulate metadata for batch processing
	if accountMetadataBatch[accountAddr] == nil {
		accountMetadataBatch[accountAddr] = make(map[string]string)
	}
	for k, v := range savedMetadata.Metadata {
		accountMetadataBatch[accountAddr][k] = v
	}
}

// accumulateMetadataFromDeleteMetadata accumulates metadata deletions from DELETE_METADATA log
func accumulateMetadataFromDeleteMetadata(
	accountMetadataDeletes map[string][]string,
	deletedMetadata *ledgerpb.DeletedMetadata,
) {
	if deletedMetadata.Target.GetAccount() == nil {
		return
	}

	accountAddr := deletedMetadata.Target.GetAccount().GetAddr()
	if accountAddr == "" {
		return
	}

	// Accumulate deletion for batch processing
	accountMetadataDeletes[accountAddr] = append(accountMetadataDeletes[accountAddr], deletedMetadata.Key)
}

// LogsToRuntimeUpdate converts logs to RuntimeUpdate by aggregating balance differences,
// metadata updates, and idempotency keys from the logs.
// This is the canonical implementation used by both the FSM and tests.
func LogsToRuntimeUpdate(logs []*ledgerpb.Log) (RuntimeUpdate, error) {
	if len(logs) == 0 {
		return RuntimeUpdate{}, nil
	}

	balanceDiffs := make(map[string]map[string]map[string]*big.Int)
	accountMetadataBatch := make(map[string]map[string]map[string]string)
	accountMetadataDeletes := make(map[string]map[string][]string)
	transactionIDs := make(map[string]map[uint64]uint64)
	revertedTransactionIDs := make(map[string]map[uint64]bool)

	for _, log := range logs {
		// Validate log data
		if log.Data == nil {
			return RuntimeUpdate{}, fmt.Errorf("log data is nil for id %d", log.Id)
		}

		ledger := log.Ledger

		// Accumulate balance differences and update accounts based on log type
		switch payload := log.Data.Payload.(type) {
		case *ledgerpb.LogPayload_CreatedTransaction:
			if payload.CreatedTransaction != nil && payload.CreatedTransaction.Transaction != nil {

				if balanceDiffs[log.Ledger] == nil {
					balanceDiffs[log.Ledger] = make(map[string]map[string]*big.Int)
				}

				accumulateBalanceDiffs(balanceDiffs[log.Ledger], payload.CreatedTransaction.Transaction.Postings)

				if accountMetadataBatch[log.Ledger] == nil {
					accountMetadataBatch[log.Ledger] = make(map[string]map[string]string)
				}
				accumulateAccountsFromTransaction(accountMetadataBatch[log.Ledger], payload.CreatedTransaction)
				// Store transaction ID -> log ID mapping
				if payload.CreatedTransaction.Transaction.Id != 0 {
					if transactionIDs[log.Ledger] == nil {
						transactionIDs[log.Ledger] = make(map[uint64]uint64)
					}
					transactionIDs[log.Ledger][payload.CreatedTransaction.Transaction.Id] = log.Id
				}
			}
		case *ledgerpb.LogPayload_RevertedTransaction:
			if payload.RevertedTransaction != nil {
				// Store transaction ID -> log ID mapping for reverted transaction
				if transactionIDs[log.Ledger] == nil {
					transactionIDs[log.Ledger] = make(map[uint64]uint64)
				}
				transactionIDs[log.Ledger][payload.RevertedTransaction.RevertedTransactionId] = log.Id
				// Mark transaction as reverted
				if revertedTransactionIDs[log.Ledger] == nil {
					revertedTransactionIDs[log.Ledger] = make(map[uint64]bool)
				}
				revertedTransactionIDs[log.Ledger][payload.RevertedTransaction.RevertedTransactionId] = true

				// Store transaction ID -> log ID mapping for revert transaction
				if transactionIDs[log.Ledger] == nil {
					transactionIDs[log.Ledger] = make(map[uint64]uint64)
				}
				transactionIDs[log.Ledger][payload.RevertedTransaction.RevertTransaction.Id] = log.Id

				// Reverse postings for balance update (subtract from destination, add to source)
				reversedPostings := make([]*ledgerpb.Posting, len(payload.RevertedTransaction.RevertTransaction.Postings))
				for i, posting := range payload.RevertedTransaction.RevertTransaction.Postings {
					if posting != nil {
						reversedPostings[i] = &ledgerpb.Posting{
							Source:      posting.Destination,
							Destination: posting.Source,
							Asset:       posting.Asset,
							Amount:      posting.Amount,
						}
					}
				}
				// Accumulate balance differences (reversed)
				if balanceDiffs[log.Ledger] == nil {
					balanceDiffs[log.Ledger] = make(map[string]map[string]*big.Int)
				}
				accumulateBalanceDiffs(balanceDiffs[ledger], reversedPostings)
			}
		case *ledgerpb.LogPayload_SavedMetadata:
			if payload.SavedMetadata != nil {
				// Accumulate metadata updates for batch processing
				if accountMetadataBatch[ledger] == nil {
					accountMetadataBatch[ledger] = make(map[string]map[string]string)
				}
				accumulateMetadataFromSetMetadata(accountMetadataBatch[ledger], payload.SavedMetadata)
			}
		case *ledgerpb.LogPayload_DeletedMetadata:
			if payload.DeletedMetadata != nil {
				// Accumulate metadata deletions for batch processing
				if accountMetadataDeletes[ledger] == nil {
					accountMetadataDeletes[ledger] = make(map[string][]string)
				}
				accumulateMetadataFromDeleteMetadata(accountMetadataDeletes[ledger], payload.DeletedMetadata)
			}
		}
	}

	return RuntimeUpdate{
		BalanceDiffs:           balanceDiffs,
		AccountMetadata:        accountMetadataBatch,
		AccountMetadataDeletes: accountMetadataDeletes,
		TransactionIDs:         transactionIDs,
		RevertedTransactionIDs: revertedTransactionIDs,
	}, nil
}
