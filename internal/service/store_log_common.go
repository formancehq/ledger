package service

import (
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// ============================================================================
// Utility Functions (shared between SQLite implementations)
// ============================================================================

// accumulateBalanceDiffs accumulates balance differences from postings into a map
// balanceDiffs: map[account]map[asset]*big.Int (positive = add, negative = subtract)
func accumulateBalanceDiffs(balanceDiffs map[string]map[string]*big.Int, postings []*ledgerpb.Posting) {
	for _, posting := range postings {
		if posting == nil {
			continue
		}
		amount := posting.Amount.Value()

		// Subtract from source
		if posting.Source != "" && posting.Source != posting.Destination {
			if balanceDiffs[posting.Source] == nil {
				balanceDiffs[posting.Source] = make(map[string]*big.Int)
			}
			if balanceDiffs[posting.Source][posting.Asset] == nil {
				balanceDiffs[posting.Source][posting.Asset] = big.NewInt(0)
			}
			balanceDiffs[posting.Source][posting.Asset] = new(big.Int).Sub(balanceDiffs[posting.Source][posting.Asset], amount)
		}

		// Add to destination
		if posting.Destination != "" {
			if balanceDiffs[posting.Destination] == nil {
				balanceDiffs[posting.Destination] = make(map[string]*big.Int)
			}
			if balanceDiffs[posting.Destination][posting.Asset] == nil {
				balanceDiffs[posting.Destination][posting.Asset] = big.NewInt(0)
			}
			balanceDiffs[posting.Destination][posting.Asset] = new(big.Int).Add(balanceDiffs[posting.Destination][posting.Asset], amount)
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

	// Accumulate balance differences for all logs
	balanceDiffs := make(map[string]map[string]*big.Int)

	// Accumulate metadata operations for batch processing
	accountMetadataBatch := make(map[string]map[string]string)
	accountMetadataDeletes := make(map[string][]string)

	// Accumulate idempotency entries for batch processing
	idempotencyKeys := make(map[string]*ledgerpb.IdempotencyEntry)
	// Accumulate transaction ID to log ID mappings
	transactionIDs := make(map[uint64]uint64)
	// Accumulate reverted transaction IDs
	revertedTransactionIDs := make(map[uint64]bool)

	for _, log := range logs {
		// Validate log data
		if log.Data == nil {
			return RuntimeUpdate{}, fmt.Errorf("log data is nil for id %d", log.Id)
		}

		// Accumulate idempotency entry if present
		if log.Idempotency != nil {
			idempotencyKeys[log.Idempotency.Key] = &ledgerpb.IdempotencyEntry{
				Hash:  log.Idempotency.Hash,
				LogId: log.Id,
			}
		}

		// Accumulate balance differences and update accounts based on log type
		switch payload := log.Data.Payload.(type) {
		case *ledgerpb.LogPayload_CreatedTransaction:
			if payload.CreatedTransaction != nil && payload.CreatedTransaction.Transaction != nil {
				// Accumulate balance differences
				accumulateBalanceDiffs(balanceDiffs, payload.CreatedTransaction.Transaction.Postings)
				// Accumulate account and metadata updates for batch processing
				accumulateAccountsFromTransaction(accountMetadataBatch, payload.CreatedTransaction)
				// Store transaction ID -> log ID mapping
				if payload.CreatedTransaction.Transaction.Id != 0 {
					transactionIDs[payload.CreatedTransaction.Transaction.Id] = log.Id
				}
			}
		case *ledgerpb.LogPayload_RevertedTransaction:
			if payload.RevertedTransaction != nil {
				// Store transaction ID -> log ID mapping for reverted transaction
				if payload.RevertedTransaction.RevertedTransaction != nil && payload.RevertedTransaction.RevertedTransaction.Id != 0 {
					transactionIDs[payload.RevertedTransaction.RevertedTransaction.Id] = log.Id
					// Mark transaction as reverted
					revertedTransactionIDs[payload.RevertedTransaction.RevertedTransaction.Id] = true
				}
				// Store transaction ID -> log ID mapping for revert transaction
				if payload.RevertedTransaction.RevertTransaction != nil && payload.RevertedTransaction.RevertTransaction.Id != 0 {
					transactionIDs[payload.RevertedTransaction.RevertTransaction.Id] = log.Id
				}
				if payload.RevertedTransaction.RevertedTransaction != nil {
					// Reverse postings for balance update (subtract from destination, add to source)
					reversedPostings := make([]*ledgerpb.Posting, len(payload.RevertedTransaction.RevertedTransaction.Postings))
					for i, posting := range payload.RevertedTransaction.RevertedTransaction.Postings {
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
					accumulateBalanceDiffs(balanceDiffs, reversedPostings)
				}
			}
		case *ledgerpb.LogPayload_SavedMetadata:
			if payload.SavedMetadata != nil {
				// Accumulate metadata updates for batch processing
				accumulateMetadataFromSetMetadata(accountMetadataBatch, payload.SavedMetadata)
			}
		case *ledgerpb.LogPayload_DeletedMetadata:
			if payload.DeletedMetadata != nil {
				// Accumulate metadata deletions for batch processing
				accumulateMetadataFromDeleteMetadata(accountMetadataDeletes, payload.DeletedMetadata)
			}
		}
	}

	return RuntimeUpdate{
		BalanceDiffs:           balanceDiffs,
		AccountMetadata:        accountMetadataBatch,
		AccountMetadataDeletes: accountMetadataDeletes,
		IdempotencyKeys:        idempotencyKeys,
		TransactionIDs:         transactionIDs,
		RevertedTransactionIDs: revertedTransactionIDs,
		LastProcessedLogID:     logs[len(logs)-1].Id,
	}, nil
}
