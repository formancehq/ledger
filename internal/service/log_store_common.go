package service

import (
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// ============================================================================
// Utility Functions (shared between SQLite implementations)
// ============================================================================

// convertMetadataToStringMap converts map[string]string to map[string]interface{}
func convertMetadataToStringMap(m map[string]string) map[string]interface{} {
	result := make(map[string]interface{}, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

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
	accountMetadataBatch map[string]map[string]interface{},
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
				accountMetadataBatch[accountAddr] = make(map[string]interface{})
			}
			for k, v := range convertMetadataToStringMap(accountMetaStruct.Entries) {
				accountMetadataBatch[accountAddr][k] = v
			}
		}
	}
}

// accumulateAccountsFromRevertedTransaction accumulates account updates from a reverted transaction
func accumulateAccountsFromRevertedTransaction(
	revertedTransaction *ledgerpb.RevertedTransaction,
	dateStr string,
) {
	postings := revertedTransaction.RevertedTransaction.Postings

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
}

// accumulateMetadataFromSetMetadata accumulates metadata updates from SET_METADATA log
func accumulateMetadataFromSetMetadata(
	accountMetadataBatch map[string]map[string]interface{},
	savedMetadata *ledgerpb.SavedMetadata,
) {
	if savedMetadata.TargetType == ledgerpb.MetaTargetTypeAccount {
		accountAddr := savedMetadata.GetAccountId()
		if accountAddr == "" {
			return
		}

		// Accumulate metadata for batch processing
		if accountMetadataBatch[accountAddr] == nil {
			accountMetadataBatch[accountAddr] = make(map[string]interface{})
		}
		for k, v := range convertMetadataToStringMap(savedMetadata.Metadata) {
			accountMetadataBatch[accountAddr][k] = v
		}
	}
}

// accumulateMetadataFromDeleteMetadata accumulates metadata deletions from DELETE_METADATA log
func accumulateMetadataFromDeleteMetadata(
	accountMetadataDeletes map[string][]string,
	deletedMetadata *ledgerpb.DeletedMetadata,
) {
	if deletedMetadata.TargetType == ledgerpb.MetaTargetTypeAccount {
		accountAddr := deletedMetadata.GetAccountId()
		if accountAddr == "" {
			return
		}

		// Accumulate deletion for batch processing
		accountMetadataDeletes[accountAddr] = append(accountMetadataDeletes[accountAddr], deletedMetadata.Key)
	}
}

