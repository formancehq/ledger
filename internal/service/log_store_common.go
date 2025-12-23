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
	accountsToUpdate map[string]string,
	accountMetadataBatch map[string]map[string]interface{},
	transactionMetadataBatch map[uint64]map[string]interface{},
	transaction *ledgerpb.CreatedTransaction,
	dateStr string,
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

	// Mark accounts for batch update (keep earliest transaction timestamp if multiple)
	// dateStr is the RFC3339 formatted timestamp of the transaction
	// first_usage will be set to the earliest transaction timestamp involving this account
	for accountAddr := range accounts {
		if existingDate, exists := accountsToUpdate[accountAddr]; !exists || dateStr < existingDate {
			accountsToUpdate[accountAddr] = dateStr
		}
	}

	// Accumulate account metadata from accountMetadata in transaction
	if len(transaction.AccountMetadata) > 0 {
		// Mark accounts for batch update
		for accountAddr := range transaction.AccountMetadata {
			if existingDate, exists := accountsToUpdate[accountAddr]; !exists || dateStr < existingDate {
				accountsToUpdate[accountAddr] = dateStr
			}
		}

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

	// Accumulate transaction metadata if present
	if transaction.Transaction != nil && transaction.Transaction.Metadata != nil && len(transaction.Transaction.Metadata) > 0 {
		transactionID := transaction.Transaction.Id
		if transactionID != 0 {
			if transactionMetadataBatch[transactionID] == nil {
				transactionMetadataBatch[transactionID] = make(map[string]interface{})
			}
			for k, v := range convertMetadataToStringMap(transaction.Transaction.Metadata) {
				transactionMetadataBatch[transactionID][k] = v
			}
		}
	}
}

// accumulateAccountsFromRevertedTransaction accumulates account updates from a reverted transaction
func accumulateAccountsFromRevertedTransaction(
	accountsToUpdate map[string]string,
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

	// Mark accounts for batch update (keep earliest transaction timestamp if multiple)
	// dateStr is the RFC3339 formatted timestamp of the reverted transaction
	// first_usage will be set to the earliest transaction timestamp involving this account
	for accountAddr := range accounts {
		if existingDate, exists := accountsToUpdate[accountAddr]; !exists || dateStr < existingDate {
			accountsToUpdate[accountAddr] = dateStr
		}
	}
}

// accumulateMetadataFromSetMetadata accumulates metadata updates from SET_METADATA log
func accumulateMetadataFromSetMetadata(
	accountMetadataBatch map[string]map[string]interface{},
	transactionMetadataBatch map[uint64]map[string]interface{},
	accountsToCreate map[string]string,
	savedMetadata *ledgerpb.SavedMetadata,
	dateStr string,
) {
	if savedMetadata.TargetType == ledgerpb.MetaTargetTypeAccount {
		accountAddr := savedMetadata.GetAccountId()
		if accountAddr == "" {
			return
		}

		// Mark account for creation (keep earliest date if multiple)
		if existingDate, exists := accountsToCreate[accountAddr]; !exists || dateStr < existingDate {
			accountsToCreate[accountAddr] = dateStr
		}

		// Accumulate metadata for batch processing
		if accountMetadataBatch[accountAddr] == nil {
			accountMetadataBatch[accountAddr] = make(map[string]interface{})
		}
		for k, v := range convertMetadataToStringMap(savedMetadata.Metadata) {
			accountMetadataBatch[accountAddr][k] = v
		}
	} else if savedMetadata.TargetType == ledgerpb.MetaTargetTypeTransaction {
		transactionID := savedMetadata.GetTransactionId()
		if transactionID == 0 {
			return
		}

		// Accumulate metadata for batch processing
		if transactionMetadataBatch[transactionID] == nil {
			transactionMetadataBatch[transactionID] = make(map[string]interface{})
		}
		for k, v := range convertMetadataToStringMap(savedMetadata.Metadata) {
			transactionMetadataBatch[transactionID][k] = v
		}
	}
}

// accumulateMetadataFromDeleteMetadata accumulates metadata deletions from DELETE_METADATA log
func accumulateMetadataFromDeleteMetadata(
	accountMetadataDeletes map[string][]string,
	transactionMetadataDeletes map[uint64][]string,
	deletedMetadata *ledgerpb.DeletedMetadata,
) {
	if deletedMetadata.TargetType == ledgerpb.MetaTargetTypeAccount {
		accountAddr := deletedMetadata.GetAccountId()
		if accountAddr == "" {
			return
		}

		// Accumulate deletion for batch processing
		accountMetadataDeletes[accountAddr] = append(accountMetadataDeletes[accountAddr], deletedMetadata.Key)
	} else if deletedMetadata.TargetType == ledgerpb.MetaTargetTypeTransaction {
		transactionID := deletedMetadata.GetTransactionId()
		if transactionID == 0 {
			return
		}

		// Accumulate deletion for batch processing
		transactionMetadataDeletes[transactionID] = append(transactionMetadataDeletes[transactionID], deletedMetadata.Key)
	}
}

