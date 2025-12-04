package service

import (
	"context"
	"math/big"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// ReconstructedVolumesStore is a VolumesStore implementation that reconstructs volumes
// from logs stored in a LogStore
type ReconstructedVolumesStore struct {
	logStore LogStore
}

// NewReconstructedVolumesStore creates a new VolumesStore that reconstructs volumes from logs
func NewReconstructedVolumesStore(logStore LogStore) *ReconstructedVolumesStore {
	return &ReconstructedVolumesStore{
		logStore: logStore,
	}
}

// GetBalance reconstructs volumes from all logs and returns balances based on the query
func (r *ReconstructedVolumesStore) GetBalance(ctx context.Context, balanceQuery map[string][]string) (ledger.Balances, error) {
	// Get all logs
	logs, err := r.logStore.GetAllLogs(ctx)
	if err != nil {
		return nil, err
	}

	// Reconstruct volumes from logs
	volumes := make(map[string]map[string]ledger.Volumes) // account -> asset -> volumes

	for _, log := range logs {
		switch log.Type {
		case ledger.NewTransactionLogType:
			if err := r.processNewTransaction(log, volumes); err != nil {
				return nil, err
			}
		case ledger.RevertedTransactionLogType:
			if err := r.processRevertedTransaction(log, volumes); err != nil {
				return nil, err
			}
		}
	}

	// Convert volumes to balances with filtering
	balances := make(ledger.Balances)

	// If query is empty, return empty balances
	if len(balanceQuery) == 0 {
		return balances, nil
	}

	// Filter by account and assets specified in query
	for account, requestedAssets := range balanceQuery {
		accountVolumes, ok := volumes[account]
		if !ok {
			// Account doesn't exist, return empty balances for this account
			balances[account] = make(map[string]*big.Int)
			continue
		}

		balances[account] = make(map[string]*big.Int)

		// If no assets specified for this account, return all assets
		if len(requestedAssets) == 0 {
			for asset, vol := range accountVolumes {
				balance := new(big.Int).Sub(vol.Input, vol.Output)
				balances[account][asset] = balance
			}
		} else {
			// Filter by requested assets
			for _, asset := range requestedAssets {
				if vol, ok := accountVolumes[asset]; ok {
					balance := new(big.Int).Sub(vol.Input, vol.Output)
					balances[account][asset] = balance
				} else {
					// Asset doesn't exist for this account, balance is 0
					balances[account][asset] = big.NewInt(0)
				}
			}
		}
	}

	return balances, nil
}

// processNewTransaction processes a new transaction log and updates volumes
func (r *ReconstructedVolumesStore) processNewTransaction(log ledger.Log, volumes map[string]map[string]ledger.Volumes) error {
	payload, ok := log.Data.(*ledger.CreatedTransaction)
	if !ok {
		return nil // Skip if not a CreatedTransaction
	}

	tx := payload.Transaction
	for _, posting := range tx.Postings {
		// Initialize account if needed
		if volumes[posting.Source] == nil {
			volumes[posting.Source] = make(map[string]ledger.Volumes)
		}
		if volumes[posting.Destination] == nil {
			volumes[posting.Destination] = make(map[string]ledger.Volumes)
		}

		// Initialize asset volumes if needed
		if _, ok := volumes[posting.Source][posting.Asset]; !ok {
			volumes[posting.Source][posting.Asset] = ledger.NewEmptyVolumes()
		}
		if _, ok := volumes[posting.Destination][posting.Asset]; !ok {
			volumes[posting.Destination][posting.Asset] = ledger.NewEmptyVolumes()
		}

		// Update volumes (create new big.Int instances to avoid shared mutations)
		// Source account: add to output
		sourceVol := volumes[posting.Source][posting.Asset]
		sourceVol.Output = new(big.Int).Add(sourceVol.Output, posting.Amount)
		volumes[posting.Source][posting.Asset] = sourceVol

		// Destination account: add to input
		destVol := volumes[posting.Destination][posting.Asset]
		destVol.Input = new(big.Int).Add(destVol.Input, posting.Amount)
		volumes[posting.Destination][posting.Asset] = destVol
	}

	return nil
}

// processRevertedTransaction processes a reverted transaction log
// The RevertTransaction contains the postings that reverse the original transaction
func (r *ReconstructedVolumesStore) processRevertedTransaction(log ledger.Log, volumes map[string]map[string]ledger.Volumes) error {
	payload, ok := log.Data.(*ledger.RevertedTransaction)
	if !ok {
		return nil // Skip if not a RevertedTransaction
	}

	// Process the revert transaction (which already has reversed postings)
	tx := payload.RevertTransaction
	for _, posting := range tx.Postings {
		// Initialize account if needed
		if volumes[posting.Source] == nil {
			volumes[posting.Source] = make(map[string]ledger.Volumes)
		}
		if volumes[posting.Destination] == nil {
			volumes[posting.Destination] = make(map[string]ledger.Volumes)
		}

		// Initialize asset volumes if needed
		if _, ok := volumes[posting.Source][posting.Asset]; !ok {
			volumes[posting.Source][posting.Asset] = ledger.NewEmptyVolumes()
		}
		if _, ok := volumes[posting.Destination][posting.Asset]; !ok {
			volumes[posting.Destination][posting.Asset] = ledger.NewEmptyVolumes()
		}

		// Process the revert transaction like a normal transaction
		// (the postings are already reversed)
		// Create new big.Int instances to avoid shared mutations
		sourceVol := volumes[posting.Source][posting.Asset]
		sourceVol.Output = new(big.Int).Add(sourceVol.Output, posting.Amount)
		volumes[posting.Source][posting.Asset] = sourceVol

		destVol := volumes[posting.Destination][posting.Asset]
		destVol.Input = new(big.Int).Add(destVol.Input, posting.Amount)
		volumes[posting.Destination][posting.Asset] = destVol
	}

	return nil
}

// ReconstructedStore combines a LogStore with a ReconstructedVolumesStore to create a Store
type ReconstructedStore struct {
	LogStore
	VolumesStore
}

// NewReconstructedStore creates a new Store that uses a LogStore for logs
// and reconstructs volumes from logs for balance queries
func NewReconstructedStore(logStore LogStore) *ReconstructedStore {
	return &ReconstructedStore{
		LogStore:     logStore,
		VolumesStore: NewReconstructedVolumesStore(logStore),
	}
}
