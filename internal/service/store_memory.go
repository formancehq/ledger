package service

import (
	"context"
	"math/big"
	"sync"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// MemoryStore is an in-memory implementation of the Store interface
type MemoryStore struct {
	mu      sync.RWMutex
	logs    []ledger.Log
	volumes map[string]map[string]ledger.Volumes // account -> asset -> volumes
}

// NewMemoryStore creates a new in-memory store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		logs:    make([]ledger.Log, 0),
		volumes: make(map[string]map[string]ledger.Volumes),
	}
}

// InsertLogs inserts logs into the store and updates volumes incrementally
func (s *MemoryStore) InsertLogs(ctx context.Context, logs ...ledger.Log) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, log := range logs {
		s.logs = append(s.logs, log)

		// Update volumes incrementally
		switch log.Type {
		case ledger.NewTransactionLogType:
			if err := s.processNewTransaction(log, s.volumes); err != nil {
				return err
			}
		case ledger.RevertedTransactionLogType:
			if err := s.processRevertedTransaction(log, s.volumes); err != nil {
				return err
			}
		}
	}

	return nil
}

// GetBalance returns balances from pre-calculated volumes based on the query
// balanceQuery: map where keys are account addresses and values are arrays of assets
func (s *MemoryStore) GetBalance(ctx context.Context, balanceQuery map[string][]string) (ledger.Balances, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Convert pre-calculated volumes to balances with filtering
	balances := make(ledger.Balances)

	// If query is empty, return empty balances
	if len(balanceQuery) == 0 {
		return balances, nil
	}

	// Filter by account and assets specified in query
	for account, requestedAssets := range balanceQuery {
		accountVolumes, ok := s.volumes[account]
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

// GetLogWithIdempotencyKey returns the log with the given idempotency key
func (s *MemoryStore) GetLogWithIdempotencyKey(ctx context.Context, idempotencyKey string) (*ledger.Log, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for i := len(s.logs) - 1; i >= 0; i-- {
		if s.logs[i].IdempotencyKey == idempotencyKey {
			log := s.logs[i]
			return &log, nil
		}
	}

	return nil, nil
}

// GetLastLog returns the last log inserted in the store
func (s *MemoryStore) GetLastLog(ctx context.Context) (*ledger.Log, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(s.logs) == 0 {
		return nil, nil
	}

	lastLog := s.logs[len(s.logs)-1]
	return &lastLog, nil
}

// processNewTransaction processes a new transaction log and updates volumes
func (s *MemoryStore) processNewTransaction(log ledger.Log, volumes map[string]map[string]ledger.Volumes) error {
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
func (s *MemoryStore) processRevertedTransaction(log ledger.Log, volumes map[string]map[string]ledger.Volumes) error {
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
