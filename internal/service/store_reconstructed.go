package service

import (
	"context"
	"fmt"
	"io"
	"math/big"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// ReconstructedVolumesStore reconstructs volumes from logs using a LogReader
type ReconstructedVolumesStore struct {
	logReader LogReader
}

// NewReconstructedVolumesStore creates a new ReconstructedVolumesStore
func NewReconstructedVolumesStore(logReader LogReader) *ReconstructedVolumesStore {
	return &ReconstructedVolumesStore{
		logReader: logReader,
	}
}

// GetBalance reconstructs balances from all logs
func (s *ReconstructedVolumesStore) GetBalance(ctx context.Context, balanceQuery map[string][]string) (ledger.Balances, error) {
	// Initialize volumes map
	volumes := make(map[string]map[string]ledger.Volumes)

	// Get cursor for all logs
	cursorPtr, err := s.logReader.GetAllLogs(ctx)
	if err != nil {
		return nil, fmt.Errorf("getting all logs: %w", err)
	}
	if cursorPtr == nil {
		return nil, fmt.Errorf("cursor is nil")
	}
	cursor := *cursorPtr
	defer cursor.Close()

	// Process all logs to reconstruct volumes
	for {
		log, err := cursor.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("reading log from cursor: %w", err)
		}

		// Process transaction logs to update volumes
		switch log.Type {
		case ledger.NewTransactionLogType:
			if err := s.processNewTransaction(log, volumes); err != nil {
				return nil, err
			}
		case ledger.RevertedTransactionLogType:
			if err := s.processRevertedTransaction(log, volumes); err != nil {
				return nil, err
			}
		}
	}

	// Filter results based on balanceQuery
	if len(balanceQuery) == 0 {
		return ledger.Balances{}, nil
	}

	result := make(ledger.Balances)
	for account, assets := range balanceQuery {
		if accountVolumes, ok := volumes[account]; ok {
			result[account] = make(map[string]*big.Int)
			for _, asset := range assets {
				if assetVolumes, ok := accountVolumes[asset]; ok {
					result[account][asset] = assetVolumes.Balance()
				} else {
					// Account exists but asset doesn't, return zero balance
					result[account][asset] = big.NewInt(0)
				}
			}
		} else {
			// Account doesn't exist, return zero balances for requested assets
			result[account] = make(map[string]*big.Int)
			for _, asset := range assets {
				result[account][asset] = big.NewInt(0)
			}
		}
	}

	return result, nil
}

// processNewTransaction processes a new transaction log and updates volumes
func (s *ReconstructedVolumesStore) processNewTransaction(log ledger.Log, volumes map[string]map[string]ledger.Volumes) error {
	payload, ok := log.Data.(*ledger.CreatedTransaction)
	if !ok {
		return fmt.Errorf("invalid transaction payload type")
	}

	for _, posting := range payload.Transaction.Postings {
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

		// Update source account (output)
		sourceVolumes := volumes[posting.Source][posting.Asset]
		sourceVolumes.Output = new(big.Int).Add(sourceVolumes.Output, posting.Amount)
		volumes[posting.Source][posting.Asset] = sourceVolumes

		// Update destination account (input)
		destVolumes := volumes[posting.Destination][posting.Asset]
		destVolumes.Input = new(big.Int).Add(destVolumes.Input, posting.Amount)
		volumes[posting.Destination][posting.Asset] = destVolumes
	}

	return nil
}

// processRevertedTransaction processes a reverted transaction log and updates volumes
func (s *ReconstructedVolumesStore) processRevertedTransaction(log ledger.Log, volumes map[string]map[string]ledger.Volumes) error {
	payload, ok := log.Data.(*ledger.RevertedTransaction)
	if !ok {
		return fmt.Errorf("invalid reverted transaction payload type")
	}

	// Process the reverted transaction (postings are already reversed)
	for _, posting := range payload.RevertTransaction.Postings {
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

		// Update source account (output)
		sourceVolumes := volumes[posting.Source][posting.Asset]
		sourceVolumes.Output = new(big.Int).Add(sourceVolumes.Output, posting.Amount)
		volumes[posting.Source][posting.Asset] = sourceVolumes

		// Update destination account (input)
		destVolumes := volumes[posting.Destination][posting.Asset]
		destVolumes.Input = new(big.Int).Add(destVolumes.Input, posting.Amount)
		volumes[posting.Destination][posting.Asset] = destVolumes
	}

	return nil
}
