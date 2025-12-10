package service

import (
	"context"
	"fmt"
	"io"
	"math/big"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// ReconstructedBalancesStore reconstructs volumes from logs by iterating through all transaction logs
type ReconstructedBalancesStore struct {
	logReader LogReader
}

// NewReconstructedBalancesStore creates a new ReconstructedBalancesStore
func NewReconstructedBalancesStore(logReader LogReader) *ReconstructedBalancesStore {
	return &ReconstructedBalancesStore{
		logReader: logReader,
	}
}

// GetBalance reconstructs balances from logs for the requested accounts and assets
func (s *ReconstructedBalancesStore) GetBalances(ctx context.Context, ledgerName string, balanceQuery map[string][]string) (ledger.Balances, error) {
	// Initialize volumes map: account -> asset -> volumes
	volumes := make(map[string]map[string]ledger.Volumes)

	// Initialize volumes map for requested accounts/assets
	for account, assets := range balanceQuery {
		if volumes[account] == nil {
			volumes[account] = make(map[string]ledger.Volumes)
		}
		for _, asset := range assets {
			volumes[account][asset] = ledger.NewEmptyVolumes()
		}
	}

	// Get all logs for this ledger
	cursorPtr, err := s.logReader.GetAllLogs(ctx, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("getting all logs: %w", err)
	}
	if cursorPtr == nil {
		return ledger.Balances{}, nil
	}
	cursor := *cursorPtr
	defer func() {
		_ = cursor.Close()
	}()

	// Iterate through all logs and reconstruct volumes
	for {
		log, err := cursor.Next(ctx)
		if err != nil {
			if err == context.Canceled || err == context.DeadlineExceeded {
				return nil, err
			}
			// io.EOF means we've processed all logs
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("reading log: %w", err)
		}

		// Only process transaction logs
		if log.Type != ledger.NewTransactionLogType && log.Type != ledger.RevertedTransactionLogType {
			continue
		}

		// Extract transaction from log
		var tx *ledger.Transaction
		switch logData := log.Data.(type) {
		case *ledger.CreatedTransaction:
			tx = &logData.Transaction
		case *ledger.RevertedTransaction:
			// For reverted transactions, we need to reverse the postings
			// The RevertedTransaction contains the original transaction that was reverted
			revertedTx := &logData.RevertedTransaction
			reversedTx := revertedTx.Reverse()
			tx = &reversedTx
		default:
			continue
		}

		// Update volumes for each posting
		for _, posting := range tx.Postings {
			// Only process if this account/asset is in our query
			if _, accountInQuery := balanceQuery[posting.Source]; accountInQuery {
				if assets, ok := balanceQuery[posting.Source]; ok {
					for _, asset := range assets {
						if asset == posting.Asset {
							// Initialize if needed
							if volumes[posting.Source] == nil {
								volumes[posting.Source] = make(map[string]ledger.Volumes)
							}
							if volumes[posting.Source][posting.Asset].Input == nil {
								volumes[posting.Source][posting.Asset] = ledger.NewEmptyVolumes()
							}
							// Add to output (source account sends money)
							volumes[posting.Source][posting.Asset].Output.Add(
								volumes[posting.Source][posting.Asset].Output,
								posting.Amount,
							)
							break
						}
					}
				}
			}

			if _, accountInQuery := balanceQuery[posting.Destination]; accountInQuery {
				if assets, ok := balanceQuery[posting.Destination]; ok {
					for _, asset := range assets {
						if asset == posting.Asset {
							// Initialize if needed
							if volumes[posting.Destination] == nil {
								volumes[posting.Destination] = make(map[string]ledger.Volumes)
							}
							if volumes[posting.Destination][posting.Asset].Input == nil {
								volumes[posting.Destination][posting.Asset] = ledger.NewEmptyVolumes()
							}
							// Add to input (destination account receives money)
							volumes[posting.Destination][posting.Asset].Input.Add(
								volumes[posting.Destination][posting.Asset].Input,
								posting.Amount,
							)
							break
						}
					}
				}
			}
		}
	}

	// Convert volumes to balances
	balances := make(ledger.Balances)
	for account, assets := range volumes {
		balances[account] = make(map[string]*big.Int)
		for asset, vol := range assets {
			balances[account][asset] = vol.Balance()
		}
	}

	return balances, nil
}
