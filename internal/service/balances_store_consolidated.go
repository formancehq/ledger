package service

import (
	"context"
	"fmt"
	"math/big"

	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// HotDiffBalancesProvider provides access to in-memory balances from the FSM
type HotDiffBalancesProvider interface {
	// GetInMemoryBalances returns the in-memory balance diff for a ledger
	// This represents the changes since the last snapshot
	GetInMemoryDiffBalances(ledgerName string) ledger.Balances
}

// ConsolidatedBalancesStore combines balances from an underlying BalancesStore with in-memory balances from the FSM
type ConsolidatedBalancesStore struct {
	underlying  BalancesStore
	fsmProvider HotDiffBalancesProvider
}

// NewConsolidatedBalancesStore creates a new ConsolidatedBalancesStore
func NewConsolidatedBalancesStore(underlying BalancesStore, fsmProvider HotDiffBalancesProvider) *ConsolidatedBalancesStore {
	return &ConsolidatedBalancesStore{
		underlying:  underlying,
		fsmProvider: fsmProvider,
	}
}

// GetBalance combines balances from the underlying store with in-memory balances from the FSM
func (s *ConsolidatedBalancesStore) GetBalances(ctx context.Context, ledgerName string, balanceQuery map[string][]string) (ledger.Balances, error) {
	// Get balances from the underlying store (persistent balances from snapshot + logs)
	underlyingBalances, err := s.underlying.GetBalances(ctx, ledgerName, balanceQuery)
	if err != nil {
		return nil, fmt.Errorf("getting balances from underlying store: %w", err)
	}

	// Get in-memory balance diff from FSM
	inMemoryDiffBalances := s.fsmProvider.GetInMemoryDiffBalances(ledgerName)

	// Use underlying balances as base (no need to copy)
	result := underlyingBalances

	// Add in-memory balance diff for requested accounts/assets
	for account, assets := range balanceQuery {
		// Initialize account if needed
		if result[account] == nil {
			result[account] = make(map[string]*big.Int)
		}

		for _, asset := range assets {
			// Initialize asset balance if needed
			if result[account][asset] == nil {
				result[account][asset] = big.NewInt(0)
			}

			// Add in-memory balance diff if it exists
			if inMemoryDiffBalances != nil {
				if accountBalances, ok := inMemoryDiffBalances[account]; ok {
					if assetBalance, ok := accountBalances[asset]; ok {
						result[account][asset] = new(big.Int).Add(result[account][asset], assetBalance)
					}
				}
			}
		}
	}

	return result, nil
}
