package service

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/formancehq/numscript"
)

type unitOfWork struct {
	*store.Store
	KeySetLocker
	releases []func()
}

func (s *unitOfWork) LockKeys(ctx context.Context, keys ...string) (func(), error) {
	release, err := s.KeySetLocker.LockKeys(ctx, keys...)
	if err != nil {
		return nil, err
	}
	s.releases = append(s.releases, release)

	return release, nil
}

func (s *unitOfWork) ReleaseLocks() {
	for _, release := range s.releases {
		release()
	}
}

func (s *unitOfWork) IsTransactionReverted(_ context.Context, ledgerID uint32, id uint64) (bool, error) {
	return s.Store.IsTransactionReverted(ledgerID, id)
}

func (s *unitOfWork) GetSequenceForTransactionID(_ context.Context, ledgerID uint32, id uint64) (uint64, error) {
	return s.Store.GetSequenceForTransactionID(ledgerID, id)
}

func (s *unitOfWork) GetLogBySequence(_ context.Context, sequence uint64) (*commonpb.Log, error) {
	return s.Store.GetLogBySequence(sequence)
}

func (s *unitOfWork) GetBalanceDiffs(ledgerID uint32, query store.BalanceDiffsQuery) (store.BalanceDiffsResult, error) {
	return s.Store.GetBalanceDiffs(ledgerID, query)
}

// numscriptStore wraps unitOfWork to implement numscript interfaces
// It holds ledgerID since numscript interface methods can't have additional parameters
type numscriptStore struct {
	*unitOfWork
	ledgerID uint32
}

func (s *numscriptStore) GetBalances(ctx context.Context, q numscript.BalanceQuery) (numscript.Balances, error) {
	// Convert numscript.BalanceQuery to our format
	balanceQuery := make(store.BalanceDiffsQuery)
	for account, assets := range q {
		balanceQuery[account] = assets
	}

	lockKeys := makeBalanceLockKeys(s.ledgerID, balanceQuery)
	_, err := s.LockKeys(ctx, lockKeys...)
	if err != nil {
		return nil, err
	}

	balanceDiffs, err := s.Store.GetBalanceDiffs(s.ledgerID, balanceQuery)
	if err != nil {
		return nil, err
	}

	// Compute balances from diffs
	result := make(numscript.Balances)
	for account, assetDiffs := range balanceDiffs {
		result[account] = make(map[string]*big.Int)
		for asset, diffs := range assetDiffs {
			balance := big.NewInt(0)
			for _, diff := range diffs {
				balance = balance.Add(balance, diff.Diff.Value())
			}
			result[account][asset] = balance
		}
	}

	return result, nil
}

// GetAccountsMetadata retrieves account metadata for accounts in the query
func (s *numscriptStore) GetAccountsMetadata(_ context.Context, q numscript.MetadataQuery) (numscript.AccountsMetadata, error) {
	// Convert numscript.MetadataQuery (map[string]struct{}) to []string
	accounts := make([]string, 0, len(q))
	for address := range q {
		accounts = append(accounts, address)
	}

	// Get metadata from the runtime store
	metadataMap, err := s.GetAccountMetadata(s.ledgerID, accounts)
	if err != nil {
		return nil, err
	}

	// Convert to numscript.AccountsMetadata format (map[string]map[string]string)
	result := make(numscript.AccountsMetadata)
	for address, accountMetadata := range metadataMap {
		result[address] = accountMetadata
	}

	// Ensure all requested accounts are in the result (even if empty)
	for address := range q {
		if _, exists := result[address]; !exists {
			result[address] = make(map[string]string)
		}
	}

	return result, nil
}

func makeBalanceLockKeys(ledgerID uint32, balanceQuery map[string][]string) []string {
	lockKeys := make([]string, 0)
	for account, assets := range balanceQuery {
		for _, asset := range assets {
			lockKeys = append(lockKeys, fmt.Sprintf("%d/%s:%s", ledgerID, account, asset))
		}
	}
	return lockKeys
}
