package service

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
	"github.com/formancehq/numscript"
)

type unitOfWork struct {
	store.Store
	KeySetLocker
	ledgerID uint32
	releases []func()
}

func (s *unitOfWork) LockKeys(ctx context.Context, keys ...string) (func(), error) {
	release, err := s.KeySetLocker.LockKeys(ctx, prepend(s.ledgerID, keys...)...)
	if err != nil {
		return nil, err
	}
	s.releases = append(s.releases, release)

	return release, nil
}

func (s *unitOfWork) TryLockKeys(ctx context.Context, keys ...string) (func(), error) {
	release, err := s.KeySetLocker.TryLockKeys(ctx, prepend(s.ledgerID, keys...)...)
	if err != nil {
		return nil, err
	}
	s.releases = append(s.releases, release)

	return release, nil
}

func (s *unitOfWork) GetBalances(ctx context.Context, q numscript.BalanceQuery) (numscript.Balances, error) {
	// Convert numscript.BalanceQuery to our format
	balanceQuery := make(map[string][]string)
	for account, assets := range q {
		balanceQuery[account] = assets
	}

	lockKeys := makeBalanceLockKeys(balanceQuery)
	_, err := s.LockKeys(ctx, prepend(s.ledgerID, lockKeys...)...)
	if err != nil {
		return nil, err
	}

	balances, err := s.Store.GetBalances(ctx, s.ledgerID, balanceQuery)
	if err != nil {
		return nil, err
	}

	// Convert to numscript.Balances format
	result := make(numscript.Balances)
	for account, accountBalances := range balances {
		result[account] = make(map[string]*big.Int)
		for asset, balance := range accountBalances {
			result[account][asset] = balance
		}
	}

	return result, nil
}

// GetAccountsMetadata retrieves account metadata for accounts in the query
func (s *unitOfWork) GetAccountsMetadata(ctx context.Context, q numscript.MetadataQuery) (numscript.AccountsMetadata, error) {
	// Convert numscript.MetadataQuery (map[string]struct{}) to []string
	accounts := make([]string, 0, len(q))
	for address := range q {
		accounts = append(accounts, address)
	}

	// Get metadata from the runtime store
	metadataMap, err := s.GetAccountMetadata(ctx, s.ledgerID, accounts)
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

func (s *unitOfWork) ReleaseLocks() {
	for _, release := range s.releases {
		release()
	}
}

func (s *unitOfWork) IsTransactionReverted(ctx context.Context, id uint64) (bool, error) {
	return s.Store.IsTransactionReverted(ctx, s.ledgerID, id)
}

func (s *unitOfWork) GetLogIDForTransactionID(ctx context.Context, id uint64) (uint64, error) {
	return s.Store.GetLogIDForTransactionID(ctx, s.ledgerID, id)
}

func (s *unitOfWork) GetLogByID(ctx context.Context, id uint64) (*ledgerpb.Log, error) {
	return s.Store.GetLogByID(ctx, s.ledgerID, id)
}

func makeBalanceLockKeys(balanceQuery map[string][]string) []string {
	lockKeys := make([]string, 0)
	for account, assets := range balanceQuery {
		for _, asset := range assets {
			lockKeys = append(lockKeys, fmt.Sprintf("%s:%s", account, asset))
		}
	}
	return lockKeys
}

func prepend(prefix uint32, keys ...string) []string {
	result := make([]string, len(keys))
	for i, key := range keys {
		result[i] = fmt.Sprintf("%d/%s", prefix, key)
	}
	return result
}
