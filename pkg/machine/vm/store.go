package vm

import (
	"context"
	"math/big"

	"github.com/numary/ledger/pkg/core"
	storageerrors "github.com/numary/ledger/pkg/storage/errors"
)

type Store interface {
	GetBalanceFromLogs(ctx context.Context, address, asset string) (*big.Int, error)
	GetMetadataFromLogs(ctx context.Context, address, key string) (string, error)
}

type emptyStore struct{}

func (e *emptyStore) GetBalanceFromLogs(ctx context.Context, address, asset string) (*big.Int, error) {
	return new(big.Int), nil
}

func (e *emptyStore) GetMetadataFromLogs(ctx context.Context, address, key string) (string, error) {
	return "", storageerrors.ErrNotFound
}

var _ Store = (*emptyStore)(nil)

var EmptyStore = &emptyStore{}

type AccountWithBalances struct {
	core.Account
	Balances map[string]*big.Int
}

type StaticStore map[string]*AccountWithBalances

func (s StaticStore) GetBalanceFromLogs(ctx context.Context, address, asset string) (*big.Int, error) {
	account, ok := s[address]
	if !ok {
		return new(big.Int), nil
	}
	balance, ok := account.Balances[asset]
	if !ok {
		return new(big.Int), nil
	}

	return balance, nil
}

func (s StaticStore) GetMetadataFromLogs(ctx context.Context, address, key string) (string, error) {
	account, ok := s[address]
	if !ok {
		return "", nil
	}
	metadata, ok := account.Metadata[key]
	if !ok {
		return "", storageerrors.ErrNotFound
	}

	return metadata, nil
}

var _ Store = StaticStore{}
