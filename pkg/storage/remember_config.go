package storage

import (
	"context"
	"github.com/numary/ledger/pkg/config"
	"github.com/numary/ledger/pkg/core"
)

type rememberConfigStorage struct {
	Store
}

func (s *rememberConfigStorage) SaveTransactions(ctx context.Context, txs []core.Transaction) (map[int]error, error) {
	defer config.Remember(ctx, s.Name())
	return s.Store.SaveTransactions(ctx, txs)
}

func NewRememberConfigStorage(underlying Store) *rememberConfigStorage {
	return &rememberConfigStorage{
		Store: underlying,
	}
}

type RememberConfigStorageDriver struct {
	Driver
}

func (f *RememberConfigStorageDriver) NewStore(ctx context.Context, name string) (Store, bool, error) {
	store, created, err := f.Driver.NewStore(ctx, name)
	if err != nil {
		return nil, false, err
	}
	return NewRememberConfigStorage(store), created, nil
}

func NewRememberConfigStorageDriver(underlying Driver) *RememberConfigStorageDriver {
	return &RememberConfigStorageDriver{
		Driver: underlying,
	}
}
