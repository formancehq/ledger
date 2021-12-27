package storage

import (
	"context"
	"github.com/numary/ledger/pkg/config"
	"github.com/numary/ledger/pkg/core"
)

type rememberConfigStorage struct {
	Store
}

func (s *rememberConfigStorage) SaveTransactions(ctx context.Context, txs []core.Transaction) error {
	defer config.Remember(s.Name())
	return s.Store.SaveTransactions(ctx, txs)
}

func NewRememberConfigStorage(underlying Store) *rememberConfigStorage {
	return &rememberConfigStorage{
		Store: underlying,
	}
}

type RememberConfigStorageFactory struct {
	Factory
}

func (f *RememberConfigStorageFactory) GetStore(name string) (Store, error) {
	store, err := f.Factory.GetStore(name)
	if err != nil {
		return nil, err
	}
	return NewRememberConfigStorage(store), nil
}

func NewRememberConfigStorageFactory(underlying Factory) *RememberConfigStorageFactory {
	return &RememberConfigStorageFactory{
		Factory: underlying,
	}
}

var _ Factory = &RememberConfigStorageFactory{}
