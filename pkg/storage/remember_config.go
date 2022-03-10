package storage

import (
	"context"
	"github.com/numary/ledger/pkg/config"
	"github.com/numary/ledger/pkg/core"
)

type rememberConfigStorage struct {
	Store
}

func (s *rememberConfigStorage) AppendLog(ctx context.Context, log ...core.Log) (map[int]error, error) {
	defer config.Remember(ctx, s.Name())
	return s.Store.AppendLog(ctx, log...)
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
