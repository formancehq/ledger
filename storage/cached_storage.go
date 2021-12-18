package storage

import (
	"context"
	"github.com/numary/ledger/core"
)

type cachedStateStorage struct {
	Store
	lastTransaction *core.Transaction
	lastMetaId      *int64
}

func (s *cachedStateStorage) LastTransaction(ctx context.Context) (*core.Transaction, error) {
	if s.lastTransaction != nil {
		return s.lastTransaction, nil
	}
	var err error
	s.lastTransaction, err = s.Store.LastTransaction(ctx)
	if err != nil {
		return nil, err
	}
	return s.lastTransaction, nil
}

func (s *cachedStateStorage) LastMetaID(ctx context.Context) (int64, error) {
	if s.lastMetaId != nil {
		return *s.lastMetaId, nil
	}
	lastMetaID, err := s.Store.LastMetaID(ctx)
	if err != nil {
		return 0, err
	}
	*s.lastMetaId = lastMetaID
	return lastMetaID, nil
}

func (s *cachedStateStorage) SaveTransactions(ctx context.Context, txs []core.Transaction) error {
	err := s.Store.SaveTransactions(ctx, txs)
	if err != nil {
		return err
	}
	if len(txs) > 0 {
		s.lastTransaction = &txs[len(txs)-1]
	}
	return nil
}

func (s *cachedStateStorage) SaveMeta(ctx context.Context, id int64, timestamp, targetType, targetID, key, value string) error {
	err := s.Store.SaveMeta(ctx, id, timestamp, targetType, targetID, key, value)
	if err != nil {
		return err
	}
	s.lastMetaId = &id
	return nil
}

func NewCachedStateStorage(underlying Store) *cachedStateStorage {
	return &cachedStateStorage{
		Store: underlying,
	}
}

type CachedStorageFactory struct {
	underlying Factory
}

func (f CachedStorageFactory) GetStore(name string) (Store, error) {
	store, err := f.underlying.GetStore(name)
	if err != nil {
		return nil, err
	}
	return NewCachedStateStorage(store), nil
}

func NewCachedStorageFactory(underlying Factory) *CachedStorageFactory {
	return &CachedStorageFactory{
		underlying: underlying,
	}
}
