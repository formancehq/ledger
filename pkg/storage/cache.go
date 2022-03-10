package storage

import (
	"context"

	"github.com/numary/ledger/pkg/core"
)

type cachedStateStorage struct {
	Store
	lastLog *core.Log
}

func (s *cachedStateStorage) AppendLog(ctx context.Context, log ...core.Log) (map[int]error, error) {
	ret, err := s.Store.AppendLog(ctx, log...)
	if err != nil {
		return ret, err
	}
	if len(log) > 0 && len(ret) == 0 {
		s.lastLog = &log[len(log)-1]
	}
	return ret, nil
}

func (s *cachedStateStorage) LastLog(ctx context.Context) (*core.Log, error) {
	if s.lastLog != nil {
		return s.lastLog, nil
	}
	lastLog, err := s.Store.LastLog(ctx)
	if err != nil {
		return nil, err
	}
	s.lastLog = lastLog
	return lastLog, nil
}

func NewCachedStateStorage(underlying Store) *cachedStateStorage {
	return &cachedStateStorage{
		Store: underlying,
	}
}

type CachedStorageFactory struct {
	underlying Factory
}

func (f *CachedStorageFactory) GetStore(name string) (Store, error) {
	store, err := f.underlying.GetStore(name)
	if err != nil {
		return nil, err
	}
	return NewCachedStateStorage(store), nil
}

func (f *CachedStorageFactory) Close(ctx context.Context) error {
	return f.underlying.Close(ctx)
}

func NewCachedStorageFactory(underlying Factory) *CachedStorageFactory {
	return &CachedStorageFactory{
		underlying: underlying,
	}
}

var _ Factory = &CachedStorageFactory{}
