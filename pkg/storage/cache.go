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

type CachedStorageDriver struct {
	Driver
}

func (f *CachedStorageDriver) GetStore(ctx context.Context, name string, create bool) (Store, bool, error) {
	store, created, err := f.Driver.GetStore(ctx, name, create)
	if err != nil {
		return nil, false, err
	}
	return NewCachedStateStorage(store), created, nil
}

func NewCachedStorageDriver(underlying Driver) *CachedStorageDriver {
	return &CachedStorageDriver{
		Driver: underlying,
	}
}
