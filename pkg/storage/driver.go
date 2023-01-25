package storage

import (
	"context"
	"errors"
)

var (
	ErrLedgerStoreNotFound = errors.New("ledger store not found")
)

type SystemStore interface {
	GetConfiguration(ctx context.Context, key string) (string, error)
	InsertConfiguration(ctx context.Context, key, value string) error
	ListLedgers(ctx context.Context) ([]string, error)
	DeleteLedger(ctx context.Context, name string) error
}

type LedgerStore interface {
	Delete(ctx context.Context) error
	Initialize(ctx context.Context) (bool, error)
	Close(ctx context.Context) error
}

type LedgerStoreProvider[STORE any] interface {
	GetLedgerStore(ctx context.Context, name string, create bool) (STORE, bool, error)
}
type LedgerStoreProviderFn[STORE any] func(ctx context.Context, name string, create bool) (STORE, bool, error)

func (fn LedgerStoreProviderFn[STORE]) GetLedgerStore(ctx context.Context, name string, create bool) (STORE, bool, error) {
	return fn(ctx, name, create)
}

type Driver[STORE any] interface {
	LedgerStoreProvider[STORE]
	Initialize(ctx context.Context) error
	Close(ctx context.Context) error
	Name() string

	GetSystemStore() SystemStore
}
