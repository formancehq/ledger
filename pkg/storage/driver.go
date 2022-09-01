package storage

import (
	"context"
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

type Driver[STORE any] interface {
	Initialize(ctx context.Context) error
	Close(ctx context.Context) error
	Name() string

	GetSystemStore() SystemStore
	GetLedgerStore(ctx context.Context, name string, create bool) (STORE, bool, error)
}
