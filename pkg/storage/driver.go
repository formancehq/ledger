package storage

import (
	"context"
)

type Driver[STORE any] interface {
	Initialize(ctx context.Context) error
	GetStore(ctx context.Context, name string, create bool) (STORE, bool, error)
	Close(ctx context.Context) error
	List(ctx context.Context) ([]string, error)
	DeleteStore(ctx context.Context, name string) error
	Name() string
	GetConfiguration(ctx context.Context, key string) (string, error)
	InsertConfiguration(ctx context.Context, key, value string) error
}
