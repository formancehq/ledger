package storage

import (
	"context"
)

type Driver interface {
	Initialize(ctx context.Context) error
	NewStore(ctx context.Context, name string) (Store, error)
	Close(ctx context.Context) error
	Name() string
}
