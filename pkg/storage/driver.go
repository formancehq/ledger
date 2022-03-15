package storage

import (
	"context"
)

type Driver interface {
	Initialize(ctx context.Context) error
	NewStore(name string) (Store, error)
	Close(ctx context.Context) error
	Name() string
	Check(ctx context.Context) error
}
