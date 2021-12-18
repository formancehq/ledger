package sqlite

import (
	"context"
	"github.com/numary/ledger/storage"
)

type Driver struct{}

func (d *Driver) Initialize(ctx context.Context) error {
	return nil
}

func (d *Driver) NewStore(name string) (storage.Store, error) {
	return NewStore(name)
}

func (d *Driver) Close(ctx context.Context) error {
	return nil
}

func init() {
	storage.RegisterDriver("sqlite", &Driver{})
}
