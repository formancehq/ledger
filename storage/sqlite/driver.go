package sqlite

import (
	"context"
	"github.com/numary/ledger/storage"
)

type Driver struct {
	storageDir string
	dbName     string
}

func (d *Driver) Initialize(ctx context.Context) error {
	return nil
}

func (d *Driver) NewStore(name string) (storage.Store, error) {
	return NewStore(d.storageDir, d.dbName, name)
}

func (d *Driver) Close(ctx context.Context) error {
	return nil
}

func NewDriver(storageDir, dbName string) *Driver {
	return &Driver{
		storageDir: storageDir,
		dbName:     dbName,
	}
}
