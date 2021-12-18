package sqlite

import "github.com/numary/ledger/storage"

type Driver struct{}

func (d *Driver) Initialize() error {
	return nil
}

func (d *Driver) NewStore(name string) (storage.Store, error) {
	return NewStore(name)
}

func (d *Driver) Close() error {
	return nil
}

func init() {
	storage.RegisterDriver("sqlite", &Driver{})
}
