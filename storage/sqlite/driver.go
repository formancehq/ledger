package sqlite

import "github.com/numary/ledger/storage"

type SQLiteSDriver struct{}

func (d *SQLiteSDriver) Initialize() error {
	return nil
}

func (d *SQLiteSDriver) NewStore(name string) (storage.Store, error) {
	return NewStore(name)
}

func init() {
	storage.RegisterDriver("sqlite", &SQLiteSDriver{})
}
