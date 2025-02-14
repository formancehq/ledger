package system

import (
	"context"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

type DefaultStorageDriverAdapter struct {
	*driver.Driver
	store Store
}

func (d *DefaultStorageDriverAdapter) OpenLedger(ctx context.Context, name string) (ledgercontroller.Store, *ledger.Ledger, error) {
	store, l, err := d.Driver.OpenLedger(ctx, name)
	if err != nil {
		return nil, nil, err
	}

	return ledgerstore.NewDefaultStoreAdapter(store), l, nil
}

func (d *DefaultStorageDriverAdapter) CreateLedger(ctx context.Context, l *ledger.Ledger) error {
	_, err := d.Driver.CreateLedger(ctx, l)
	return err
}

func (d *DefaultStorageDriverAdapter) GetSystemStore() Store {
	return d.store
}

func NewControllerStorageDriverAdapter(d *driver.Driver, systemStore Store) *DefaultStorageDriverAdapter {
	return &DefaultStorageDriverAdapter{
		Driver: d,
		store:  systemStore,
	}
}

var _ Driver = (*DefaultStorageDriverAdapter)(nil)
