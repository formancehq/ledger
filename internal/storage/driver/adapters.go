package driver

import (
	"context"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

type DefaultStorageDriverAdapter struct {
	*Driver
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

func NewControllerStorageDriverAdapter(d *Driver) *DefaultStorageDriverAdapter {
	return &DefaultStorageDriverAdapter{Driver: d}
}

var _ systemcontroller.Store = (*DefaultStorageDriverAdapter)(nil)
