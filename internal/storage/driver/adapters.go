package driver

import (
	"context"
	"fmt"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger/legacy"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

type DefaultStorageDriverAdapter struct {
	*Driver
}

func (d *DefaultStorageDriverAdapter) OpenLedger(ctx context.Context, name string) (ledgercontroller.Store, *ledger.Ledger, error) {
	store, l, err := d.Driver.OpenLedger(ctx, name)
	if err != nil {
		return nil, nil, err
	}

	isUpToDate, err := store.GetBucket().IsUpToDate(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("checking if bucket is up to date: %w", err)
	}

	return ledgerstore.NewDefaultStoreAdapter(isUpToDate, store), l, nil
}

func (d *DefaultStorageDriverAdapter) CreateLedger(ctx context.Context, l *ledger.Ledger) error {
	_, err := d.Driver.CreateLedger(ctx, l)
	return err
}

func NewControllerStorageDriverAdapter(d *Driver) *DefaultStorageDriverAdapter {
	return &DefaultStorageDriverAdapter{Driver: d}
}

var _ systemcontroller.Store = (*DefaultStorageDriverAdapter)(nil)
