package system

import (
	"context"
	"database/sql"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/pagination"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
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

	return NewDefaultStoreAdapter(store), l, nil
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

type DefaultStoreAdapter struct {
	*ledgerstore.Store
}

func (d *DefaultStoreAdapter) IsUpToDate(ctx context.Context) (bool, error) {
	return d.HasMinimalVersion(ctx)
}

func (d *DefaultStoreAdapter) BeginTX(ctx context.Context, opts *sql.TxOptions) (ledgercontroller.Store, error) {
	store, err := d.Store.BeginTX(ctx, opts)
	if err != nil {
		return nil, err
	}

	return &DefaultStoreAdapter{
		Store: store,
	}, nil
}

func (d *DefaultStoreAdapter) Commit() error {
	return d.Store.Commit()
}

func (d *DefaultStoreAdapter) Rollback() error {
	return d.Store.Rollback()
}

func (d *DefaultStoreAdapter) AggregatedBalances() pagination.Resource[ledger.AggregatedVolumes, ledgerstore.GetAggregatedVolumesOptions] {
	return d.AggregatedVolumes()
}

func NewDefaultStoreAdapter(store *ledgerstore.Store) *DefaultStoreAdapter {
	return &DefaultStoreAdapter{
		Store: store,
	}
}
