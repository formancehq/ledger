package system

import (
	"context"
	"database/sql"

	"github.com/uptrace/bun"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/common"
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

func (d *DefaultStoreAdapter) BeginTX(ctx context.Context, opts *sql.TxOptions) (ledgercontroller.Store, *bun.Tx, error) {
	store, tx, err := d.Store.BeginTX(ctx, opts)
	if err != nil {
		return nil, nil, err
	}

	return &DefaultStoreAdapter{
		Store: store,
	}, tx, nil
}

func (d *DefaultStoreAdapter) Commit(ctx context.Context) error {
	return d.Store.Commit(ctx)
}

func (d *DefaultStoreAdapter) Rollback(ctx context.Context) error {
	return d.Store.Rollback(ctx)
}

func (d *DefaultStoreAdapter) AggregatedBalances() common.Resource[ledger.AggregatedVolumes, ledger.GetAggregatedVolumesOptions] {
	return d.AggregatedVolumes()
}

func (d *DefaultStoreAdapter) LockLedger(ctx context.Context) (ledgercontroller.Store, bun.IDB, func() error, error) {
	lockLedger, conn, release, err := d.Store.LockLedger(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	return &DefaultStoreAdapter{
		Store: lockLedger,
	}, conn, release, nil
}

func NewDefaultStoreAdapter(store *ledgerstore.Store) *DefaultStoreAdapter {
	return &DefaultStoreAdapter{
		Store: store,
	}
}

var _ ledgercontroller.Store = (*DefaultStoreAdapter)(nil)
