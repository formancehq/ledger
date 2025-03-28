package ledger

import (
	"context"
	"database/sql"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/resources"
	"github.com/uptrace/bun"
)

type TX struct {
	*Store
}

type DefaultStoreAdapter struct {
	*Store
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

func (d *DefaultStoreAdapter) Commit() error {
	return d.Store.Commit()
}

func (d *DefaultStoreAdapter) Rollback() error {
	return d.Store.Rollback()
}

func (d *DefaultStoreAdapter) AggregatedBalances() resources.Resource[ledger.AggregatedVolumes, ledgercontroller.GetAggregatedVolumesOptions] {
	return d.AggregatedVolumes()
}

func NewDefaultStoreAdapter(store *Store) *DefaultStoreAdapter {
	return &DefaultStoreAdapter{
		Store: store,
	}
}

var _ ledgercontroller.Store = (*DefaultStoreAdapter)(nil)
