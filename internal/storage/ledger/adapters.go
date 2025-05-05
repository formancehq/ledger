package ledger

import (
	"context"
	"database/sql"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/common"
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

func (d *DefaultStoreAdapter) AggregatedBalances() common.Resource[ledger.AggregatedVolumes, ledgercontroller.GetAggregatedVolumesOptions] {
	return d.AggregatedVolumes()
}

func (d *DefaultStoreAdapter) LockLedger(ctx context.Context) (ledgercontroller.Store, bun.IDB, func() error, error) {
	lockLedger, b, f, err := d.Store.LockLedger(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	return &DefaultStoreAdapter{
		Store: lockLedger,
	}, b, f, err
}

func NewDefaultStoreAdapter(store *Store) *DefaultStoreAdapter {
	return &DefaultStoreAdapter{
		Store: store,
	}
}

var _ ledgercontroller.Store = (*DefaultStoreAdapter)(nil)
