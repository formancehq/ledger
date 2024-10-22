package ledger

import (
	"context"
	"database/sql"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/uptrace/bun"
)

type TX struct {
	*Store
	sqlTX bun.Tx
}

type DefaultStoreAdapter struct {
	*Store
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

func NewDefaultStoreAdapter(store *Store) *DefaultStoreAdapter {
	return &DefaultStoreAdapter{
		Store: store,
	}
}

var _ ledgercontroller.Store = (*DefaultStoreAdapter)(nil)
