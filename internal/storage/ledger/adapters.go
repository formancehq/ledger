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

func (d *DefaultStoreAdapter) WithTX(ctx context.Context, opts *sql.TxOptions, f func(ledgercontroller.TX) (bool, error)) error {
	if opts == nil {
		opts = &sql.TxOptions{}
	}

	tx, err := d.GetDB().BeginTx(ctx, opts)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()

	if commit, err := f(&TX{
		Store: d.Store.WithDB(tx),
		sqlTX: tx,
	}); err != nil {
		return err
	} else {
		if commit {
			return tx.Commit()
		}
	}

	return nil
}

func NewDefaultStoreAdapter(store *Store) *DefaultStoreAdapter {
	return &DefaultStoreAdapter{
		Store: store,
	}
}

var _ ledgercontroller.Store = (*DefaultStoreAdapter)(nil)
