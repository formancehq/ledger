package ledger

import (
	"context"
	"database/sql"

	systemstore "github.com/formancehq/ledger/internal/storage/system"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/uptrace/bun"
)

type TX struct {
	*Store
	sqlTX bun.Tx
}

func (t *TX) SwitchLedgerState(ctx context.Context, name string, state string) error {
	return systemstore.New(t.sqlTX).UpdateLedgerState(ctx, name, state)
}

type DefaultStoreAdapter struct {
	*Store
}

func (d *DefaultStoreAdapter) WithTX(ctx context.Context, f func(ledgercontroller.TX) (bool, error)) error {
	tx, err := d.GetDB().BeginTx(ctx, &sql.TxOptions{})
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
