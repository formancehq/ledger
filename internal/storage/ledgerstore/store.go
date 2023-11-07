package ledgerstore

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/formancehq/ledger/internal/storage"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"
	"github.com/uptrace/bun"
)

type Store struct {
	db       *bun.DB
	onDelete func(ctx context.Context) error

	name string
}

func (store *Store) Name() string {
	return store.name
}

func (store *Store) GetDatabase() *bun.DB {
	return store.db
}

func (store *Store) Delete(ctx context.Context) error {
	_, err := store.db.ExecContext(ctx, "delete schema ? cascade", store.name)
	if err != nil {
		return err
	}
	return errors.Wrap(store.onDelete(ctx), "deleting ledger store")
}

func (store *Store) prepareTransaction(ctx context.Context) (bun.Tx, error) {
	txOptions := &sql.TxOptions{}

	tx, err := store.db.BeginTx(ctx, txOptions)
	if err != nil {
		return tx, err
	}
	if _, err := tx.Exec(fmt.Sprintf(`set search_path = "%s"`, store.Name())); err != nil {
		return tx, err
	}
	return tx, nil
}

func (store *Store) withTransaction(ctx context.Context, callback func(tx bun.Tx) error) error {
	tx, err := store.prepareTransaction(ctx)
	if err != nil {
		return err
	}
	if err := callback(tx); err != nil {
		_ = tx.Rollback()
		return storage.PostgresError(err)
	}
	return tx.Commit()
}

func (store *Store) IsSchemaUpToDate(ctx context.Context) (bool, error) {
	return store.getMigrator().IsUpToDate(ctx, store.db)
}

func New(
	db *bun.DB,
	name string,
	onDelete func(ctx context.Context) error,
) (*Store, error) {
	return &Store{
		db:       db,
		name:     name,
		onDelete: onDelete,
	}, nil
}
