package ledgerstore

import (
	"context"
	"database/sql"

	"github.com/formancehq/stack/libs/go-libs/migrations"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/bun"
)

type Store struct {
	bucket *Bucket

	name string
}

func (store *Store) Name() string {
	return store.name
}

func (store *Store) GetDB() *bun.DB {
	return store.bucket.db
}

func (store *Store) withTransaction(ctx context.Context, callback func(tx bun.Tx) error) error {
	return store.bucket.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		return callback(tx)
	})
}

func (store *Store) IsUpToDate(ctx context.Context) (bool, error) {
	return store.bucket.IsUpToDate(ctx)
}

func (store *Store) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return store.bucket.GetMigrationsInfo(ctx)
}

func New(
	bucket *Bucket,
	name string,
) (*Store, error) {
	return &Store{
		bucket: bucket,
		name:   name,
	}, nil
}
