package ledgerstore

import (
	"context"

	"github.com/formancehq/go-libs/migrations"

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
