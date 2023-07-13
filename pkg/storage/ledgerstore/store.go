package ledgerstore

import (
	"context"
	"sync"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/migrations"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"
)

const (
	SQLCustomFuncMetaCompare = "meta_compare"
)

type Store struct {
	schema   storage.Schema
	onDelete func(ctx context.Context) error

	once sync.Once

	isInitialized bool
}

func (s *Store) Schema() storage.Schema {
	return s.schema
}

func (s *Store) Name() string {
	return s.schema.Name()
}

func (s *Store) Delete(ctx context.Context) error {
	if err := s.schema.Delete(ctx); err != nil {
		return err
	}
	return errors.Wrap(s.onDelete(ctx), "deleting ledger store")
}

func (s *Store) Migrate(ctx context.Context) (bool, error) {
	ms, err := migrations.CollectMigrationFiles(MigrationsFS)
	if err != nil {
		return false, err
	}

	modified, err := migrations.Migrate(ctx, s.schema, ms...)
	if err == nil {
		s.isInitialized = true
	}

	return modified, err
}

func (s *Store) IsInitialized() bool {
	return s.isInitialized
}

func New(
	schema storage.Schema,
	onDelete func(ctx context.Context) error,
) (*Store, error) {
	return &Store{
		schema:   schema,
		onDelete: onDelete,
	}, nil
}
