package sqlstorage

import (
	"context"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/storage"
)

const (
	SQLCustomFuncMetaCompare = "meta_compare"
)

type LedgerStore struct {
	schema  Schema
	onClose func(ctx context.Context) error
}

func (s *LedgerStore) Schema() Schema {
	return s.schema
}

func (s *LedgerStore) error(err error) error {
	if err == nil {
		return nil
	}
	return errorFromFlavor(Flavor(s.schema.Flavor()), err)
}

func NewStore(schema Schema, onClose func(ctx context.Context) error) (*LedgerStore, error) {
	return &LedgerStore{
		schema:  schema,
		onClose: onClose,
	}, nil
}

func (s *LedgerStore) Name() string {
	return s.schema.Name()
}

func (s *LedgerStore) Migrate(ctx context.Context) (bool, error) {
	sharedlogging.GetLogger(ctx).Debug("Initialize store")

	migrations, err := CollectMigrationFiles(MigrationsFS)
	if err != nil {
		return false, err
	}

	return Migrate(ctx, s.schema, migrations...)
}

func (s *LedgerStore) Close(ctx context.Context) error {
	return s.onClose(ctx)
}

var _ storage.LedgerStore = &LedgerStore{}
