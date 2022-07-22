package sqlstorage

import (
	"context"
	"database/sql"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/storage"
)

const (
	SQLCustomFuncMetaCompare = "meta_compare"
)

var sqlTxKey = struct{}{}

type Store struct {
	schema  Schema
	onClose func(ctx context.Context) error
}

func (s *Store) getExecutorFromContext(ctx context.Context) executor {
	sqlTx := ctx.Value(sqlTxKey)
	if sqlTx == nil {
		return s.schema
	}
	return sqlTx.(*sql.Tx)
}

func (s *Store) Schema() Schema {
	return s.schema
}

func (s *Store) error(err error) error {
	if err == nil {
		return nil
	}
	return errorFromFlavor(Flavor(s.schema.Flavor()), err)
}

func (s *Store) Name() string {
	return s.schema.Name()
}

func (s *Store) Initialize(ctx context.Context) (bool, error) {
	sharedlogging.GetLogger(ctx).Debug("Initialize store")

	migrations, err := CollectMigrationFiles(MigrationsFS)
	if err != nil {
		return false, err
	}

	return Migrate(ctx, s.schema, migrations...)
}

func (s *Store) Close(ctx context.Context) error {
	return s.onClose(ctx)
}

func NewStore(schema Schema, onClose func(ctx context.Context) error) *Store {
	return &Store{
		schema:  schema,
		onClose: onClose,
	}
}

var _ storage.Store = &Store{}
