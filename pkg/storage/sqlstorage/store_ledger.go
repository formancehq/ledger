package sqlstorage

import (
	"context"
	"database/sql"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/pkg/errors"
)

const (
	SQLCustomFuncMetaCompare = "meta_compare"
)

type API struct {
	schema   Schema
	executor executor
}

func (s *API) Schema() Schema {
	return s.schema
}

func (s *API) error(err error) error {
	if err == nil {
		return nil
	}
	return errorFromFlavor(Flavor(s.schema.Flavor()), err)
}

func NewAPI(schema Schema, executor executor) *API {
	return &API{
		schema:   schema,
		executor: executor,
	}
}

func (s *API) Name() string {
	return s.schema.Name()
}

var _ ledger.API = &API{}

type Store struct {
	*API
	schema   Schema
	onClose  func(ctx context.Context) error
	onDelete func(ctx context.Context) error
}

func (s *Store) Delete(ctx context.Context) error {
	if err := s.schema.Delete(ctx); err != nil {
		return err
	}
	return errors.Wrap(s.onDelete(ctx), "deleting ledger store")
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

func (s *Store) withSqlTx(ctx context.Context, callback func(tx *sql.Tx) error) error {
	tx, err := s.schema.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return s.error(err)
	}
	defer func() {
		_ = tx.Rollback()
	}()
	err = callback(tx)
	if err != nil {
		return s.error(err)
	}
	return s.error(tx.Commit())
}

func (s *Store) WithTX(ctx context.Context, callback func(api ledger.API) error) error {
	return s.withSqlTx(ctx, func(tx *sql.Tx) error {
		return callback(NewAPI(s.schema, tx))
	})
}

func NewStore(schema Schema, onClose, onDelete func(ctx context.Context) error) *Store {
	return &Store{
		API:      NewAPI(schema, schema),
		schema:   schema,
		onClose:  onClose,
		onDelete: onDelete,
	}
}

var _ ledger.Store = &Store{}
