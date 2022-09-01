package sqlstorage

import (
	"context"

	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/pkg/errors"
)

const (
	SQLCustomFuncMetaCompare = "meta_compare"
)

type Store struct {
	executorProvider func(ctx context.Context) (executor, error)
	schema           Schema
	onClose          func(ctx context.Context) error
	onDelete         func(ctx context.Context) error
}

func (s *Store) error(err error) error {
	if err == nil {
		return nil
	}
	return errorFromFlavor(Flavor(s.schema.Flavor()), err)
}

func (s *Store) Schema() Schema {
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

//func (s *Store) withSqlTx(ctx context.Context, callback func(tx *sql.Tx) error) error {
//	tx, err := s.schema.BeginTx(ctx, &sql.TxOptions{})
//	if err != nil {
//		return s.error(err)
//	}
//	defer func() {
//		_ = tx.Rollback()
//	}()
//	err = callback(tx)
//	if err != nil {
//		return s.error(err)
//	}
//	return s.error(tx.Commit())
//}
//
//func (s *Store) WithTX(ctx context.Context, callback func(api ledger.API) error) error {
//	return s.withSqlTx(ctx, func(tx *sql.Tx) error {
//		return callback(NewAPI(s.schema, tx))
//	})
//}

func NewStore(schema Schema, executorProvider func(ctx context.Context) (executor, error),
	onClose, onDelete func(ctx context.Context) error) *Store {
	return &Store{
		executorProvider: executorProvider,
		schema:           schema,
		onClose:          onClose,
		onDelete:         onDelete,
	}
}

var _ ledger.Store = &Store{}
