package sqlstorage

import (
	"context"
	"time"

	"github.com/formancehq/stack/libs/go-libs/logging"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/patrickmn/go-cache"
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
	lastLog          *core.Log
	lastTx           *core.ExpandedTransaction
	cache            *cache.Cache
	multipleInstance bool
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
	logging.FromContext(ctx).Debug("Initialize store")

	migrations, err := CollectMigrationFiles(MigrationsFS)
	if err != nil {
		return false, err
	}

	return Migrate(ctx, s.schema, migrations...)
}

func (s *Store) Close(ctx context.Context) error {
	return s.onClose(ctx)
}

func NewStore(schema Schema, multipleInstance bool, executorProvider func(ctx context.Context) (executor, error),
	onClose, onDelete func(ctx context.Context) error) *Store {

	return &Store{
		executorProvider: executorProvider,
		schema:           schema,
		onClose:          onClose,
		onDelete:         onDelete,
		multipleInstance: multipleInstance,
		cache:            cache.New(5*time.Minute, 10*time.Minute),
	}
}

var _ ledger.Store = &Store{}
