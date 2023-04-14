package ledger

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	sqlerrors "github.com/formancehq/ledger/pkg/storage/sqlstorage/errors"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/migrations"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/schema"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/worker"
	"github.com/formancehq/stack/libs/go-libs/logging"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
)

const (
	SQLCustomFuncMetaCompare = "meta_compare"
)

type Store struct {
	schema          schema.Schema
	metricsRegistry *metrics.SQLStorageMetricsRegistry
	storeConfig     StoreConfig
	onClose         func(ctx context.Context) error
	onDelete        func(ctx context.Context) error

	logsBatchWorker *worker.Worker[*core.Log]
	previousLog     *core.Log
	once            sync.Once

	isInitialized bool
}

type StoreConfig struct {
	StoreWorkerConfig worker.WorkerConfig
}

var (
	DefaultStoreConfig = StoreConfig{
		StoreWorkerConfig: worker.DefaultConfig,
	}
)

func (s *Store) Schema() schema.Schema {
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

func (s *Store) Close(ctx context.Context) error {
	return s.onClose(ctx)
}

func (s *Store) IsInitialized() bool {
	return s.isInitialized
}

func (s *Store) RunInTransaction(ctx context.Context, f func(ctx context.Context, store storage.LedgerStore) error) error {
	tx, err := s.schema.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return sqlerrors.PostgresError(err)
	}

	// Create a fake store to use the tx instead of the bun.DB struct
	// TODO(polo): it can be heavy to create and drop store for each transaction
	// since we're creating workers etc...
	newStore, err := NewStore(
		ctx,
		schema.NewSchema(tx.Tx, s.schema.Name()),
		s.onClose,
		s.onDelete,
		s.storeConfig,
	)
	if err != nil {
		return errors.Wrap(err, "creating new store")
	}

	newStore.isInitialized = s.isInitialized

	defer func() {
		_ = tx.Rollback()
	}()

	err = f(ctx, newStore)
	if err != nil {
		return errors.Wrap(err, "running transaction function")
	}

	return sqlerrors.PostgresError(tx.Commit())
}

func (s *Store) instrumentalized(ctx context.Context, name string) func() {
	now := time.Now()
	attrs := []attribute.KeyValue{
		attribute.String("schema", s.schema.Name()),
		attribute.String("op", name),
	}

	return func() {
		latency := time.Since(now)
		s.metricsRegistry.Latencies.Record(ctx, latency.Milliseconds(), attrs...)
	}
}

func NewStore(
	ctx context.Context,
	schema schema.Schema,
	onClose, onDelete func(ctx context.Context) error,
	storeConfig StoreConfig,
) (*Store, error) {
	s := &Store{
		schema:      schema,
		onClose:     onClose,
		onDelete:    onDelete,
		storeConfig: storeConfig,
	}

	logsBatchWorker := worker.NewWorker(s.batchLogs, storeConfig.StoreWorkerConfig)
	s.logsBatchWorker = logsBatchWorker

	metricsRegistry, err := metrics.RegisterSQLStorageMetrics(s.schema.Name())
	if err != nil {
		return nil, errors.Wrap(err, "registering metrics")
	}
	s.metricsRegistry = metricsRegistry

	go logsBatchWorker.Run(logging.ContextWithLogger(
		context.Background(),
		logging.FromContext(ctx),
	))

	return s, nil
}

var _ storage.LedgerStore = &Store{}
