package ledgerstore

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	sqlerrors "github.com/formancehq/ledger/pkg/storage/errors"
	"github.com/formancehq/ledger/pkg/storage/migrations"
	"github.com/formancehq/ledger/pkg/storage/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage/schema"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
)

const (
	SQLCustomFuncMetaCompare = "meta_compare"
)

// TODO(gfyrag): useless, we have to throttle the application at higher level
type Config struct {
	MaxPendingSize   int
	MaxWriteChanSize int
}

var (
	DefaultConfig = Config{
		MaxPendingSize:   0,
		MaxWriteChanSize: 1024,
	}
)

type OnLogWrote func([]*AppendedLog)

type Store struct {
	schema          schema.Schema
	metricsRegistry *metrics.SQLStorageMetricsRegistry
	storeConfig     StoreConfig
	onDelete        func(ctx context.Context) error

	previousLog *core.PersistedLog
	once        sync.Once

	isInitialized bool

	writeChannel chan pendingLog
	stopChan     chan chan struct{}
	stopped      chan struct{}
	onLogsWrote  []OnLogWrote
}

type StoreConfig struct {
	StoreWorkerConfig Config
}

var (
	DefaultStoreConfig = StoreConfig{
		StoreWorkerConfig: DefaultConfig,
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

func (s *Store) RunInTransaction(ctx context.Context, f func(ctx context.Context, store *Store) error) error {
	tx, err := s.schema.BeginTx(ctx, &sql.TxOptions{})
	if err != nil {
		return sqlerrors.PostgresError(err)
	}

	// Create a fake store to use the tx instead of the bun.DB struct
	// TODO(polo): it can be heavy to create and drop store for each transaction
	// since we're creating workers etc...
	newStore, err := NewStore(
		schema.NewSchema(tx.Tx, s.schema.Name()),
		func(ctx context.Context) error {
			return nil
		},
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

func (s *Store) OnLogWrote(fn OnLogWrote) {
	s.onLogsWrote = append(s.onLogsWrote, fn)
}

func NewStore(
	schema schema.Schema,
	onDelete func(ctx context.Context) error,
	storeConfig StoreConfig,
) (*Store, error) {
	s := &Store{
		schema:       schema,
		onDelete:     onDelete,
		storeConfig:  storeConfig,
		writeChannel: make(chan pendingLog, storeConfig.StoreWorkerConfig.MaxWriteChanSize),
		stopChan:     make(chan chan struct{}, 1),
		stopped:      make(chan struct{}),
	}

	metricsRegistry, err := metrics.RegisterSQLStorageMetrics(s.schema.Name())
	if err != nil {
		return nil, errors.Wrap(err, "registering metrics")
	}
	s.metricsRegistry = metricsRegistry

	return s, nil
}
