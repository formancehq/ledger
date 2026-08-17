package bootstrap

import (
	"context"
	"errors"
	"path/filepath"
	"sync"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	appbalancehistory "github.com/formancehq/ledger/v3/internal/application/balancehistory"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const balanceHistoryDir = "balance-history"

// balanceHistoryModule wires an always-available local projection worker.
// Client commands decide which ledgers are projected; server configuration
// only sizes the worker and its isolated Pebble store.
func balanceHistoryModule() fx.Option {
	return fx.Options(fx.Provide(
		func(cfg Config, logger logging.Logger) (*balancehistorystore.Store, error) {
			dir := balanceHistoryStoreDir(cfg)
			logger.WithFields(map[string]any{"path": dir}).Infof("Opening historical-balance peer store")

			return balancehistorystore.New(dir, logger, balancehistorystore.DefaultConfig())
		},
		newBalanceHistorySource,
		fx.Annotate(func(
			source appbalancehistory.Source,
			store *balancehistorystore.Store,
			notifications *signal.Notifications,
			cfg Config,
			logger logging.Logger,
			meterProvider metric.MeterProvider,
		) *appbalancehistory.Builder {
			historyConfig := cfg.BalanceHistoryConfig.Effective()

			return appbalancehistory.NewBuilder(
				source,
				store,
				notifications,
				cfg.ClusterID,
				logger,
				meterProvider.Meter("balancehistory.builder"),
				historyConfig.BuilderBatchSize,
				historyConfig.SegmentCompactionThreshold,
				historyConfig.BackfillYield,
				historyConfig.DurabilityInterval,
			)
		}, fx.ParamTags(``, ``, `name:"balancehistory"`, ``, ``, ``)),
		newBalanceHistoryRuntime,
		newBalanceHistoryVolumeViewProvider,
	))
}

func balanceHistoryStoreDir(cfg Config) string {
	if cfg.BalanceHistoryConfig.Dir != "" {
		return cfg.BalanceHistoryConfig.Dir
	}

	return filepath.Join(cfg.DataDir, balanceHistoryDir)
}

func balanceHistoryColdBucketID(cfg Config) string {
	if cfg.ColdStorageConfig.BucketID != "" {
		return cfg.ColdStorageConfig.BucketID
	}

	return cfg.ClusterID
}

type balanceHistorySourceParams struct {
	fx.In

	Config     Config
	Store      *dal.Store
	ColdReader *coldstorage.ColdReader `optional:"true"`
}

func newBalanceHistorySource(params balanceHistorySourceParams) appbalancehistory.Source {
	if params.ColdReader == nil {
		return appbalancehistory.NewHotSource(params.Store)
	}

	return appbalancehistory.NewHotColdSource(
		params.Store,
		params.ColdReader,
		balanceHistoryColdBucketID(params.Config),
	)
}

type balanceHistoryRuntime struct {
	store             *balancehistorystore.Store
	builder           *appbalancehistory.Builder
	maintenance       *balanceHistoryMaintenanceWorker
	storeRegistration metric.Registration

	quiesceMu sync.Mutex
	quiesced  bool
	closeMu   sync.Mutex
	closed    bool
	closeErr  error
}

type balanceHistoryRuntimeParams struct {
	fx.In

	Store         *balancehistorystore.Store
	Builder       *appbalancehistory.Builder
	MeterProvider metric.MeterProvider
	Logger        logging.Logger
	Config        Config
}

func newBalanceHistoryRuntime(params balanceHistoryRuntimeParams) (*balanceHistoryRuntime, error) {
	historyConfig := params.Config.BalanceHistoryConfig.Effective()
	registration, err := params.Store.RegisterMetrics(params.MeterProvider.Meter("balancehistory.store"))
	if err != nil {
		_ = params.Store.Close()

		return nil, err
	}

	return &balanceHistoryRuntime{
		store:   params.Store,
		builder: params.Builder,
		maintenance: newBalanceHistoryMaintenanceWorker(
			params.Logger,
			historyConfig,
			params.Store.CompactContext,
			params.Store.Changes,
		),
		storeRegistration: registration,
	}, nil
}

func (r *balanceHistoryRuntime) start() {
	r.builder.Start()
	r.maintenance.Start()
}

func (r *balanceHistoryRuntime) quiesce() error {
	r.quiesceMu.Lock()
	defer r.quiesceMu.Unlock()
	if r.quiesced {
		return nil
	}
	r.maintenance.Stop()
	if r.builder == nil {
		return errors.New("invariant: historical-balance runtime has no builder")
	}
	if err := r.builder.Stop(); err != nil {
		return err
	}
	r.quiesced = true

	return nil
}

func (r *balanceHistoryRuntime) close() error {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.closed {
		return r.closeErr
	}
	if err := r.quiesce(); err != nil {
		return err
	}
	r.closeErr = errors.Join(r.storeRegistration.Unregister(), r.store.Close())
	r.closed = true

	return r.closeErr
}

func registerBalanceHistoryCloseLifecycle(lc fx.Lifecycle, runtime *balanceHistoryRuntime) {
	lc.Append(fx.Hook{OnStop: func(_ context.Context) error { return runtime.close() }})
}

func registerBalanceHistoryQuiesceLifecycle(lc fx.Lifecycle, runtime *balanceHistoryRuntime) {
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			runtime.start()

			return nil
		},
		OnStop: func(_ context.Context) error { return runtime.quiesce() },
	})
}

type coldReaderLifecycleParams struct {
	fx.In

	Lifecycle      fx.Lifecycle
	ColdReader     *coldstorage.ColdReader
	BalanceHistory *balanceHistoryRuntime `optional:"true"`
}

func registerColdReaderLifecycle(params coldReaderLifecycleParams) {
	params.Lifecycle.Append(fx.Hook{OnStop: func(_ context.Context) error {
		var historyErr error
		if params.BalanceHistory != nil {
			historyErr = params.BalanceHistory.quiesce()
		}

		return errors.Join(historyErr, params.ColdReader.Close())
	}})
}

var _ ctrl.VolumeViewProvider = (*balanceHistoryVolumeViewProvider)(nil)
