package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	"go.opentelemetry.io/otel/metric"
	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	appbalancehistory "github.com/formancehq/ledger/v3/internal/application/balancehistory"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistoryarchive"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const (
	balanceHistoryDir              = "balance-history"
	balanceHistoryTierMinimumLevel = 1
)

// balanceHistoryModule wires the local, rebuildable monetary-history
// projection. It deliberately stays outside the FSM and the global health
// checker: while a replica is building or lagging, live traffic remains
// available and PIT reads fail closed through the provider.
func balanceHistoryModule() fx.Option {
	return fx.Options(
		fx.Provide(
			func(cfg Config, logger logging.Logger) (*balancehistorystore.Store, error) {
				dir := balanceHistoryStoreDir(cfg)
				logger.WithFields(map[string]any{
					"path": dir,
				}).Infof("Opening balance history peer store")

				return balancehistorystore.New(dir, logger, balancehistorystore.DefaultConfig())
			},
			newBalanceHistorySource,
			func(
				source appbalancehistory.Source,
				store *balancehistorystore.Store,
				certifier *appbalancehistory.HistoryVerifier,
				cfg Config,
				logger logging.Logger,
				meterProvider metric.MeterProvider,
			) *appbalancehistory.Builder {
				historyConfig := cfg.BalanceHistoryConfig.Effective()

				return appbalancehistory.NewBuilder(
					source,
					store,
					certifier,
					// Per-log wakeups can create immutable runs faster than bounded
					// maintenance can merge them. The 200 ms ticker caps publication
					// at 5 runs/s (each may contain up to BuilderBatchSize audits),
					// while two threshold-four merges per second can retire 6 runs/s.
					// This also avoids adding a fifth notifier to the FSM hot path.
					nil,
					cfg.ClusterID,
					logger,
					meterProvider.Meter("balancehistory.builder"),
					historyConfig.BuilderBatchSize,
					historyConfig.RunCompactionThreshold,
					historyConfig.BackfillYield,
					historyConfig.DurabilityInterval,
				)
			},
			func(
				source appbalancehistory.Source,
				store *balancehistorystore.Store,
				cfg Config,
				logger logging.Logger,
				meterProvider metric.MeterProvider,
			) (*appbalancehistory.HistoryVerifier, error) {
				historyConfig := cfg.BalanceHistoryConfig.Effective()

				return appbalancehistory.NewHistoryVerifier(
					source,
					store,
					cfg.ClusterID,
					logger,
					meterProvider.Meter("balancehistory.verifier"),
					appbalancehistory.VerifierConfig{
						Interval:    historyConfig.VerifierInterval,
						BatchSize:   historyConfig.BuilderBatchSize,
						ReplayEvery: historyConfig.VerifierReplayEvery,
						ReplayYield: historyConfig.BackfillYield,
						ScratchParent: filepath.Join(
							balanceHistoryStoreDir(cfg),
							"verifier-scratch",
						),
					},
				)
			},
			newBalanceHistoryRuntime,
			newBalanceHistoryVolumeViewProvider,
		),
	)
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

	Config      Config
	Store       *dal.Store
	ColdReader  *coldstorage.ColdReader `optional:"true"`
	ColdStorage coldstorage.ColdStorage `optional:"true"`
}

func newBalanceHistorySource(params balanceHistorySourceParams) appbalancehistory.Source {
	if params.ColdReader == nil || params.ColdStorage == nil {
		return appbalancehistory.NewHotSource(params.Store)
	}

	return appbalancehistory.NewHotColdSource(
		params.Store,
		params.ColdReader,
		params.ColdStorage,
		balanceHistoryColdBucketID(params.Config),
	)
}

type balanceHistoryRuntime struct {
	store                *balancehistorystore.Store
	archive              *balancehistoryarchive.Store
	builder              *appbalancehistory.Builder
	stopBuilder          func() error
	verifier             *appbalancehistory.HistoryVerifier
	maintenance          *balanceHistoryMaintenanceWorker
	storeRegistration    metric.Registration
	remoteGCRegistration metric.Registration
	enabled              bool

	quiesceMu sync.Mutex
	quiesced  bool

	closeMu  sync.Mutex
	closed   bool
	closeErr error
}

type balanceHistoryRuntimeParams struct {
	fx.In

	Store         *balancehistorystore.Store
	Builder       *appbalancehistory.Builder
	Verifier      *appbalancehistory.HistoryVerifier
	MeterProvider metric.MeterProvider
	Logger        logging.Logger
	Config        Config
	ColdStorage   coldstorage.ColdStorage `optional:"true"`
}

func newBalanceHistoryRuntime(params balanceHistoryRuntimeParams) (*balanceHistoryRuntime, error) {
	historyConfig := params.Config.BalanceHistoryConfig.Effective()
	cleanupStore := true
	defer func() {
		if cleanupStore {
			_ = params.Store.Close()
		}
	}()

	var (
		archive   *balancehistoryarchive.Store
		collector *balancehistorystore.RemoteCollector
		tier      balanceHistoryTierFunc
		collect   balanceHistoryCollectFunc
	)
	if historyConfig.ColdTierEnabled {
		if params.ColdStorage == nil {
			return nil, errors.New("balance history cold tier is enabled but cold storage is unavailable")
		}

		var err error
		archive, err = balancehistoryarchive.New(
			params.ColdStorage,
			balancehistoryarchive.Config{
				BaseBucketID:  balanceHistoryColdBucketID(params.Config),
				OwnerID:       fmt.Sprintf("node-%d", params.Config.RaftConfig.NodeID),
				CacheDir:      filepath.Join(balanceHistoryStoreDir(params.Config), "archive-cache"),
				CacheMaxBytes: historyConfig.ArchiveCacheMaxBytes,
			},
			params.MeterProvider.Meter("balancehistory.archive"),
		)
		if err != nil {
			return nil, fmt.Errorf("opening balance history cold archive: %w", err)
		}
		defer func() {
			if cleanupStore {
				_ = archive.Close()
			}
		}()

		if err := params.Store.ConfigureTiering(balancehistorystore.TieringConfig{
			Archive:         archive,
			MinimumLevel:    balanceHistoryTierMinimumLevel,
			RetainLocalRuns: historyConfig.RetainLocalRuns,
			MaxSegmentBytes: historyConfig.MaxSegmentBytes,
			MaxRunsPerPass:  historyConfig.MaxRunsPerTierPass,
		}); err != nil {
			return nil, fmt.Errorf("configuring balance history cold tier: %w", err)
		}

		collector, err = balancehistorystore.NewRemoteCollector(
			params.Store,
			balancehistorystore.RemoteCollectorConfig{
				GracePeriod: historyConfig.RemoteGCGracePeriod,
			},
		)
		if err != nil {
			return nil, fmt.Errorf("creating balance history remote collector: %w", err)
		}
		tier = params.Store.Tier
		collect = collector.Collect
	} else if err := params.Store.ConfigureTiering(balancehistorystore.TieringConfig{}); err != nil {
		return nil, fmt.Errorf("validating local-only balance history store: %w", err)
	}

	var maintenance *balanceHistoryMaintenanceWorker
	if historyConfig.Enabled {
		maintenance = newBalanceHistoryMaintenanceWorker(
			params.Logger,
			historyConfig,
			params.Store.CompactContext,
			tier,
			collect,
		)
	}

	storeRegistration, err := params.Store.RegisterMetrics(params.MeterProvider.Meter("balancehistory.store"))
	if err != nil {
		return nil, fmt.Errorf("registering balance history store metrics: %w", err)
	}
	cleanupStoreRegistration := true
	defer func() {
		if cleanupStoreRegistration {
			_ = storeRegistration.Unregister()
		}
	}()

	var remoteGCRegistration metric.Registration
	if collector != nil {
		remoteGCRegistration, err = collector.RegisterMetrics(params.MeterProvider.Meter("balancehistory.remote_gc"))
		if err != nil {
			return nil, fmt.Errorf("registering balance history remote-GC metrics: %w", err)
		}
	}

	cleanupStore = false
	cleanupStoreRegistration = false

	return &balanceHistoryRuntime{
		store:                params.Store,
		archive:              archive,
		builder:              params.Builder,
		stopBuilder:          params.Builder.Stop,
		verifier:             params.Verifier,
		maintenance:          maintenance,
		storeRegistration:    storeRegistration,
		remoteGCRegistration: remoteGCRegistration,
		enabled:              historyConfig.Enabled,
	}, nil
}

func (r *balanceHistoryRuntime) start() {
	if !r.enabled {
		return
	}

	r.builder.Start()
	r.verifier.Start()
	if r.maintenance != nil {
		r.maintenance.Start()
	}
}

// quiesce closes the process-local read gate and drains every history worker
// without closing resources. The separate close phase runs only after API
// servers have gracefully drained and released their pinned Views.
func (r *balanceHistoryRuntime) quiesce() error {
	r.quiesceMu.Lock()
	defer r.quiesceMu.Unlock()

	if r.quiesced {
		return nil
	}

	if r.enabled {
		// Maintenance may be hydrating cold runs, so cancel it before draining
		// the verifier and builder. Builder.Stop clears Ready before waiting and
		// leaves the final WAL barrier retryable on failure.
		if r.maintenance != nil {
			r.maintenance.Stop()
		}
		r.verifier.Stop()
		if r.stopBuilder == nil {
			return errors.New("invariant: enabled balance history runtime has no builder stop barrier")
		}
		if err := r.stopBuilder(); err != nil {
			return err
		}
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
	// A failed final builder durability barrier must leave both the archive and
	// local store open so a later lifecycle hook can retry it safely.
	if err := r.quiesce(); err != nil {
		return err
	}

	var remoteGCMetricsErr error
	if r.remoteGCRegistration != nil {
		remoteGCMetricsErr = r.remoteGCRegistration.Unregister()
	}
	var archiveErr error
	if r.archive != nil {
		archiveErr = r.archive.Close()
	}
	r.closeErr = errors.Join(
		remoteGCMetricsErr,
		r.storeRegistration.Unregister(),
		archiveErr,
		r.store.Close(),
	)
	r.closed = true

	return r.closeErr
}

func registerBalanceHistoryCloseLifecycle(
	lc fx.Lifecycle,
	runtime *balanceHistoryRuntime,
) {
	lc.Append(fx.Hook{
		OnStop: func(_ context.Context) error {
			return runtime.close()
		},
	})
}

func registerBalanceHistoryQuiesceLifecycle(
	lc fx.Lifecycle,
	runtime *balanceHistoryRuntime,
) {
	lc.Append(fx.Hook{
		OnStart: func(_ context.Context) error {
			runtime.start()

			return nil
		},
		OnStop: func(_ context.Context) error {
			return runtime.quiesce()
		},
	})
}

type coldReaderLifecycleParams struct {
	fx.In

	Lifecycle      fx.Lifecycle
	ColdReader     *coldstorage.ColdReader
	BalanceHistory *balanceHistoryRuntime `optional:"true"`
}

func registerColdReaderLifecycle(params coldReaderLifecycleParams) {
	params.Lifecycle.Append(fx.Hook{
		OnStop: func(_ context.Context) error {
			var historyErr error
			if params.BalanceHistory != nil {
				historyErr = params.BalanceHistory.quiesce()
			}

			return errors.Join(historyErr, params.ColdReader.Close())
		},
	})
}
