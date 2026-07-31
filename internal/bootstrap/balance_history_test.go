package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	appbalancehistory "github.com/formancehq/ledger/v3/internal/application/balancehistory"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestBalanceHistoryModuleLifecycle(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	logger := logging.Testing()
	meterProvider := sdkmetric.NewMeterProvider()
	primary, err := dal.NewStore(
		filepath.Join(root, "runtime"),
		logger,
		meterProvider.Meter("test.runtime"),
		dal.DefaultConfig(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, primary.Close())
		require.NoError(t, meterProvider.Shutdown(context.Background()))
	})

	var (
		historyStore *balancehistorystore.Store
		runtime      *balanceHistoryRuntime
		volumeViews  ctrl.VolumeViewProvider
		source       appbalancehistory.Source
	)
	app := fx.New(
		fx.Supply(Config{
			ClusterID: "balance-history-test",
			DataDir:   root,
			BalanceHistoryConfig: BalanceHistoryConfig{
				Enabled: true,
			},
		}),
		fx.Supply(primary),
		fx.Provide(
			func() logging.Logger { return logger },
			func() metric.MeterProvider { return meterProvider },
		),
		balanceHistoryModule(),
		fx.Invoke(registerBalanceHistoryCloseLifecycle),
		fx.Invoke(registerBalanceHistoryQuiesceLifecycle),
		fx.Populate(&historyStore, &runtime, &volumeViews, &source),
	)
	require.NoError(t, app.Err())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))
	require.Equal(t, filepath.Join(root, balanceHistoryDir), historyStore.Path())
	require.NotNil(t, runtime.maintenance, "enabled local-only history must run maintenance")
	require.IsType(t, &appbalancehistory.HotSource{}, source)

	_, err = volumeViews.Open(ctx, "ledger", 1, ctrl.PointInTimeSelector{
		At:   1,
		Axis: balancehistorystore.AxisEffective,
	}, 0)
	var sourceMissing *balancehistorystore.ErrSourceMissing
	var building *balancehistorystore.ErrBuilding
	require.Error(t, err)
	require.True(t, errors.As(err, &sourceMissing) || errors.As(err, &building))

	require.NoError(t, app.Stop(ctx))
}

func TestBalanceHistoryModuleUsesHotColdSource(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	logger := logging.Testing()
	meterProvider := sdkmetric.NewMeterProvider()
	primary, err := dal.NewStore(
		filepath.Join(root, "runtime"),
		logger,
		meterProvider.Meter("test.runtime"),
		dal.DefaultConfig(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, primary.Close())
		require.NoError(t, meterProvider.Shutdown(context.Background()))
	})

	const bucketID = "history-archives"
	coldStorage := coldstorage.NewFilesystemStorage(filepath.Join(root, "archives"))
	coldReader := coldstorage.NewColdReader(
		coldStorage,
		bucketID,
		filepath.Join(root, "cold-cache"),
		2,
		0,
		logger,
	)

	var source appbalancehistory.Source
	app := fx.New(
		fx.Supply(Config{
			ClusterID: "balance-history-test",
			DataDir:   root,
			BalanceHistoryConfig: BalanceHistoryConfig{
				Enabled: true,
			},
			ColdStorageConfig: coldstorage.Config{
				BucketID: bucketID,
			},
		}),
		fx.Supply(primary, coldReader),
		fx.Provide(
			func() logging.Logger { return logger },
			func() metric.MeterProvider { return meterProvider },
			func() coldstorage.ColdStorage { return coldStorage },
		),
		balanceHistoryModule(),
		fx.Invoke(registerBalanceHistoryCloseLifecycle),
		fx.Invoke(registerBalanceHistoryQuiesceLifecycle),
		// The server adds ColdStorageModule after Module. Mirror that ordering so
		// this hook exercises its own idempotent quiesce before closing the reader.
		fx.Invoke(registerColdReaderLifecycle),
		fx.Populate(&source),
	)
	require.NoError(t, app.Err())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))
	require.IsType(t, &appbalancehistory.HotColdSource{}, source)
	require.NoError(t, app.Stop(ctx))
}

func TestBalanceHistoryLifecycleQuiescesBeforeAPIDrainAndClosesAfterView(t *testing.T) {
	t.Parallel()

	const clusterID = "balance-history-lifecycle-order"
	root := t.TempDir()
	logger := logging.Testing()
	meterProvider := sdkmetric.NewMeterProvider()
	primary, err := dal.NewStore(
		filepath.Join(root, "runtime"),
		logger,
		meterProvider.Meter("test.runtime"),
		dal.DefaultConfig(),
	)
	require.NoError(t, err)
	lastSequence := seedBalanceHistoryColdSource(t, primary, clusterID, 1)

	var (
		primaryCloseOnce sync.Once
		primaryCloseErr  error
		eventsMu         sync.Mutex
		events           []string
		heldView         *ctrl.HistoricalVolumeView
		provider         ctrl.VolumeViewProvider
		builder          *appbalancehistory.Builder
	)
	closePrimary := func() error {
		primaryCloseOnce.Do(func() {
			primaryCloseErr = primary.Close()
		})

		return primaryCloseErr
	}
	appendEvent := func(event string) {
		eventsMu.Lock()
		events = append(events, event)
		eventsMu.Unlock()
	}
	t.Cleanup(func() {
		require.NoError(t, closePrimary())
		require.NoError(t, meterProvider.Shutdown(context.Background()))
	})

	historyConfig := DefaultBalanceHistoryConfig()
	historyConfig.Enabled = true
	historyConfig.BuilderBatchSize = 1
	historyConfig.MaintenanceInterval = time.Hour
	historyConfig.VerifierInterval = time.Hour
	app := fx.New(
		fx.NopLogger,
		fx.Supply(Config{
			ClusterID:            clusterID,
			DataDir:              root,
			BalanceHistoryConfig: historyConfig,
		}),
		fx.Supply(primary),
		fx.Provide(
			func() logging.Logger { return logger },
			func() metric.MeterProvider { return meterProvider },
		),
		balanceHistoryModule(),
		// Registration order is the production contract: primary close,
		// history close, API drain, then the late history quiesce hook.
		fx.Invoke(func(lc fx.Lifecycle) {
			lc.Append(fx.Hook{OnStop: func(context.Context) error {
				appendEvent("primary-close")

				return closePrimary()
			}})
		}),
		fx.Invoke(func(lc fx.Lifecycle, runtime *balanceHistoryRuntime) {
			lc.Append(fx.Hook{OnStop: func(context.Context) error {
				runtime.closeMu.Lock()
				closed := runtime.closed
				runtime.closeMu.Unlock()
				if !closed {
					return errors.New("history close hook did not run before its observer")
				}
				appendEvent("history-close")

				return nil
			}})
		}),
		fx.Invoke(registerBalanceHistoryCloseLifecycle),
		fx.Invoke(func(lc fx.Lifecycle, lifecycleBuilder *appbalancehistory.Builder) {
			lc.Append(fx.Hook{OnStop: func(ctx context.Context) error {
				if lifecycleBuilder.Ready() {
					return errors.New("history read gate remained open during API drain")
				}
				if heldView == nil {
					return errors.New("API drain has no pinned history view")
				}
				if _, aggregateErr := heldView.Aggregate(ctx, []string{"cash"}, query.AggregateOptions{}); aggregateErr != nil {
					return fmt.Errorf("using pinned history view during API drain: %w", aggregateErr)
				}
				if closeErr := heldView.Close(); closeErr != nil {
					return fmt.Errorf("closing pinned history view during API drain: %w", closeErr)
				}
				appendEvent("api-drain")

				return nil
			}})
		}),
		fx.Invoke(registerBalanceHistoryQuiesceLifecycle),
		fx.Populate(&provider, &builder),
	)
	require.NoError(t, app.Err())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))
	require.Eventually(t, builder.Ready, 5*time.Second, time.Millisecond)
	heldView, err = provider.Open(
		ctx,
		"default",
		7,
		ctrl.PointInTimeSelector{At: 1_000, Axis: balancehistorystore.AxisEffective},
		lastSequence,
	)
	require.NoError(t, err)

	require.NoError(t, app.Stop(ctx))
	require.False(t, builder.Ready())
	eventsMu.Lock()
	require.Equal(t, []string{"api-drain", "history-close", "primary-close"}, events)
	eventsMu.Unlock()
}

func TestBalanceHistoryRuntimeCloseRetriesFailedQuiesceBeforeClosingStore(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	logger := logging.Testing()
	meterProvider := sdkmetric.NewMeterProvider()
	primary, err := dal.NewStore(
		filepath.Join(root, "runtime"),
		logger,
		meterProvider.Meter("test.runtime"),
		dal.DefaultConfig(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, primary.Close())
		require.NoError(t, meterProvider.Shutdown(context.Background()))
	})

	var (
		runtime      *balanceHistoryRuntime
		historyStore *balancehistorystore.Store
	)
	app := fx.New(
		fx.NopLogger,
		fx.Supply(Config{
			ClusterID: "balance-history-retry-close",
			DataDir:   root,
			BalanceHistoryConfig: BalanceHistoryConfig{
				Enabled: true,
			},
		}),
		fx.Supply(primary),
		fx.Provide(
			func() logging.Logger { return logger },
			func() metric.MeterProvider { return meterProvider },
		),
		balanceHistoryModule(),
		fx.Populate(&runtime, &historyStore),
	)
	require.NoError(t, app.Err())

	realStopBuilder := runtime.stopBuilder
	t.Cleanup(func() {
		runtime.stopBuilder = realStopBuilder
		_ = runtime.close()
	})
	barrierFailure := errors.New("injected final WAL barrier failure")
	stopCalls := 0
	runtime.stopBuilder = func() error {
		stopCalls++
		if stopCalls == 1 {
			return barrierFailure
		}

		return nil
	}

	require.ErrorIs(t, runtime.close(), barrierFailure)
	runtime.closeMu.Lock()
	require.False(t, runtime.closed)
	runtime.closeMu.Unlock()
	_, err = historyStore.Manifest()
	require.NoError(t, err, "failed quiesce must leave the history store retryable")

	require.NoError(t, runtime.close())
	require.Equal(t, 2, stopCalls)
	runtime.closeMu.Lock()
	require.True(t, runtime.closed)
	runtime.closeMu.Unlock()
}
