package bootstrap

import (
	"context"
	"errors"
	"path/filepath"
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
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func balanceHistoryTestDependencies(config Config, primary *dal.Store, logger logging.Logger, meterProvider metric.MeterProvider) fx.Option {
	return fx.Options(
		fx.Supply(config, primary),
		fx.Provide(
			func() logging.Logger { return logger },
			func() metric.MeterProvider { return meterProvider },
			fx.Annotate(signal.NewNotifications, fx.ResultTags(`name:"balancehistory"`)),
		),
	)
}

func TestBalanceHistoryModuleLifecycle(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	logger := logging.Testing()
	meterProvider := sdkmetric.NewMeterProvider()
	primary, err := dal.NewStore(filepath.Join(root, "runtime"), logger, meterProvider.Meter("test.runtime"), dal.DefaultConfig())
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
		balanceHistoryTestDependencies(Config{ClusterID: "balance-history-test", DataDir: root}, primary, logger, meterProvider),
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
	require.NotNil(t, runtime.maintenance)
	require.IsType(t, &appbalancehistory.HotSource{}, source)

	_, err = volumeViews.Open(ctx, "ledger", ctrl.HistoricalBalanceSelector{At: 1, Temporality: balancehistorystore.TemporalityEffective}, 0)
	var sourceMissing *balancehistorystore.ErrSourceMissing
	var building *balancehistorystore.ErrBuilding
	require.True(t, errors.As(err, &sourceMissing) || errors.As(err, &building))
	require.NoError(t, app.Stop(ctx))
}

func TestBalanceHistoryModuleUsesAuditColdSourceOnly(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	logger := logging.Testing()
	meterProvider := sdkmetric.NewMeterProvider()
	primary, err := dal.NewStore(filepath.Join(root, "runtime"), logger, meterProvider.Meter("test.runtime"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, primary.Close())
		require.NoError(t, meterProvider.Shutdown(context.Background()))
	})

	const bucketID = "audit-archives"
	coldStorage := coldstorage.NewFilesystemStorage(filepath.Join(root, "archives"))
	coldReader := coldstorage.NewColdReader(coldStorage, bucketID, filepath.Join(root, "cold-cache"), 2, 0, logger)
	var source appbalancehistory.Source
	app := fx.New(
		balanceHistoryTestDependencies(Config{ClusterID: "balance-history-test", DataDir: root, ColdStorageConfig: coldstorage.Config{BucketID: bucketID}}, primary, logger, meterProvider),
		fx.Supply(coldReader),
		fx.Provide(func() coldstorage.ColdStorage { return coldStorage }),
		balanceHistoryModule(),
		fx.Invoke(registerBalanceHistoryCloseLifecycle),
		fx.Invoke(registerBalanceHistoryQuiesceLifecycle),
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
