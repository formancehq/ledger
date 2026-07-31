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
	"github.com/formancehq/ledger/v3/internal/storage/balancehistorystore"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestBalanceHistoryDisabledStopsWorkersAndFailsClosed(t *testing.T) {
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
		provider ctrl.VolumeViewProvider
		builder  *appbalancehistory.Builder
		verifier *appbalancehistory.HistoryVerifier
	)
	app := fx.New(
		fx.Supply(Config{
			ClusterID: "disabled-balance-history-test",
			DataDir:   root,
			BalanceHistoryConfig: BalanceHistoryConfig{
				Enabled: false,
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
		fx.Populate(&provider, &builder, &verifier),
	)
	require.NoError(t, app.Err())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, app.Start(ctx))

	_, err = provider.Open(ctx, "ledger", 1, ctrl.PointInTimeSelector{
		At:   1,
		Axis: balancehistorystore.AxisEffective,
	}, 0)
	var missing *balancehistorystore.ErrSourceMissing
	require.Error(t, err)
	require.True(t, errors.As(err, &missing))
	require.Contains(t, missing.Detail, "not enabled by configuration")
	require.Zero(t, builder.SourceHeadAuditSequence())
	require.Zero(t, verifier.VerifiedRuns())

	require.NoError(t, app.Stop(ctx))
}
