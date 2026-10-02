package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	ledgermetrics "github.com/formancehq/ledger/v3/internal/infra/monitoring/metrics"
)

// TestDecorateMeterProvider_OnlyRenamesLedgerInstruments reproduces the
// go-libs observefx wiring — a concrete *sdkmetric.MeterProvider, exposed
// as metric.MeterProvider and installed as the global provider used by
// the Go runtime, host, otelhttp and otelgrpc instrumentation — and checks
// that the ledger decorator prefixes instruments created through the
// injected interface only. Semantic-convention instruments, which go
// through the concrete SDK provider, must keep their upstream names.
func TestDecorateMeterProvider_OnlyRenamesLedgerInstruments(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	sdk := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = sdk.Shutdown(context.Background())
	})

	app := fxtest.New(t,
		fx.Supply(Config{MetricsPrefix: ledgermetrics.DefaultPrefix}),
		fx.Supply(sdk),
		fx.Provide(func(mp *sdkmetric.MeterProvider) metric.MeterProvider { return mp }),
		fx.Decorate(decorateMeterProvider),
		fx.Invoke(func(injected metric.MeterProvider, global *sdkmetric.MeterProvider) {
			ledger, err := injected.Meter("raft.node").Int64Counter("raft.fsm.logs_appended")
			require.NoError(t, err)
			ledger.Add(context.Background(), 1)

			semconv, err := global.Meter("go.opentelemetry.io/contrib/instrumentation/runtime").Int64Counter("go.memory.allocated")
			require.NoError(t, err)
			semconv.Add(context.Background(), 1)
		}),
	)
	app.RequireStart().RequireStop()

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
		}
	}

	require.ElementsMatch(t, []string{
		"formance.ledger.raft.fsm.logs_appended",
		"go.memory.allocated",
	}, names)
}

func TestDecorateMeterProvider_NoneDisablesPrefix(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	sdk := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = sdk.Shutdown(context.Background())
	})

	mp, err := decorateMeterProvider(Config{MetricsPrefix: ledgermetrics.NoPrefix}, sdk)
	require.NoError(t, err)
	c, err := mp.Meter("raft.node").Int64Counter("raft.fsm.logs_appended")
	require.NoError(t, err)
	c.Add(context.Background(), 1)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	require.Len(t, rm.ScopeMetrics, 1)
	require.Len(t, rm.ScopeMetrics[0].Metrics, 1)
	require.Equal(t, "raft.fsm.logs_appended", rm.ScopeMetrics[0].Metrics[0].Name)
}
