package metrics_test

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/metrics"
)

// collectInstrumentNames builds a fresh meter provider backed by a
// manual reader, drives the wrapping factory under test, registers
// instruments and returns the set of names actually exported by the
// SDK.
func collectInstrumentNames(t *testing.T, naming metrics.Naming, register func(otelmetric.MeterProvider)) []string {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})

	factory := metrics.NewFactory(provider, naming)
	register(factory)

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	var names []string
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			names = append(names, m.Name)
		}
	}

	return names
}

func TestFactory_OTelNamingPreservesNames(t *testing.T) {
	t.Parallel()

	names := collectInstrumentNames(t, metrics.NamingOTel, func(mp otelmetric.MeterProvider) {
		c, err := mp.Meter("admission").Int64Counter("admission.preload.total")
		require.NoError(t, err)
		c.Add(context.Background(), 1)

		// Library-style meter names (raft, pebble) are equally
		// preserved in OTel mode — the policy only triggers in prom.
		c2, err := mp.Meter("raft.node").Int64Counter("raft.fsm.logs_appended")
		require.NoError(t, err)
		c2.Add(context.Background(), 1)
	})

	require.ElementsMatch(t, []string{
		"admission.preload.total",
		"raft.fsm.logs_appended",
	}, names)
}

func TestFactory_PromNamingPrefixesEveryInstrument(t *testing.T) {
	t.Parallel()

	// Any instrument created via the factory is prefixed — meters
	// our code owns (admission, wal, …) and meters whose names hint
	// at library code we wrap ourselves (raft, pebble, numscript)
	// alike. The OTel auto-instrumentation that targets the *global*
	// MeterProvider bypasses the factory and is not exercised here.
	names := collectInstrumentNames(t, metrics.NamingProm, func(mp otelmetric.MeterProvider) {
		register := func(meter, instrument string) {
			c, err := mp.Meter(meter).Int64Counter(instrument)
			require.NoError(t, err)
			c.Add(context.Background(), 1)
		}
		register("admission", "admission.preload.total")
		register("wal", "wal.append.save.duration")
		register("raft.node", "raft.fsm.logs_appended")
		register("pebble.runtime_store", "pebble.flush.total")
		register("numscript", "numscript.cache.size")
	})

	require.ElementsMatch(t, []string{
		"ledger_admission_preload_total",
		"ledger_wal_append_save_duration",
		"ledger_raft_fsm_logs_appended",
		"ledger_pebble_flush_total",
		"ledger_numscript_cache_size",
	}, names)
}

func TestFactory_PromNamingDoesNotDoublePrefix(t *testing.T) {
	t.Parallel()

	// Call sites that hand-rolled the application namespace
	// (`ledger.preload.coverage_miss`) must keep a single `ledger_`
	// prefix in prom mode rather than getting `ledger_ledger_…`.
	names := collectInstrumentNames(t, metrics.NamingProm, func(mp otelmetric.MeterProvider) {
		c, err := mp.Meter("raft.node").Int64Counter("ledger.preload.coverage_miss")
		require.NoError(t, err)
		c.Add(context.Background(), 1)
	})

	require.Equal(t, []string{"ledger_preload_coverage_miss"}, names)
}

func TestFactory_PromNamingCoversEveryInstrumentKind(t *testing.T) {
	t.Parallel()

	// Smoke-test every constructor on metric.Meter to make sure the
	// wrapper doesn't drop any kind. The exact name choice doesn't
	// matter — we only check that it comes out prefixed.
	names := collectInstrumentNames(t, metrics.NamingProm, func(mp otelmetric.MeterProvider) {
		m := mp.Meter("admission")
		ctx := context.Background()

		c, err := m.Int64Counter("admission.int_counter")
		require.NoError(t, err)
		c.Add(ctx, 1)

		ud, err := m.Int64UpDownCounter("admission.int_updown")
		require.NoError(t, err)
		ud.Add(ctx, 1)

		h, err := m.Int64Histogram("admission.int_hist")
		require.NoError(t, err)
		h.Record(ctx, 1)

		g, err := m.Int64Gauge("admission.int_gauge")
		require.NoError(t, err)
		g.Record(ctx, 1)

		fc, err := m.Float64Counter("admission.float_counter")
		require.NoError(t, err)
		fc.Add(ctx, 1)

		fud, err := m.Float64UpDownCounter("admission.float_updown")
		require.NoError(t, err)
		fud.Add(ctx, 1)

		fh, err := m.Float64Histogram("admission.float_hist")
		require.NoError(t, err)
		fh.Record(ctx, 1)

		fg, err := m.Float64Gauge("admission.float_gauge")
		require.NoError(t, err)
		fg.Record(ctx, 1)
	})

	// Every collected name should be prefixed.
	require.NotEmpty(t, names)
	for _, n := range names {
		require.True(t, strings.HasPrefix(n, "ledger_"),
			"instrument %q is not prefixed", n)
	}
}

func TestParseNaming(t *testing.T) {
	t.Parallel()

	tests := []struct {
		in   string
		want metrics.Naming
		err  bool
	}{
		{"otel", metrics.NamingOTel, false},
		{"prom", metrics.NamingProm, false},
		// Empty string is accepted and maps to the default so config
		// fixtures constructed as struct literals don't have to set
		// this field explicitly.
		{"", metrics.DefaultNaming, false},
		{"prometheus", "", true},
		{"OTEL", "", true}, // case-sensitive: rejects ambiguity at config time
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got, err := metrics.ParseNaming(tc.in)
			if tc.err {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
