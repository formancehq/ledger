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
func collectInstrumentNames(t *testing.T, naming metrics.Naming, prefix string, register func(otelmetric.MeterProvider)) []string {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})

	factory := metrics.NewFactory(provider, naming, prefix)
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

// registerSampleInstruments creates one counter per meter our code
// owns (admission, wal, …) and per meter whose name hints at library
// code we wrap ourselves (raft, pebble, numscript). Any instrument
// created via the factory is subject to the policy. The OTel
// auto-instrumentation that targets the *global* MeterProvider
// bypasses the factory and is not exercised here.
func registerSampleInstruments(t *testing.T) func(otelmetric.MeterProvider) {
	t.Helper()

	return func(mp otelmetric.MeterProvider) {
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
	}
}

func TestFactory_OTelNamingWithoutPrefixPreservesNames(t *testing.T) {
	t.Parallel()

	names := collectInstrumentNames(t, metrics.NamingOTel, "", registerSampleInstruments(t))

	require.ElementsMatch(t, []string{
		"admission.preload.total",
		"wal.append.save.duration",
		"raft.fsm.logs_appended",
		"pebble.flush.total",
		"numscript.cache.size",
	}, names)
}

func TestFactory_OTelNamingPrefixesEveryInstrument(t *testing.T) {
	t.Parallel()

	names := collectInstrumentNames(t, metrics.NamingOTel, metrics.DefaultPrefix, registerSampleInstruments(t))

	require.ElementsMatch(t, []string{
		"formance.ledger.admission.preload.total",
		"formance.ledger.wal.append.save.duration",
		"formance.ledger.raft.fsm.logs_appended",
		"formance.ledger.pebble.flush.total",
		"formance.ledger.numscript.cache.size",
	}, names)
}

func TestFactory_PromNamingPrefixesEveryInstrument(t *testing.T) {
	t.Parallel()

	names := collectInstrumentNames(t, metrics.NamingProm, metrics.DefaultPrefix, registerSampleInstruments(t))

	require.ElementsMatch(t, []string{
		"formance_ledger_admission_preload_total",
		"formance_ledger_wal_append_save_duration",
		"formance_ledger_raft_fsm_logs_appended",
		"formance_ledger_pebble_flush_total",
		"formance_ledger_numscript_cache_size",
	}, names)
}

func TestFactory_PromNamingWithoutPrefixOnlyReplacesDots(t *testing.T) {
	t.Parallel()

	names := collectInstrumentNames(t, metrics.NamingProm, "", registerSampleInstruments(t))

	require.ElementsMatch(t, []string{
		"admission_preload_total",
		"wal_append_save_duration",
		"raft_fsm_logs_appended",
		"pebble_flush_total",
		"numscript_cache_size",
	}, names)
}

func TestFactory_CustomPrefix(t *testing.T) {
	t.Parallel()

	register := func(mp otelmetric.MeterProvider) {
		c, err := mp.Meter("admission").Int64Counter("admission.preload.total")
		require.NoError(t, err)
		c.Add(context.Background(), 1)
	}

	require.Equal(t, []string{"acme.payments_ledger.admission.preload.total"},
		collectInstrumentNames(t, metrics.NamingOTel, "acme.payments_ledger", register))
	require.Equal(t, []string{"acme_payments_ledger_admission_preload_total"},
		collectInstrumentNames(t, metrics.NamingProm, "acme.payments_ledger", register))
}

func TestFactory_PromNamingCoversEveryInstrumentKind(t *testing.T) {
	t.Parallel()

	// Smoke-test every constructor on metric.Meter to make sure the
	// wrapper doesn't drop any kind. The exact name choice doesn't
	// matter — we only check that it comes out prefixed.
	names := collectInstrumentNames(t, metrics.NamingProm, metrics.DefaultPrefix, func(mp otelmetric.MeterProvider) {
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
		require.True(t, strings.HasPrefix(n, "formance_ledger_"),
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

func TestParsePrefix(t *testing.T) {
	t.Parallel()

	long := strings.Repeat("a", metrics.MaxPrefixLength)
	tests := []struct {
		in   string
		want string
		err  bool
	}{
		{in: metrics.NoPrefix, want: ""}, // disables the namespace
		{in: "", want: ""},               // explicit empty flag value
		{in: metrics.DefaultPrefix, want: metrics.DefaultPrefix},
		{in: "ledger", want: "ledger"},
		{in: "acme.payments_ledger2", want: "acme.payments_ledger2"},
		{in: long, want: long},
		{in: long + "a", err: true},
		{in: "formance.ledger.", err: true}, // trailing separator would yield ".."
		{in: "formance..ledger", err: true},
		{in: "formance_.ledger", err: true},
		{in: "formance._ledger", err: true},
		{in: ".formance", err: true},
		{in: "formance_", err: true},
		{in: "1formance", err: true},
		{in: "formance-ledger", err: true}, // "-" is invalid in Prometheus names
		{in: "formance ledger", err: true},
		{in: "formance/ledger", err: true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			t.Parallel()
			got, err := metrics.ParsePrefix(tc.in)
			if tc.err {
				require.Error(t, err)

				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
