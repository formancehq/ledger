package tracing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// histogramDatapoints returns the int64 histogram datapoints for the named
// metric, aggregated across every scope so the helper does not depend on the
// metric living in a single instrumentation scope.
func histogramDatapoints(t *testing.T, rdr *sdkmetric.ManualReader, name string) []metricdata.HistogramDataPoint[int64] {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, rdr.Collect(context.Background(), &rm))
	var dps []metricdata.HistogramDataPoint[int64]
	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != name {
				continue
			}
			h, ok := md.Data.(metricdata.Histogram[int64])
			require.True(t, ok)
			dps = append(dps, h.DataPoints...)
			found = true
		}
	}
	require.True(t, found, "metric %q not found", name)
	return dps
}

// Without the wrapper the per-ledger identity lives only on the scope, so the
// datapoint has no ledger label. With MeterWithAttributes it becomes a real
// label, which is what keeps each ledger's cumulative series distinct.
func TestMeterWithAttributes_InjectsLedgerLabel(t *testing.T) {
	rdr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr))

	meter := MeterWithAttributes(mp.Meter("ledger"), attribute.String("ledger", "deriv"))
	h, err := meter.Int64Histogram("controller.create_transaction")
	require.NoError(t, err)
	h.Record(context.Background(), 5)
	h.Record(context.Background(), 7)

	dps := histogramDatapoints(t, rdr, "controller.create_transaction")
	require.Len(t, dps, 1)
	require.Equal(t, uint64(2), dps[0].Count)
	v, ok := dps[0].Attributes.Value("ledger")
	require.True(t, ok, "ledger label must be present as a datapoint attribute")
	require.Equal(t, "deriv", v.AsString())
}

// Two ledgers must yield two distinct series (no collapse).
func TestMeterWithAttributes_SeparatesLedgers(t *testing.T) {
	rdr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr))

	for _, name := range []string{"main", "treasury"} {
		meter := MeterWithAttributes(mp.Meter("ledger"), attribute.String("ledger", name))
		h, err := meter.Int64Histogram("controller.create_transaction")
		require.NoError(t, err)
		h.Record(context.Background(), 1)
	}

	dps := histogramDatapoints(t, rdr, "controller.create_transaction")
	require.Len(t, dps, 2, "each ledger must keep its own series")
}

// Caller-supplied attributes (e.g. the deadlock counter's "operation") must be
// preserved alongside the injected ledger label.
func TestMeterWithAttributes_MergesCallerAttributes(t *testing.T) {
	rdr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr))

	meter := MeterWithAttributes(mp.Meter("ledger"), attribute.String("ledger", "deriv"))
	c, err := meter.Int64Counter("controller.deadlocks")
	require.NoError(t, err)
	c.Add(context.Background(), 1, metric.WithAttributes(attribute.String("operation", "CreateTransaction")))

	var rm metricdata.ResourceMetrics
	require.NoError(t, rdr.Collect(context.Background(), &rm))
	var got attribute.Set
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != "controller.deadlocks" {
				continue
			}
			sum, ok := md.Data.(metricdata.Sum[int64])
			require.True(t, ok)
			require.Len(t, sum.DataPoints, 1)
			got = sum.DataPoints[0].Attributes
		}
	}
	ledger, ok := got.Value("ledger")
	require.True(t, ok)
	require.Equal(t, "deriv", ledger.AsString())
	op, ok := got.Value("operation")
	require.True(t, ok)
	require.Equal(t, "CreateTransaction", op.AsString())
}

// A caller that supplies the same key as the injected attribute must win: the
// injected option is prepended and OTel applies options in order, last value
// taking precedence on conflict.
func TestMeterWithAttributes_CallerOverridesInjected(t *testing.T) {
	rdr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr))

	meter := MeterWithAttributes(mp.Meter("ledger"), attribute.String("ledger", "deriv"))
	h, err := meter.Int64Histogram("controller.create_transaction")
	require.NoError(t, err)
	h.Record(context.Background(), 1, metric.WithAttributes(attribute.String("ledger", "override")))

	dps := histogramDatapoints(t, rdr, "controller.create_transaction")
	require.Len(t, dps, 1)
	v, ok := dps[0].Attributes.Value("ledger")
	require.True(t, ok)
	require.Equal(t, "override", v.AsString(), "caller attribute must take precedence")
}

// Float64 instruments are wrapped too, so a future Float64 metric does not
// silently lose the label.
func TestMeterWithAttributes_Float64InstrumentsLabelled(t *testing.T) {
	rdr := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(rdr))

	meter := MeterWithAttributes(mp.Meter("ledger"), attribute.String("ledger", "deriv"))
	c, err := meter.Float64Counter("controller.some_float_counter")
	require.NoError(t, err)
	c.Add(context.Background(), 1.5)

	var rm metricdata.ResourceMetrics
	require.NoError(t, rdr.Collect(context.Background(), &rm))
	for _, sm := range rm.ScopeMetrics {
		for _, md := range sm.Metrics {
			if md.Name != "controller.some_float_counter" {
				continue
			}
			sum, ok := md.Data.(metricdata.Sum[float64])
			require.True(t, ok)
			require.Len(t, sum.DataPoints, 1)
			v, ok := sum.DataPoints[0].Attributes.Value("ledger")
			require.True(t, ok, "Float64Counter must carry the injected label")
			require.Equal(t, "deriv", v.AsString())
			return
		}
	}
	t.Fatal("controller.some_float_counter not found")
}
