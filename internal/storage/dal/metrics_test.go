package dal

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestStatusFromErr(t *testing.T) {
	t.Parallel()

	t.Run("nil error returns ok", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, "ok", statusFromErr(nil))
	})

	t.Run("non-nil error returns error", func(t *testing.T) {
		t.Parallel()
		require.Equal(t, "error", statusFromErr(errors.New("something")))
	})
}

func TestNewMetricsListener(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	listener := NewMetricsListener(meter, NewWriteStallState())
	require.NotNil(t, listener)
	require.NotNil(t, listener.FlushEnd)
	require.NotNil(t, listener.CompactionEnd)
	require.NotNil(t, listener.WriteStallBegin)
	require.NotNil(t, listener.WriteStallEnd)
}

func TestMetricsListener_Callbacks(t *testing.T) {
	t.Parallel()

	meter := noop.NewMeterProvider().Meter("test")
	listener := NewMetricsListener(meter, NewWriteStallState())

	// Exercise FlushEnd callback (should not panic)
	listener.FlushEnd(pebble.FlushInfo{
		Reason:   "test",
		Duration: 42 * time.Millisecond,
	})

	// Exercise FlushEnd with error
	listener.FlushEnd(pebble.FlushInfo{
		Reason: "test-err",
		Err:    errors.New("flush failed"),
	})

	// Exercise CompactionEnd callback
	listener.CompactionEnd(pebble.CompactionInfo{
		Reason:   "test",
		Duration: 100 * time.Millisecond,
	})

	// Exercise CompactionEnd with error
	listener.CompactionEnd(pebble.CompactionInfo{
		Reason: "test-err",
		Err:    errors.New("compact failed"),
	})

	// Exercise WriteStallBegin/End cycle
	listener.WriteStallBegin(pebble.WriteStallBeginInfo{
		Reason: "memtable",
	})
	listener.WriteStallEnd()

	// Exercise WriteStallEnd without a preceding Begin
	listener.WriteStallEnd()

	// Exercise nested stall begin (second begin while first is still active)
	listener.WriteStallBegin(pebble.WriteStallBeginInfo{
		Reason: "first-stall",
	})
	listener.WriteStallBegin(pebble.WriteStallBeginInfo{
		Reason: "second-stall",
	})
	listener.WriteStallEnd()
}

// TestMetricsListener_RecordsSeconds pins the unit conversion end to end:
// Pebble reports time.Duration values, and the histograms declare seconds,
// so a 1.5s flush must be observed as 1.5, not 1500 (ms) or 1500000 (us).
func TestMetricsListener_RecordsSeconds(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	listener := NewMetricsListener(provider.Meter("test"), NewWriteStallState())

	listener.FlushEnd(pebble.FlushInfo{Reason: "test", Duration: 1500 * time.Millisecond})
	listener.CompactionEnd(pebble.CompactionInfo{Reason: "test", Duration: 2 * time.Second})
	listener.DiskSlow(pebble.DiskSlowInfo{Duration: 250 * time.Millisecond})

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	want := map[string]float64{
		"pebble.flush.duration":      1.5,
		"pebble.compaction.duration": 2,
		"pebble.disk_slow.duration":  0.25,
	}
	got := make(map[string]float64)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if _, ok := want[m.Name]; !ok {
				continue
			}
			require.Equal(t, "s", m.Unit, m.Name)
			hist, ok := m.Data.(metricdata.Histogram[float64])
			require.True(t, ok, "%s must be a float64 histogram, got %T", m.Name, m.Data)
			require.Len(t, hist.DataPoints, 1, m.Name)
			require.EqualValues(t, 1, hist.DataPoints[0].Count, m.Name)
			got[m.Name] = hist.DataPoints[0].Sum
		}
	}
	require.Equal(t, want, got)
}

func TestStore_GetMetrics(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	metrics := s.GetMetrics()
	require.NotNil(t, metrics)
}
