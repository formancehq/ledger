package tailworker

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestComputeLag(t *testing.T) {
	t.Parallel()

	require.Equal(t, int64(5), computeLag(3, 8))
	require.Equal(t, int64(0), computeLag(8, 8))
	require.Equal(t, int64(0), computeLag(10, 8), "indexed ahead of source clamps to 0")
}

func TestRegisterTailGaugesObserves(t *testing.T) {
	t.Parallel()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := provider.Meter("test")

	var indexed, sourceLast atomic.Uint64
	indexed.Store(3)
	sourceLast.Store(8)

	reg, err := RegisterTailGauges(meter, "widget", "source", &indexed, &sourceLast)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reg.Unregister() })

	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))

	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			g, ok := m.Data.(metricdata.Gauge[int64])
			require.True(t, ok, "metric %s must be an int64 gauge", m.Name)
			got[m.Name] = g.DataPoints[0].Value
		}
	}

	require.Equal(t, int64(3), got["widget.last_indexed_sequence"])
	require.Equal(t, int64(8), got["widget.source_last_sequence"])
	require.Equal(t, int64(5), got["widget.lag"])
}
