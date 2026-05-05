package diskusage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func newTestMeter() sdkmetric.Option {
	return sdkmetric.WithReader(sdkmetric.NewManualReader())
}

func TestCollector_StartAndStop(t *testing.T) {
	t.Parallel()

	walDir := t.TempDir()
	dataDir := t.TempDir()

	provider := sdkmetric.NewMeterProvider(newTestMeter())
	c := NewCollector(walDir, dataDir, 100*time.Millisecond, provider.Meter("test"))
	c.Start()

	// After Start, collect should have run once synchronously via Statfs
	require.Positive(t, c.WALVolume.UsedBytes())
	require.Positive(t, c.WALVolume.TotalBytes())
	require.Positive(t, c.DataVolume.UsedBytes())
	require.Positive(t, c.DataVolume.TotalBytes())

	c.Stop()
}

func TestCollector_RegisterMetrics(t *testing.T) {
	t.Parallel()

	walDir := t.TempDir()
	dataDir := t.TempDir()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	c := NewCollector(walDir, dataDir, time.Hour, provider.Meter("test"))
	c.Start()

	var rm metricdata.ResourceMetrics

	err := reader.Collect(t.Context(), &rm)
	require.NoError(t, err)
	require.NotEmpty(t, rm.ScopeMetrics)

	c.Stop()
}
