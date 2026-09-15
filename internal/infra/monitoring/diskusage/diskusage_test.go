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

func TestVolumeUsage_PublishesCoherentSample(t *testing.T) {
	t.Parallel()

	var usage VolumeUsage
	usage.store(70, 100)

	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 200000 {
			usage.store(154, 200)
			usage.store(70, 100)
		}
	}()

	for {
		used, total := usage.Load()
		require.True(t,
			used == 70 && total == 100 || used == 154 && total == 200,
			"observed fabricated disk sample %d/%d", used, total,
		)

		select {
		case <-done:
			return
		default:
		}
	}
}

func TestCollector_StartAndStop(t *testing.T) {
	t.Parallel()

	walDir := t.TempDir()
	dataDir := t.TempDir()

	provider := sdkmetric.NewMeterProvider(newTestMeter())
	c := NewCollector(walDir, dataDir, 100*time.Millisecond, provider.Meter("test"))
	c.Start()

	// After Start, collect should have run once synchronously via Statfs
	walUsed, walTotal := c.WALVolume.Load()
	dataUsed, dataTotal := c.DataVolume.Load()
	require.Positive(t, walUsed)
	require.Positive(t, walTotal)
	require.Positive(t, dataUsed)
	require.Positive(t, dataTotal)

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
