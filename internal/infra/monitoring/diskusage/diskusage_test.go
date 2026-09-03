package diskusage

import (
	"os"
	"path/filepath"
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
	walSample := c.WALVolume.Load()
	require.True(t, walSample.Valid)
	require.Empty(t, walSample.Error)
	require.False(t, walSample.ObservedAt.IsZero())
	require.Positive(t, walSample.UsedBytes)
	require.Positive(t, walSample.TotalBytes)
	require.Equal(t, walSample.UsedBytes, c.WALVolume.UsedBytes())
	require.Equal(t, walSample.TotalBytes, c.WALVolume.TotalBytes())

	dataSample := c.DataVolume.Load()
	require.True(t, dataSample.Valid)
	require.Empty(t, dataSample.Error)
	require.False(t, dataSample.ObservedAt.IsZero())
	require.Positive(t, dataSample.UsedBytes)
	require.Positive(t, dataSample.TotalBytes)

	c.Stop()
}

func TestCollector_InvalidatesButPreservesLastSuccessfulSample(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	require.NoError(t, os.Mkdir(walDir, 0755))

	provider := sdkmetric.NewMeterProvider(newTestMeter())
	c := NewCollector(walDir, t.TempDir(), time.Hour, provider.Meter("test"))
	c.Start()
	c.Stop()

	lastSuccess := c.WALVolume.Load()
	require.True(t, lastSuccess.Valid)
	require.NoError(t, os.Remove(walDir))

	c.collect()

	failed := c.WALVolume.Load()
	require.False(t, failed.Valid)
	require.NotEmpty(t, failed.Error)
	require.Equal(t, lastSuccess.ObservedAt, failed.ObservedAt)
	require.Equal(t, lastSuccess.UsedBytes, failed.UsedBytes)
	require.Equal(t, lastSuccess.TotalBytes, failed.TotalBytes)
	require.True(t, c.DataVolume.Load().Valid)
}

func TestCollector_RecoversAfterCollectionFailure(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	provider := sdkmetric.NewMeterProvider(newTestMeter())
	c := NewCollector(walDir, t.TempDir(), time.Hour, provider.Meter("test"))

	c.collect()
	failed := c.WALVolume.Load()
	require.False(t, failed.Valid)
	require.NotEmpty(t, failed.Error)
	require.True(t, failed.ObservedAt.IsZero())

	require.NoError(t, os.Mkdir(walDir, 0755))
	c.collect()
	recovered := c.WALVolume.Load()
	require.True(t, recovered.Valid)
	require.Empty(t, recovered.Error)
	require.False(t, recovered.ObservedAt.IsZero())
}

func TestVolumeSampleUsable(t *testing.T) {
	t.Parallel()

	now := time.Now()
	require.True(t, (VolumeSample{Valid: true, TotalBytes: 100, ObservedAt: now.Add(-MaximumSampleAge)}).Usable(now))
	require.False(t, (VolumeSample{Valid: true, TotalBytes: 100, ObservedAt: now.Add(-MaximumSampleAge - time.Millisecond)}).Usable(now))
	require.False(t, (VolumeSample{Valid: false, TotalBytes: 100, ObservedAt: now}).Usable(now))
	require.False(t, (VolumeSample{Valid: true, TotalBytes: 0, ObservedAt: now}).Usable(now))
	require.True(t, (VolumeSample{Valid: true, TotalBytes: 100, ObservedAt: now.Add(time.Millisecond)}).Usable(now))
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
