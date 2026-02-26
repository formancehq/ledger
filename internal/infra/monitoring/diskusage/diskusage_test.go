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

func TestDirSize_SingleFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), []byte("hello"), 0644))

	size, err := DirSize(dir)
	require.NoError(t, err)
	require.Equal(t, int64(5), size)
}

func TestDirSize_NestedFiles(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	sub := filepath.Join(dir, "sub")
	require.NoError(t, os.MkdirAll(sub, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.txt"), []byte("aaa"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(sub, "b.txt"), []byte("bbbbb"), 0644))

	size, err := DirSize(dir)
	require.NoError(t, err)
	require.Equal(t, int64(8), size) // 3 + 5
}

func TestDirSize_EmptyDirectory(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	size, err := DirSize(dir)
	require.NoError(t, err)
	require.Equal(t, int64(0), size)
}

func TestDirSize_NonExistentDirectory(t *testing.T) {
	t.Parallel()

	_, err := DirSize("/nonexistent/path/that/does/not/exist")
	require.Error(t, err)
}

func TestDirSizeExcluding(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	excluded := filepath.Join(dir, "excluded")
	require.NoError(t, os.MkdirAll(excluded, 0755))

	require.NoError(t, os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("keep"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(excluded, "skip.txt"), []byte("skip this"), 0644))

	size, err := dirSizeExcluding(dir, excluded)
	require.NoError(t, err)
	require.Equal(t, int64(4), size) // only "keep"
}

func TestCollector_StartAndStop(t *testing.T) {
	t.Parallel()

	walDir := t.TempDir()
	dataDir := t.TempDir()

	// Create spool subdir
	spoolDir := filepath.Join(walDir, "spool")
	require.NoError(t, os.MkdirAll(spoolDir, 0755))

	// Create test files
	require.NoError(t, os.WriteFile(filepath.Join(walDir, "wal.log"), []byte("wal data"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(spoolDir, "spool.dat"), []byte("spool"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "data.db"), []byte("database content"), 0644))

	c := NewCollector(walDir, dataDir, 100*time.Millisecond)
	c.Start()

	// After Start, collect should have run once synchronously
	require.Greater(t, c.SpoolBytes(), int64(0))
	require.GreaterOrEqual(t, c.WALBytes(), int64(0))
	require.Greater(t, c.DataBytes(), int64(0))
	require.Greater(t, c.WALVolumeBytes(), int64(0))
	require.Greater(t, c.DataVolumeBytes(), int64(0))
	require.Greater(t, c.WALVolumeTotalBytes(), int64(0))
	require.Greater(t, c.DataVolumeTotalBytes(), int64(0))

	c.Stop()
}

func TestCollector_RegisterMetrics(t *testing.T) {
	t.Parallel()

	walDir := t.TempDir()
	dataDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(walDir, "spool"), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(walDir, "wal.log"), []byte("wal"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "data.db"), []byte("data"), 0644))

	c := NewCollector(walDir, dataDir, time.Hour)
	c.collect()

	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	meter := provider.Meter("test")

	reg, err := c.RegisterMetrics(meter)
	require.NoError(t, err)
	require.NotNil(t, reg)

	// Trigger the callback by collecting metrics
	var rm metricdata.ResourceMetrics
	err = reader.Collect(t.Context(), &rm)
	require.NoError(t, err)
	require.NotEmpty(t, rm.ScopeMetrics)
}

func TestCollector_AtomicReads(t *testing.T) {
	t.Parallel()

	walDir := t.TempDir()
	dataDir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(walDir, "spool"), 0755))

	c := NewCollector(walDir, dataDir, time.Hour) // long interval - we'll call collect manually

	// Before collect, everything should be 0
	require.Equal(t, int64(0), c.SpoolBytes())
	require.Equal(t, int64(0), c.WALBytes())
	require.Equal(t, int64(0), c.DataBytes())

	// Write a file and collect
	require.NoError(t, os.WriteFile(filepath.Join(dataDir, "file.db"), []byte("content"), 0644))
	c.collect()

	require.Equal(t, int64(7), c.DataBytes())
}
