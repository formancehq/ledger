package dal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultConfig()

	require.Equal(t, uint64(256<<20), cfg.MemTableSize)
	require.Equal(t, 6, cfg.MemTableStopWritesThreshold)
	require.Equal(t, 4, cfg.L0CompactionThreshold)
	require.Equal(t, 16, cfg.L0StopWritesThreshold)
	require.Equal(t, int64(2<<30), cfg.LBaseMaxBytes)
	require.Equal(t, int64(1024<<20), cfg.CacheSize)
	require.Equal(t, int64(256<<20), cfg.TargetFileSize)
	require.Equal(t, 1<<20, cfg.BytesPerSync)
	require.Equal(t, 1<<20, cfg.WALBytesPerSync)
	require.Equal(t, 2, cfg.MaxConcurrentCompactions)
	require.Equal(t, time.Duration(0), cfg.WALMinSyncInterval)
	require.False(t, cfg.DisableWAL)
	require.Equal(t, 10, cfg.MaxCheckpoints)
}
