package dal

import (
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/storage/pebblecfg"
)

// Config contains all configurable options for Pebble storage.
// All sizes are in bytes unless otherwise specified.
type Config struct {
	pebblecfg.Config `yaml:",inline"`

	// WALBytesPerSync is the number of bytes written to the WAL before syncing.
	// Default: 1MB (1 << 20)
	WALBytesPerSync int `yaml:"walBytesPerSync"`

	// MaxConcurrentDownloads is unused (kept for config compat).
	// Default: 0
	MaxConcurrentDownloads int `yaml:"maxConcurrentDownloads"`

	// WALMinSyncInterval is the minimum interval between WAL syncs.
	// Set to 0 for immediate sync (default), or a duration like 100ms for batching.
	// Non-zero values improve write throughput but risk data loss on crash.
	// Default: 0 (immediate sync)
	WALMinSyncInterval time.Duration `yaml:"walMinSyncInterval"`

	// DisableWAL disables the write-ahead log entirely.
	// WARNING: This risks data loss on crash. Only use for testing or ephemeral data.
	// Default: false
	DisableWAL bool `yaml:"disableWAL"`

	// MaxCheckpoints is the maximum number of checkpoints to keep.
	// Older checkpoints beyond this limit are deleted during snapshot creation.
	// Default: 10
	MaxCheckpoints int `yaml:"maxCheckpoints"`

	// IncrementalCompactThreshold is the number of new log entries before the
	// SmartCompactor triggers an incremental compaction of just the new range.
	// Default: 100000
	IncrementalCompactThreshold uint64 `yaml:"incrementalCompactThreshold"`
}

// DefaultConfig returns the default Pebble configuration.
// These defaults are tuned for write-heavy workloads.
func DefaultConfig() Config {
	return Config{
		Config: pebblecfg.Config{
			MemTableSize:                256 << 20, // 256MB
			MemTableStopWritesThreshold: 6,
			L0CompactionThreshold:       4,
			L0StopWritesThreshold:       16,
			LBaseMaxBytes:               2 << 30,    // 2GB
			CacheSize:                   1024 << 20, // 1GB
			TargetFileSize:              256 << 20,  // 256MB
			BytesPerSync:                1 << 20,    // 1MB
			MaxConcurrentCompactions:    2,
		},
		WALBytesPerSync:             1 << 20, // 1MB
		WALMinSyncInterval:          0,       // immediate sync
		DisableWAL:                  false,
		MaxCheckpoints:              10,
		IncrementalCompactThreshold: 100_000,
	}
}
