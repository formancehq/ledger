package data

import "time"

// Config contains all configurable options for Pebble storage.
// All sizes are in bytes unless otherwise specified.
type Config struct {
	// MemTableSize is the size of a single memtable in bytes.
	// Larger values absorb more writes before flush, reducing SST files and compactions.
	// Default: 256MB (256 << 20)
	MemTableSize uint64 `yaml:"memTableSize"`

	// MemTableStopWritesThreshold is the number of memtables before writes are stopped.
	// Higher values reduce write stalls at the cost of increased memory usage.
	// Default: 6
	MemTableStopWritesThreshold int `yaml:"memTableStopWritesThreshold"`

	// L0CompactionThreshold triggers L0->L1 compactions when L0 files reach this count.
	// Lower values prevent runaway L0 growth but increase compaction frequency.
	// Default: 64
	L0CompactionThreshold int `yaml:"l0CompactionThreshold"`

	// L0StopWritesThreshold is the L0 file count at which writes are stopped.
	// Higher values allow more L0 files before stalling, but increase read amplification.
	// Default: 256
	L0StopWritesThreshold int `yaml:"l0StopWritesThreshold"`

	// LBaseMaxBytes is the maximum size of the base level (L1) in bytes.
	// Larger values are better for larger datasets.
	// Default: 2GB (2 << 30)
	LBaseMaxBytes int64 `yaml:"lBaseMaxBytes"`

	// CacheSize is the size of the block cache in bytes.
	// Larger caches improve read performance at the cost of memory.
	// Default: 1GB (1024 << 20)
	CacheSize int64 `yaml:"cacheSize"`

	// TargetFileSize is the target size for SST files in bytes.
	// Larger files mean fewer files and fewer compactions.
	// Default: 256MB (256 << 20)
	TargetFileSize int64 `yaml:"targetFileSize"`

	// BytesPerSync is the number of bytes written before syncing to disk.
	// Smooths IO during flush/compactions.
	// Default: 1MB (1 << 20)
	BytesPerSync int `yaml:"bytesPerSync"`

	// WALBytesPerSync is the number of bytes written to the WAL before syncing.
	// Default: 1MB (1 << 20)
	WALBytesPerSync int `yaml:"walBytesPerSync"`

	// MaxConcurrentCompactions is the maximum number of concurrent compaction operations.
	// Higher values can speed up compaction but may saturate IO.
	// Default: 2
	MaxConcurrentCompactions int `yaml:"maxConcurrentCompactions"`

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
}

// DefaultConfig returns the default Pebble configuration.
// These defaults are tuned for write-heavy workloads.
func DefaultConfig() Config {
	return Config{
		MemTableSize:                256 << 20, // 256MB
		MemTableStopWritesThreshold: 6,
		L0CompactionThreshold:       64,
		L0StopWritesThreshold:       256,
		LBaseMaxBytes:               2 << 30,    // 2GB
		CacheSize:                   1024 << 20, // 1GB
		TargetFileSize:              256 << 20,  // 256MB
		BytesPerSync:                1 << 20,    // 1MB
		WALBytesPerSync:             1 << 20,    // 1MB
		MaxConcurrentCompactions:    2,
		WALMinSyncInterval:          0, // immediate sync
		DisableWAL:                  false,
		MaxCheckpoints:              10,
	}
}
