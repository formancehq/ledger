package dal

import (
	"time"

	"github.com/formancehq/ledger/v3/internal/storage/pebblecfg"
)

// ValueSeparationConfig controls Pebble's value separation feature.
// When enabled, large values are stored in external blob files instead of
// inline in SSTables, reducing compaction IO for write-heavy workloads.
type ValueSeparationConfig struct {
	// Enabled controls whether value separation is active.
	// Requires columnar blocks (automatically enabled when this is true).
	// Default: false
	Enabled bool `yaml:"enabled"`

	// MinimumSize is the minimum value size (in bytes) to separate into a blob file.
	// Values smaller than this are kept inline in the SSTable.
	// Default: 256
	MinimumSize int `yaml:"minimumSize"`

	// MaxBlobReferenceDepth limits overlapping blob files referenced by a single SSTable.
	// Lower values reduce read amplification at the cost of more rewrite compactions.
	// Default: 4
	MaxBlobReferenceDepth int `yaml:"maxBlobReferenceDepth"`

	// RewriteMinimumAge is the minimum age of a blob file before it can be rewritten
	// to reclaim space. Lower values reduce space amplification but increase write amp.
	// Default: 1h
	RewriteMinimumAge time.Duration `yaml:"rewriteMinimumAge"`

	// TargetGarbageRatio is the fraction of unreferenced data in blob files
	// before the DB rewrites them. Range [0, 1.0].
	// 0.20 means rewrite when 20% of blob data is garbage.
	// 1.0 disables blob rewriting entirely.
	// Default: 0.20
	TargetGarbageRatio float64 `yaml:"targetGarbageRatio"`
}

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

	// WALFailoverDir is the secondary WAL directory for automatic failover
	// when the primary disk experiences high latency. When set, Pebble monitors
	// WAL write latency and switches to this directory if the primary becomes
	// unhealthy (>100ms operation latency by default). It probes the primary
	// every second and fails back when healthy for 15s.
	//
	// Useful in cloud environments where EBS/PD volumes have unpredictable
	// latency spikes. Set to a path on a different physical volume (or tmpfs)
	// for best results.
	//
	// Default: "" (disabled)
	WALFailoverDir string `yaml:"walFailoverDir"`

	// ValueSeparation controls Pebble's value separation (blob files) feature.
	ValueSeparation ValueSeparationConfig `yaml:"valueSeparation"`

	// DeterministicEncoding routes every WriteSession proto marshal through
	// MarshalDeterministicVT (map keys sorted) instead of the default
	// MarshalToSizedBufferVT. Set by the bootstrap layer from the immutable
	// PersistedConfig.fsm_determinism_enabled flag. The store records the
	// value at NewStore time; every OpenWriteSession inherits it.
	DeterministicEncoding bool `yaml:"-"`
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
			Compression:                 pebblecfg.DefaultLevelCompression(),
		},
		WALBytesPerSync:    1 << 20, // 1MB
		WALMinSyncInterval: 0,       // immediate sync
		DisableWAL:         false,
		MaxCheckpoints:     10,
		ValueSeparation: ValueSeparationConfig{
			Enabled:               false,
			MinimumSize:           256,
			MaxBlobReferenceDepth: 4,
			RewriteMinimumAge:     time.Hour,
			TargetGarbageRatio:    0.20,
		},
	}
}
