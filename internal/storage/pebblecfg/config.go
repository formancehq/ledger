package pebblecfg

import (
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
)

// Config contains the common Pebble tunables shared by both the primary
// DAL store and the read index store.
type Config struct {
	// MemTableSize is the size of a single memtable in bytes.
	MemTableSize uint64 `yaml:"memTableSize"`

	// MemTableStopWritesThreshold is the number of memtables before writes are stopped.
	MemTableStopWritesThreshold int `yaml:"memTableStopWritesThreshold"`

	// L0CompactionThreshold triggers L0→L1 compactions when L0 files reach this count.
	L0CompactionThreshold int `yaml:"l0CompactionThreshold"`

	// L0StopWritesThreshold is the L0 file count at which writes are stopped.
	L0StopWritesThreshold int `yaml:"l0StopWritesThreshold"`

	// LBaseMaxBytes is the maximum size of the base level (L1) in bytes.
	LBaseMaxBytes int64 `yaml:"lBaseMaxBytes"`

	// CacheSize is the size of the block cache in bytes.
	CacheSize int64 `yaml:"cacheSize"`

	// TargetFileSize is the target size for SST files in bytes.
	TargetFileSize int64 `yaml:"targetFileSize"`

	// BytesPerSync is the number of bytes written before syncing to disk.
	BytesPerSync int `yaml:"bytesPerSync"`

	// MaxConcurrentCompactions is the maximum number of concurrent compaction operations.
	MaxConcurrentCompactions int `yaml:"maxConcurrentCompactions"`

	// Compression is the per-level compression configuration (L0–L6).
	// Default: L0–L3 Snappy, L4–L6 Zstd.
	Compression LevelCompression `yaml:"compression"`
}

// BuildLevels constructs the [NumLevels]pebble.LevelOptions from this configuration.
func (cfg Config) BuildLevels() [NumLevels]pebble.LevelOptions {
	var levels [NumLevels]pebble.LevelOptions
	for i := range levels {
		profile := cfg.Compression[i].ToPebble()
		levels[i] = pebble.LevelOptions{
			FilterPolicy: bloom.FilterPolicy(10),
			Compression: func() *sstable.CompressionProfile {
				return profile
			},
		}
	}

	return levels
}

// BuildTargetFileSizes constructs the [NumLevels]int64 target file sizes.
func (cfg Config) BuildTargetFileSizes() [NumLevels]int64 {
	var sizes [NumLevels]int64
	for i := range sizes {
		sizes[i] = cfg.TargetFileSize
	}

	return sizes
}
