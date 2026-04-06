package pebblecfg

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/sstable/block"
)

// NumLevels is the number of Pebble LSM levels.
const NumLevels = 7

// Compression represents a named Pebble block compression profile.
type Compression int

const (
	DefaultCompression Compression = iota
	NoCompression
	SnappyCompression
	ZstdCompression
	FastestCompression
	FastCompression
	BalancedCompression
	GoodCompression
)

var compressionNames = map[Compression]string{
	DefaultCompression:  "default",
	NoCompression:       "none",
	SnappyCompression:   "snappy",
	ZstdCompression:     "zstd",
	FastestCompression:  "fastest",
	FastCompression:     "fast",
	BalancedCompression: "balanced",
	GoodCompression:     "good",
}

var compressionValues = func() map[string]Compression {
	m := make(map[string]Compression, len(compressionNames))
	for k, v := range compressionNames {
		m[v] = k
	}

	return m
}()

func (c Compression) String() string {
	if name, ok := compressionNames[c]; ok {
		return name
	}

	return fmt.Sprintf("unknown(%d)", int(c))
}

// ToPebble converts to a *block.CompressionProfile.
func (c Compression) ToPebble() *block.CompressionProfile {
	switch c {
	case NoCompression:
		return block.NoCompression
	case SnappyCompression:
		return block.SnappyCompression
	case ZstdCompression:
		return block.ZstdCompression
	case FastestCompression:
		return block.FastestCompression
	case FastCompression:
		return block.FastCompression
	case BalancedCompression:
		return block.BalancedCompression
	case GoodCompression:
		return block.GoodCompression
	default:
		return block.DefaultCompression
	}
}

// ParseCompression parses a compression name (case-insensitive).
func ParseCompression(s string) (Compression, error) {
	if c, ok := compressionValues[strings.ToLower(strings.TrimSpace(s))]; ok {
		return c, nil
	}

	return DefaultCompression, fmt.Errorf(
		"unknown compression %q (valid: none, snappy, zstd, fastest, fast, balanced, good, default)", s,
	)
}

// LevelCompression holds the compression profile for each of the 7 Pebble levels (L0–L6).
type LevelCompression [NumLevels]Compression

// DefaultLevelCompression returns the default per-level compression:
// L0–L3 use Fastest (minimal CPU on hot levels),
// L4–L5 use Fast (good ratio/CPU trade-off),
// L6 uses Balanced (best ratio for cold data without full zstd cost).
func DefaultLevelCompression() LevelCompression {
	return LevelCompression{
		FastestCompression,  // L0
		FastestCompression,  // L1
		FastestCompression,  // L2
		FastestCompression,  // L3
		FastCompression,     // L4
		FastCompression,     // L5
		BalancedCompression, // L6
	}
}

// String returns a comma-separated representation (e.g. "snappy,snappy,snappy,snappy,zstd,zstd,zstd").
func (lc LevelCompression) String() string {
	parts := make([]string, NumLevels)
	for i, c := range lc {
		parts[i] = c.String()
	}

	return strings.Join(parts, ",")
}

// ParseLevelCompression parses a comma-separated list of 7 compression names.
func ParseLevelCompression(s string) (LevelCompression, error) {
	parts := strings.Split(s, ",")
	if len(parts) != NumLevels {
		return LevelCompression{}, fmt.Errorf("expected %d comma-separated compression values, got %d", NumLevels, len(parts))
	}

	var lc LevelCompression
	for i, p := range parts {
		c, err := ParseCompression(p)
		if err != nil {
			return LevelCompression{}, fmt.Errorf("level %d: %w", i, err)
		}
		lc[i] = c
	}

	return lc, nil
}

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
