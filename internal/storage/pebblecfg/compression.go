package pebblecfg

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble/v2/sstable/block"
)

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

// NumLevels is the number of Pebble LSM levels.
const NumLevels = 7

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
