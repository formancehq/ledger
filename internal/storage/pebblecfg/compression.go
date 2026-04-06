package pebblecfg

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/pebble"
)

// Compression represents a Pebble block compression algorithm.
type Compression int

const (
	DefaultCompression Compression = iota
	NoCompression
	SnappyCompression
	ZstdCompression
)

var compressionNames = map[Compression]string{
	DefaultCompression: "default",
	NoCompression:      "none",
	SnappyCompression:  "snappy",
	ZstdCompression:    "zstd",
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

// ToPebble converts to the pebble.Compression type.
func (c Compression) ToPebble() pebble.Compression {
	switch c {
	case NoCompression:
		return pebble.NoCompression
	case SnappyCompression:
		return pebble.SnappyCompression
	case ZstdCompression:
		return pebble.ZstdCompression
	default:
		return pebble.DefaultCompression
	}
}

// ParseCompression parses a compression name (case-insensitive).
func ParseCompression(s string) (Compression, error) {
	if c, ok := compressionValues[strings.ToLower(strings.TrimSpace(s))]; ok {
		return c, nil
	}
	return DefaultCompression, fmt.Errorf("unknown compression %q (valid: none, snappy, zstd, default)", s)
}

// LevelCompression holds the compression algorithm for each of the 7 Pebble levels (L0–L6).
type LevelCompression [NumLevels]Compression

// NumLevels is the number of Pebble LSM levels.
const NumLevels = 7

// DefaultLevelCompression returns the default per-level compression:
// L0–L3 use Snappy, L4–L6 use Zstd.
func DefaultLevelCompression() LevelCompression {
	var lc LevelCompression
	for i := 0; i < 4; i++ {
		lc[i] = SnappyCompression
	}
	for i := 4; i < NumLevels; i++ {
		lc[i] = ZstdCompression
	}
	return lc
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
