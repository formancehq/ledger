package pebblecfg

import (
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestParseCompression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  Compression
	}{
		{"none", NoCompression},
		{"snappy", SnappyCompression},
		{"zstd", ZstdCompression},
		{"default", DefaultCompression},
		{"SNAPPY", SnappyCompression},
		{" Zstd ", ZstdCompression},
	}
	for _, tt := range tests {
		c, err := ParseCompression(tt.input)
		require.NoError(t, err, tt.input)
		require.Equal(t, tt.want, c, tt.input)
	}

	_, err := ParseCompression("lz4")
	require.Error(t, err)
}

func TestParseLevelCompression(t *testing.T) {
	t.Parallel()

	lc, err := ParseLevelCompression("none,snappy,snappy,snappy,zstd,zstd,zstd")
	require.NoError(t, err)
	require.Equal(t, NoCompression, lc[0])
	require.Equal(t, SnappyCompression, lc[1])
	require.Equal(t, ZstdCompression, lc[6])

	_, err = ParseLevelCompression("snappy,snappy")
	require.Error(t, err)

	_, err = ParseLevelCompression("snappy,snappy,snappy,snappy,invalid,zstd,zstd")
	require.Error(t, err)
}

func TestLevelCompressionString(t *testing.T) {
	t.Parallel()

	lc := DefaultLevelCompression()
	require.Equal(t, "snappy,snappy,snappy,snappy,zstd,zstd,zstd", lc.String())
}

func TestCompressionToPebble(t *testing.T) {
	t.Parallel()

	require.Equal(t, pebble.NoCompression, NoCompression.ToPebble())
	require.Equal(t, pebble.SnappyCompression, SnappyCompression.ToPebble())
	require.Equal(t, pebble.ZstdCompression, ZstdCompression.ToPebble())
	require.Equal(t, pebble.DefaultCompression, DefaultCompression.ToPebble())
}

func TestBuildLevels(t *testing.T) {
	t.Parallel()

	cfg := Config{
		TargetFileSize: 64 << 20,
		Compression:    DefaultLevelCompression(),
	}
	levels := cfg.BuildLevels()
	require.Len(t, levels, NumLevels)
	require.Equal(t, pebble.SnappyCompression, levels[0].Compression)
	require.Equal(t, pebble.SnappyCompression, levels[3].Compression)
	require.Equal(t, pebble.ZstdCompression, levels[4].Compression)
	require.Equal(t, pebble.ZstdCompression, levels[6].Compression)
}
