package rocksdbcfg

import (
	"testing"

	"github.com/linxGnu/grocksdb"
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
		{"fastest", FastestCompression},
		{"fast", FastCompression},
		{"balanced", BalancedCompression},
		{"good", GoodCompression},
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
	require.Equal(t, "fastest,fastest,fastest,fastest,fast,fast,balanced", lc.String())
}

func TestCompressionToRocksDB(t *testing.T) {
	t.Parallel()

	require.Equal(t, grocksdb.NoCompression, NoCompression.ToRocksDB())
	require.Equal(t, grocksdb.SnappyCompression, SnappyCompression.ToRocksDB())
	require.Equal(t, grocksdb.ZSTDCompression, ZstdCompression.ToRocksDB())
	require.Equal(t, grocksdb.SnappyCompression, DefaultCompression.ToRocksDB())
	require.Equal(t, grocksdb.SnappyCompression, FastestCompression.ToRocksDB())
	require.Equal(t, grocksdb.SnappyCompression, FastCompression.ToRocksDB())
	require.Equal(t, grocksdb.ZSTDCompression, BalancedCompression.ToRocksDB())
	require.Equal(t, grocksdb.ZSTDCompression, GoodCompression.ToRocksDB())
}

func TestRocksDBCompression(t *testing.T) {
	t.Parallel()

	cfg := Config{
		TargetFileSize: 64 << 20,
		Compression:    DefaultLevelCompression(),
	}
	levels := cfg.RocksDBCompression()
	require.Equal(t, grocksdb.SnappyCompression, levels[0])
	require.Equal(t, grocksdb.SnappyCompression, levels[3])
	require.Equal(t, grocksdb.SnappyCompression, levels[4])
	require.Equal(t, grocksdb.ZSTDCompression, levels[6])
}
