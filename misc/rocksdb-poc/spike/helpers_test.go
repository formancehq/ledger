package spike

import (
	"path/filepath"
	"regexp"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
)

// openOptions returns options close to what the ledger read store would use:
// block-based table with a 10-bit bloom, whole-key filtering, statistics on,
// auto compaction off so tests control the LSM shape.
//
// Finding: rocksdb_options_set_merge_operator and
// rocksdb_options_set_prefix_extractor take ownership of the C object
// (std::shared_ptr), and grocksdb's Options.Destroy deletes it again — a
// double free that segfaults. grocksdb's own test-suite never destroys
// options carrying either; neither do we when one is set.
func openOptions(t *testing.T, tr grocksdb.SliceTransform, mo grocksdb.MergeOperator) *grocksdb.Options {
	t.Helper()

	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetFilterPolicy(grocksdb.NewBloomFilter(10))
	bbto.SetWholeKeyFiltering(true)

	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetBlockBasedTableFactory(bbto)
	opts.EnableStatistics()
	opts.SetDisableAutoCompactions(true)
	if tr != nil {
		opts.SetPrefixExtractor(tr)
		opts.SetMemTablePrefixBloomSizeRatio(0.1)
	}
	if mo != nil {
		opts.SetMergeOperator(mo)
	}
	if tr == nil && mo == nil {
		t.Cleanup(opts.Destroy)
	}

	return opts
}

func openDB(t *testing.T, opts *grocksdb.Options) *grocksdb.DB {
	t.Helper()

	db, err := grocksdb.OpenDb(opts, filepath.Join(t.TempDir(), "db"))
	require.NoError(t, err)
	t.Cleanup(db.Close)

	return db
}

func flush(t *testing.T, db *grocksdb.DB) {
	t.Helper()

	fo := grocksdb.NewDefaultFlushOptions()
	defer fo.Destroy()
	require.NoError(t, db.Flush(fo))
}

// tickerByName reads a ticker from the statistics dump by its RocksDB name.
//
// Finding: grocksdb.TickerType is an iota-based mirror of the C enum and
// drifts between RocksDB versions; with 11.0.4 the BLOOM_FILTER_PREFIX_*
// constants read an unrelated counter. Names are stable, enum offsets are
// not.
func tickerByName(t *testing.T, opts *grocksdb.Options, name string) uint64 {
	t.Helper()

	re := regexp.MustCompile(regexp.QuoteMeta(name) + ` COUNT : (\d+)`)
	m := re.FindStringSubmatch(opts.GetStatisticsString())
	require.NotNil(t, m, "ticker %s not found", name)
	v, err := strconv.ParseUint(m[1], 10, 64)
	require.NoError(t, err)

	return v
}

// countingMergeOperator records how many times RocksDB invoked each entry point.
type countingMergeOperator struct {
	inner   ReplayMergeOperator
	full    atomic.Int64
	partial atomic.Int64
}

func (c *countingMergeOperator) Name() string { return c.inner.Name() }

func (c *countingMergeOperator) FullMerge(key, existing []byte, operands [][]byte) ([]byte, bool) {
	c.full.Add(1)

	return c.inner.FullMerge(key, existing, operands)
}

func (c *countingMergeOperator) PartialMerge(key, left, right []byte) ([]byte, bool) {
	c.partial.Add(1)

	return c.inner.PartialMerge(key, left, right)
}
