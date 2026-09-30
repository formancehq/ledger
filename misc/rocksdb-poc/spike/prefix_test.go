package spike

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/linxGnu/grocksdb"
	"github.com/stretchr/testify/require"
)

const (
	testLedgers       = 16
	testKeysPerLedger = 200
	testPrefix        = 0x10
)

// writeLedgers writes testLedgers ledgers, one SST file per ledger, so that a
// scan on a single ledger has 15 foreign files the prefix bloom can skip.
func writeLedgers(t *testing.T, db *grocksdb.DB) {
	t.Helper()

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	for l := range testLedgers {
		batch := grocksdb.NewWriteBatch()
		for k := range testKeysPerLedger {
			key := LedgerKey(testPrefix, fmt.Sprintf("ledger-%02d", l), []byte(fmt.Sprintf("k%06d", k)))
			batch.Put(key, []byte("v"))
		}
		// One internal singleton key per file, shorter than the ledger prefix.
		batch.Put([]byte{PrefixInternal, byte(l)}, []byte("internal"))
		require.NoError(t, db.Write(wo, batch))
		batch.Destroy()
		flush(t, db)
	}
}

func scanLedger(t *testing.T, db *grocksdb.DB, ledger string) int {
	t.Helper()

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	ro.SetPrefixSameAsStart(true)

	it := db.NewIterator(ro)
	defer it.Close()

	n := 0
	prefix := LedgerKey(testPrefix, ledger, nil)
	for it.Seek(prefix); it.Valid(); it.Next() {
		k := it.Key()
		same := bytes.Equal(prefix, k.Data()[:LedgerScopedPrefixLen])
		k.Free()
		if !same {
			// Without a prefix extractor prefix_same_as_start is ignored and
			// the iterator runs into the next ledger.
			break
		}
		n++
	}
	require.NoError(t, it.Err())

	return n
}

func testPrefixBloom(t *testing.T, tr grocksdb.SliceTransform) {
	t.Helper()

	opts := openOptions(t, tr, nil)
	db := openDB(t, opts)
	writeLedgers(t, db)

	require.Equal(t, testKeysPerLedger, scanLedger(t, db, "ledger-07"))
	require.Equal(t, testKeysPerLedger, scanLedger(t, db, "ledger-00"))
	require.Equal(t, 0, scanLedger(t, db, "ledger-99"))

	// RocksDB >= 7 reports prefix-bloom seek skips through the *.seek.filtered
	// tickers (the legacy bloom.filter.prefix.* counters stay at zero).
	filtered := seekFiltered(t, opts)
	matched := tickerByName(t, opts, "rocksdb.non.last.level.seek.filter.match")
	t.Logf("prefix bloom on seek: filtered=%d matched=%d", filtered, matched)
	// Three scans over 16 files: every foreign file must be skipped by the
	// prefix bloom (15 + 15 + 16 = 46), and only the two home files opened.
	require.Equal(t, uint64(3*testLedgers-2), filtered)
	require.Equal(t, uint64(2), matched)

	// Internal singleton keys stay reachable by point lookup (whole-key bloom).
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	v, err := db.Get(ro, []byte{PrefixInternal, 3})
	require.NoError(t, err)
	require.Equal(t, "internal", string(v.Data()))
	v.Free()
}

func TestFixedPrefixExtractorUsesBloomPerLedger(t *testing.T) {
	testPrefixBloom(t, NewFixedLedgerPrefixExtractor())
}

func TestSplitPrefixExtractorUsesBloomPerLedger(t *testing.T) {
	testPrefixBloom(t, SplitPrefixExtractor{})
}

func TestNoPrefixExtractorScansEveryFile(t *testing.T) {
	opts := openOptions(t, nil, nil)
	db := openDB(t, opts)
	writeLedgers(t, db)

	require.Equal(t, testKeysPerLedger, scanLedger(t, db, "ledger-07"))
	require.Zero(t, seekFiltered(t, opts))
}

func seekFiltered(t *testing.T, opts *grocksdb.Options) uint64 {
	t.Helper()

	return tickerByName(t, opts, "rocksdb.non.last.level.seek.filtered") +
		tickerByName(t, opts, "rocksdb.last.level.seek.filtered")
}

// BenchmarkPrefixExtractorWrite measures the ingestion cost of the prefix
// extractor: the native fixed transform runs in C++, the Split port crosses
// the cgo boundary for every key at memtable insert and SST build time.
func BenchmarkPrefixExtractorWrite(b *testing.B) {
	for name, tr := range map[string]func() grocksdb.SliceTransform{
		"fixed": NewFixedLedgerPrefixExtractor,
		"split": func() grocksdb.SliceTransform { return SplitPrefixExtractor{} },
	} {
		b.Run(name, func(b *testing.B) {
			bbto := grocksdb.NewDefaultBlockBasedTableOptions()
			bbto.SetFilterPolicy(grocksdb.NewBloomFilter(10))
			opts := grocksdb.NewDefaultOptions()
			opts.SetCreateIfMissing(true)
			opts.SetBlockBasedTableFactory(bbto)
			opts.SetPrefixExtractor(tr())
			opts.SetMemTablePrefixBloomSizeRatio(0.1)
			// No opts.Destroy(): see openOptions on prefix-extractor ownership.
			db, err := grocksdb.OpenDb(opts, b.TempDir())
			require.NoError(b, err)
			defer db.Close()
			wo := grocksdb.NewDefaultWriteOptions()
			defer wo.Destroy()
			fo := grocksdb.NewDefaultFlushOptions()
			defer fo.Destroy()

			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				key := LedgerKey(testPrefix, fmt.Sprintf("ledger-%02d", i%testLedgers), []byte(fmt.Sprintf("k%09d", i)))
				require.NoError(b, db.Put(wo, key, key))
				if i%50_000 == 49_999 {
					require.NoError(b, db.Flush(fo))
				}
			}
		})
	}
}
