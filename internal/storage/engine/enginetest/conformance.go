// Package kvtest is the engine conformance suite: every engine.DB implementation
// must pass it with identical results, so that stores written against Pebble
// semantics behave the same on another engine.
package enginetest

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/engine"
)

// Opener opens (creating if needed) a database at dir.
type Opener func(dir string, o engine.Options) (engine.DB, error)

// Run executes the conformance suite against open.
func Run(t *testing.T, open Opener) {
	t.Helper()

	cases := map[string]func(*testing.T, Opener){
		"GetSetNotFound":         testGetSetNotFound,
		"BatchAtomicAndCount":    testBatchAtomicAndCount,
		"BatchCloseDiscards":     testBatchCloseDiscards,
		"Deletes":                testDeletes,
		"IteratorBounds":         testIteratorBounds,
		"IteratorSeeks":          testIteratorSeeks,
		"SeekPrefixGE":           testSeekPrefixGE,
		"LongScanMixedDirection": testLongScanMixedDirection,
		"IteratorEmpty":          testIteratorEmpty,
		"SnapshotIsolation":      testSnapshotIsolation,
		"CheckpointIsOpenable":   testCheckpointIsOpenable,
		"CheckpointRefusesDir":   testCheckpointRefusesExistingDir,
		"PersistsAcrossReopen":   testPersistsAcrossReopen,
		"FlushCompactAndStats":   testFlushCompactAndStats,
		"PrefixBloomOption":      testPrefixBloomOption,
		"ErrorIfExistsReadOnly":  testErrorIfExistsAndReadOnly,
	}
	for name, fn := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fn(t, open)
		})
	}
}

func mustOpen(t *testing.T, open Opener, dir string, o engine.Options) engine.DB {
	t.Helper()

	db, err := open(dir, o)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	return db
}

func get(t *testing.T, r engine.Getter, key string) (string, bool) {
	t.Helper()

	v, closer, err := r.Get([]byte(key))
	if errors.Is(err, engine.ErrNotFound) {
		require.Nil(t, closer)

		return "", false
	}
	require.NoError(t, err)
	require.NotNil(t, closer)
	out := string(v)
	require.NoError(t, closer.Close())

	return out, true
}

func put(t *testing.T, db engine.DB, kvs ...string) {
	t.Helper()

	require.Zero(t, len(kvs)%2)
	b := db.NewBatch()
	for i := 0; i < len(kvs); i += 2 {
		require.NoError(t, b.Set([]byte(kvs[i]), []byte(kvs[i+1])))
	}
	require.NoError(t, b.Commit(false))
	require.NoError(t, b.Close())
}

func collect(t *testing.T, it engine.Iterator, forward bool) []string {
	t.Helper()

	var out []string
	ok := it.First()
	if !forward {
		ok = it.Last()
	}
	for ; ok; ok = step(it, forward) {
		require.True(t, it.Valid())
		// One accessor call per position: Pebble's invariants build (on under
		// -race) poisons the previous Value() buffer on the next accessor call.
		v, err := it.ValueAndErr()
		require.NoError(t, err)
		out = append(out, string(it.Key())+"="+string(v))
	}
	require.False(t, it.Valid())
	require.NoError(t, it.Error())

	return out
}

func step(it engine.Iterator, forward bool) bool {
	if forward {
		return it.Next()
	}

	return it.Prev()
}

func testGetSetNotFound(t *testing.T, open Opener) {
	db := mustOpen(t, open, t.TempDir(), engine.Options{})

	_, found := get(t, db, "missing")
	require.False(t, found)

	require.NoError(t, db.Set([]byte("a"), []byte("1"), false))
	require.NoError(t, db.Set([]byte("b"), []byte(""), true))
	v, found := get(t, db, "a")
	require.True(t, found)
	require.Equal(t, "1", v)
	v, found = get(t, db, "b")
	require.True(t, found)
	require.Equal(t, "", v)

	// Value bytes must stay valid until the closer is closed, even after a
	// concurrent overwrite.
	raw, closer, err := db.Get([]byte("a"))
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("a"), []byte("2"), false))
	require.Equal(t, "1", string(raw))
	require.NoError(t, closer.Close())
	require.NoError(t, db.SyncWAL())
}

func testBatchAtomicAndCount(t *testing.T, open Opener) {
	db := mustOpen(t, open, t.TempDir(), engine.Options{})

	b := db.NewBatch()
	require.Zero(t, b.Count())
	require.NoError(t, b.Set([]byte("k1"), []byte("v1")))
	require.NoError(t, b.Set([]byte("k2"), []byte("v2")))
	require.NoError(t, b.Delete([]byte("k3")))
	require.Equal(t, uint32(3), b.Count())
	require.Positive(t, b.Len())

	// Nothing visible before commit.
	_, found := get(t, db, "k1")
	require.False(t, found)

	require.NoError(t, b.Commit(true))
	require.NoError(t, b.Close())
	v, found := get(t, db, "k2")
	require.True(t, found)
	require.Equal(t, "v2", v)
}

func testBatchCloseDiscards(t *testing.T, open Opener) {
	db := mustOpen(t, open, t.TempDir(), engine.Options{})

	b := db.NewBatch()
	require.NoError(t, b.Set([]byte("k"), []byte("v")))
	require.NoError(t, b.Close())
	_, found := get(t, db, "k")
	require.False(t, found)
}

func testDeletes(t *testing.T, open Opener) {
	db := mustOpen(t, open, t.TempDir(), engine.Options{})
	put(t, db, "a", "1", "b", "2", "c", "3", "d", "4", "e", "5")

	b := db.NewBatch()
	require.NoError(t, b.Delete([]byte("a")))
	require.NoError(t, b.SingleDelete([]byte("b")))
	require.NoError(t, b.DeleteRange([]byte("c"), []byte("e"))) // [c, e)
	require.NoError(t, b.Commit(false))
	require.NoError(t, b.Close())

	it, err := db.NewIter(nil)
	require.NoError(t, err)
	require.Equal(t, []string{"e=5"}, collect(t, it, true))
	require.NoError(t, it.Close())
}

func testIteratorBounds(t *testing.T, open Opener) {
	db := mustOpen(t, open, t.TempDir(), engine.Options{})
	put(t, db, "a", "1", "b", "2", "c", "3", "d", "4", "e", "5")

	for _, tc := range []struct {
		lower, upper string
		want         []string
	}{
		{"", "", []string{"a=1", "b=2", "c=3", "d=4", "e=5"}},
		{"b", "", []string{"b=2", "c=3", "d=4", "e=5"}},
		{"", "d", []string{"a=1", "b=2", "c=3"}},
		{"b", "d", []string{"b=2", "c=3"}},
		{"bb", "cc", []string{"c=3"}},
		{"x", "z", nil},
	} {
		opts := &engine.IterOptions{}
		if tc.lower != "" {
			opts.LowerBound = []byte(tc.lower)
		}
		if tc.upper != "" {
			opts.UpperBound = []byte(tc.upper)
		}

		it, err := db.NewIter(opts)
		require.NoError(t, err)
		require.Equal(t, tc.want, collect(t, it, true), "forward [%s,%s)", tc.lower, tc.upper)
		require.Equal(t, reverse(tc.want), collect(t, it, false), "reverse [%s,%s)", tc.lower, tc.upper)
		require.NoError(t, it.Close())
	}

	// Bounds are respected by seeks too.
	it, err := db.NewIter(&engine.IterOptions{LowerBound: []byte("b"), UpperBound: []byte("d")})
	require.NoError(t, err)
	require.False(t, it.SeekGE([]byte("d")))
	require.True(t, it.SeekGE([]byte("a")))
	require.Equal(t, "b", string(it.Key()))
	require.False(t, it.SeekLT([]byte("b")))
	require.True(t, it.SeekLT([]byte("z")))
	require.Equal(t, "c", string(it.Key()))
	require.NoError(t, it.Close())
}

func reverse(in []string) []string {
	if in == nil {
		return nil
	}
	out := make([]string, len(in))
	for i, v := range in {
		out[len(in)-1-i] = v
	}

	return out
}

func testIteratorSeeks(t *testing.T, open Opener) {
	db := mustOpen(t, open, t.TempDir(), engine.Options{})
	put(t, db, "b", "2", "d", "4", "f", "6")

	it, err := db.NewIter(nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()

	require.True(t, it.SeekGE([]byte("d")))
	require.Equal(t, "d", string(it.Key()))
	require.True(t, it.SeekGE([]byte("c")))
	require.Equal(t, "d", string(it.Key()))
	// nil rather than []byte(""): Pebble's invariants build (enabled under
	// -race) indexes key[0] on a non-nil empty seek key.
	require.True(t, it.SeekGE(nil))
	require.Equal(t, "b", string(it.Key()))
	require.False(t, it.SeekGE([]byte("g")))

	// SeekLT is strictly less than.
	require.True(t, it.SeekLT([]byte("d")))
	require.Equal(t, "b", string(it.Key()))
	require.True(t, it.SeekLT([]byte("e")))
	require.Equal(t, "d", string(it.Key()))
	require.True(t, it.SeekLT([]byte("z")))
	require.Equal(t, "f", string(it.Key()))
	require.False(t, it.SeekLT([]byte("b")))
	require.False(t, it.SeekLT(nil))

	// Walking off either end invalidates; Prev after First too.
	require.True(t, it.First())
	require.False(t, it.Prev())
	require.False(t, it.Valid())
	require.True(t, it.Last())
	require.False(t, it.Next())
	require.False(t, it.Valid())

	// Key/Value stay valid until the next positioning call.
	require.True(t, it.SeekGE([]byte("d")))
	k, v := it.Key(), it.Value()
	require.NoError(t, db.Set([]byte("d"), []byte("changed"), false))
	require.Equal(t, "d", string(k))
	require.Equal(t, "4", string(v))
}

func testIteratorEmpty(t *testing.T, open Opener) {
	db := mustOpen(t, open, t.TempDir(), engine.Options{})

	it, err := db.NewIter(nil)
	require.NoError(t, err)
	require.False(t, it.First())
	require.False(t, it.Last())
	require.False(t, it.SeekGE([]byte("a")))
	require.False(t, it.SeekLT([]byte("a")))
	require.False(t, it.Valid())
	require.NoError(t, it.Error())
	require.NoError(t, it.Close())
}

func testSeekPrefixGE(t *testing.T, open Opener) {
	testSeekPrefixGEImpl(t, open)
}

// testLongScanMixedDirection exercises read-ahead implementations: a scan
// spanning many refill boundaries, direction changes mid-scan, seeks that
// land inside a buffered window, and a value larger than any read-ahead
// buffer.
func testLongScanMixedDirection(t *testing.T, open Opener) {
	db := mustOpen(t, open, t.TempDir(), engine.Options{})
	const n = 5000
	value := make([]byte, 100)
	b := db.NewBatch()
	for i := range n {
		for j := range value {
			value[j] = byte(i + j)
		}
		require.NoError(t, b.Set(fmt.Appendf(nil, "k%06d", i), value))
	}
	big := make([]byte, 300<<10)
	require.NoError(t, b.Set([]byte("k999999"), big))
	require.NoError(t, b.Commit(false))
	require.NoError(t, b.Close())
	require.NoError(t, db.Flush())

	keyAt := func(i int) string { return fmt.Sprintf("k%06d", i) }
	it, err := db.NewIter(nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, it.Close()) }()

	// Full forward scan, values checked, then the oversized value.
	i := 0
	for ok := it.First(); ok; ok = it.Next() {
		if i == n {
			require.Equal(t, "k999999", string(it.Key()))
			require.Len(t, it.Value(), len(big))
			i++

			continue
		}
		require.Equal(t, keyAt(i), string(it.Key()))
		require.Equal(t, byte(i), it.Value()[0])
		i++
	}
	require.Equal(t, n+1, i)
	require.NoError(t, it.Error())

	// Forward 1234 then back 100: exact positions.
	require.True(t, it.First())
	for range 1234 {
		require.True(t, it.Next())
	}
	require.Equal(t, keyAt(1234), string(it.Key()))
	for k := 1; k <= 100; k++ {
		require.True(t, it.Prev())
		require.Equal(t, keyAt(1234-k), string(it.Key()))
	}
	// Forward again after backing up.
	require.True(t, it.Next())
	require.Equal(t, keyAt(1135), string(it.Key()))

	// Seek inside, step back, step forward twice.
	require.True(t, it.SeekGE([]byte(keyAt(2500))))
	require.True(t, it.Prev())
	require.Equal(t, keyAt(2499), string(it.Key()))
	require.True(t, it.Next())
	require.True(t, it.Next())
	require.Equal(t, keyAt(2501), string(it.Key()))

	// Pebble semantics: running off either end leaves a position "before
	// first" / "after last", so the opposite step re-enters at the edge.
	require.True(t, it.First())
	require.False(t, it.Prev())
	require.True(t, it.Next())
	require.Equal(t, keyAt(0), string(it.Key()))
	require.True(t, it.Last())
	require.False(t, it.Next())
	require.True(t, it.Prev())
	require.Equal(t, "k999999", string(it.Key()))
	require.False(t, it.SeekGE([]byte("z")))
	require.True(t, it.Prev())
	require.Equal(t, "k999999", string(it.Key()))
	require.False(t, it.SeekLT([]byte("a")))
	require.True(t, it.Next())
	require.Equal(t, keyAt(0), string(it.Key()))

	// Bounds hold across refills.
	bit, err := db.NewIter(&engine.IterOptions{LowerBound: []byte(keyAt(100)), UpperBound: []byte(keyAt(4100))})
	require.NoError(t, err)
	cnt := 0
	for ok := bit.First(); ok; ok = bit.Next() {
		cnt++
	}
	require.Equal(t, 4000, cnt)
	require.True(t, bit.Last())
	require.Equal(t, keyAt(4099), string(bit.Key()))
	require.NoError(t, bit.Close())
}

func testSeekPrefixGEImpl(t *testing.T, open Opener) {
	// Whole-key prefix (no extractor): SeekPrefixGE is a point probe.
	db := mustOpen(t, open, t.TempDir(), engine.Options{})
	put(t, db, "a", "1", "b", "2", "c", "3")
	it, err := db.NewIter(nil)
	require.NoError(t, err)
	require.True(t, it.SeekPrefixGE([]byte("b")))
	require.Equal(t, "b", string(it.Key()))
	require.False(t, it.Next())
	require.False(t, it.SeekPrefixGE([]byte("bb")))
	require.True(t, it.SeekGE([]byte("b"))) // absolute seek lifts the confinement
	require.True(t, it.Next())
	require.Equal(t, "c", string(it.Key()))
	require.NoError(t, it.Close())

	// Fixed 4-byte prefix: confined to the prefix, across files.
	pdb := mustOpen(t, open, t.TempDir(), engine.Options{BloomBitsPerKey: 10, FixedPrefixLen: 4})
	put(t, pdb, "aaaa1", "1", "aaaa2", "2")
	require.NoError(t, pdb.Flush())
	put(t, pdb, "aaaa3", "3", "bbbb1", "4")
	it, err = pdb.NewIter(nil)
	require.NoError(t, err)
	var got []string
	for ok := it.SeekPrefixGE([]byte("aaaa2")); ok; ok = it.Next() {
		got = append(got, string(it.Key()))
	}
	require.Equal(t, []string{"aaaa2", "aaaa3"}, got)
	require.False(t, it.SeekPrefixGE([]byte("cccc")))
	require.NoError(t, it.Close())
}

func testSnapshotIsolation(t *testing.T, open Opener) {
	db := mustOpen(t, open, t.TempDir(), engine.Options{})
	put(t, db, "a", "1", "b", "2")

	snap := db.NewSnapshot()
	put(t, db, "a", "1'", "c", "3")

	v, found := get(t, snap, "a")
	require.True(t, found)
	require.Equal(t, "1", v)
	_, found = get(t, snap, "c")
	require.False(t, found)

	it, err := snap.NewIter(nil)
	require.NoError(t, err)
	require.Equal(t, []string{"a=1", "b=2"}, collect(t, it, true))
	require.NoError(t, it.Close())
	require.NoError(t, snap.Close())

	v, found = get(t, db, "a")
	require.True(t, found)
	require.Equal(t, "1'", v)
}

func testCheckpointIsOpenable(t *testing.T, open Opener) {
	root := t.TempDir()
	db := mustOpen(t, open, filepath.Join(root, "live"), engine.Options{})
	put(t, db, "a", "1", "b", "2")
	require.NoError(t, db.Flush())
	put(t, db, "c", "3") // still in the memtable / WAL when the checkpoint is taken

	cpDir := filepath.Join(root, "cp")
	require.NoError(t, db.Checkpoint(cpDir))

	put(t, db, "d", "4") // after the checkpoint, must not appear in it

	cp := mustOpen(t, open, cpDir, engine.Options{})
	it, err := cp.NewIter(nil)
	require.NoError(t, err)
	require.Equal(t, []string{"a=1", "b=2", "c=3"}, collect(t, it, true))
	require.NoError(t, it.Close())
}

func testCheckpointRefusesExistingDir(t *testing.T, open Opener) {
	root := t.TempDir()
	db := mustOpen(t, open, filepath.Join(root, "live"), engine.Options{})
	existing := filepath.Join(root, "existing")
	require.NoError(t, os.MkdirAll(existing, 0o750))
	require.Error(t, db.Checkpoint(existing))
}

func testPersistsAcrossReopen(t *testing.T, open Opener) {
	dir := filepath.Join(t.TempDir(), "nested", "live") // parents must be created like Pebble does
	db, err := open(dir, engine.Options{})
	require.NoError(t, err)
	put(t, db, "a", "1")
	require.NoError(t, db.Set([]byte("b"), []byte("2"), true))
	require.NoError(t, db.Close())

	db = mustOpen(t, open, dir, engine.Options{})
	it, err := db.NewIter(nil)
	require.NoError(t, err)
	require.Equal(t, []string{"a=1", "b=2"}, collect(t, it, true))
	require.NoError(t, it.Close())
}

func testFlushCompactAndStats(t *testing.T, open Opener) {
	db := mustOpen(t, open, t.TempDir(), engine.Options{CacheBytes: 8 << 20, MemTableBytes: 4 << 20})
	for i := range 3 {
		var kvs []string
		for k := range 500 {
			kvs = append(kvs, fmt.Sprintf("k%05d", k), fmt.Sprintf("v%d-%d", i, k))
		}
		put(t, db, kvs...)
		require.NoError(t, db.Flush())
	}

	before := db.Stats()
	require.NotEmpty(t, before.Levels)
	require.Positive(t, before.DiskSpaceBytes)
	files := int64(0)
	for _, l := range before.Levels {
		files += l.Files
	}
	require.Positive(t, files) // background compaction may already have merged some

	require.NoError(t, db.Compact([]byte("k"), []byte("l")))
	after := db.Stats()
	files = 0
	for _, l := range after.Levels {
		files += l.Files
	}
	require.Equal(t, int64(1), files)

	v, found := get(t, db, "k00042")
	require.True(t, found)
	require.Equal(t, "v2-42", v)
}

func testPrefixBloomOption(t *testing.T, open Opener) {
	db := mustOpen(t, open, t.TempDir(), engine.Options{BloomBitsPerKey: 10, FixedPrefixLen: 4})
	put(t, db, "aaaa1", "1", "aaaa2", "2", "bbbb1", "3", "x", "short")
	require.NoError(t, db.Flush())

	it, err := db.NewIter(&engine.IterOptions{LowerBound: []byte("aaaa"), UpperBound: []byte("aaab")})
	require.NoError(t, err)
	require.Equal(t, []string{"aaaa1=1", "aaaa2=2"}, collect(t, it, true))
	require.NoError(t, it.Close())

	v, found := get(t, db, "x")
	require.True(t, found)
	require.Equal(t, "short", v)
}

func testErrorIfExistsAndReadOnly(t *testing.T, open Opener) {
	dir := t.TempDir()
	db, err := open(dir, engine.Options{})
	require.NoError(t, err)
	put(t, db, "a", "1")
	require.NoError(t, db.Close())

	_, err = open(dir, engine.Options{ErrorIfExists: true})
	require.Error(t, err)

	ro := mustOpen(t, open, dir, engine.Options{ReadOnly: true})
	v, found := get(t, ro, "a")
	require.True(t, found)
	require.Equal(t, "1", v)
	require.Error(t, ro.Set([]byte("b"), []byte("2"), false))
}
