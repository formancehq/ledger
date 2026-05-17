package dal

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := NewStore(t.TempDir(), logger, meter, DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

func TestBatch_CommitAndCancel(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	batch := s.NewBatch()

	require.NoError(t, batch.SetBytes([]byte("key1"), []byte("val1")))
	require.NoError(t, batch.Commit())

	// Verify the data was committed
	val, closer, err := s.Get([]byte("key1"))
	require.NoError(t, err)
	require.Equal(t, []byte("val1"), val)
	require.NoError(t, closer.Close())
}

func TestBatch_CancelBeforeCommit(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	batch := s.NewBatch()

	require.NoError(t, batch.SetBytes([]byte("key1"), []byte("val1")))
	require.NoError(t, batch.Cancel())

	// Data should NOT be committed
	_, _, err := s.Get([]byte("key1"))
	require.Error(t, err)
}

func TestBatch_CancelAfterCommit(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	batch := s.NewBatch()

	require.NoError(t, batch.SetBytes([]byte("key1"), []byte("val1")))
	require.NoError(t, batch.Commit())

	// Cancel after commit should be a no-op
	require.NoError(t, batch.Cancel())
}

func TestBatch_DoubleCommit(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	batch := s.NewBatch()

	require.NoError(t, batch.SetBytes([]byte("key1"), []byte("val1")))
	require.NoError(t, batch.Commit())

	// Second commit should return error
	err := batch.Commit()
	require.Error(t, err)
	require.Contains(t, err.Error(), "already committed")
}

func TestBatch_SetBytesAfterCommit(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	batch := s.NewBatch()

	require.NoError(t, batch.Commit())

	err := batch.SetBytes([]byte("key"), []byte("val"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "already committed")
}

func TestBatch_DeleteKey(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// First write a key
	batch := s.NewBatch()
	require.NoError(t, batch.SetBytes([]byte("to-delete"), []byte("value")))
	require.NoError(t, batch.Commit())

	// Verify it exists
	val, closer, err := s.Get([]byte("to-delete"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), val)
	require.NoError(t, closer.Close())

	// Delete the key
	batch2 := s.NewBatch()
	require.NoError(t, batch2.DeleteKey([]byte("to-delete")))
	require.NoError(t, batch2.Commit())

	// Verify it's gone
	_, _, err = s.Get([]byte("to-delete"))
	require.Error(t, err)
}

func TestBatch_DeleteKeyAfterCommit(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	batch := s.NewBatch()
	require.NoError(t, batch.Commit())

	err := batch.DeleteKey([]byte("key"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "already committed")
}

func TestBatch_DeleteRange(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write multiple keys
	batch := s.NewBatch()
	require.NoError(t, batch.SetBytes([]byte("aaa"), []byte("1")))
	require.NoError(t, batch.SetBytes([]byte("bbb"), []byte("2")))
	require.NoError(t, batch.SetBytes([]byte("ccc"), []byte("3")))
	require.NoError(t, batch.SetBytes([]byte("ddd"), []byte("4")))
	require.NoError(t, batch.Commit())

	// Delete range [bbb, ddd)
	batch2 := s.NewBatch()
	require.NoError(t, batch2.DeleteRange([]byte("bbb"), []byte("ddd"), pebble.NoSync))
	require.NoError(t, batch2.Commit())

	// "aaa" should exist
	val, closer, err := s.Get([]byte("aaa"))
	require.NoError(t, err)
	require.Equal(t, []byte("1"), val)
	require.NoError(t, closer.Close())

	// "bbb" and "ccc" should be deleted
	_, _, err = s.Get([]byte("bbb"))
	require.Error(t, err)
	_, _, err = s.Get([]byte("ccc"))
	require.Error(t, err)

	// "ddd" should still exist (upper bound is exclusive)
	val, closer, err = s.Get([]byte("ddd"))
	require.NoError(t, err)
	require.Equal(t, []byte("4"), val)
	require.NoError(t, closer.Close())
}

func TestBatch_SingleDeleteKey(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write a key exactly once
	batch := s.NewBatch()
	require.NoError(t, batch.SetBytes([]byte("once-key"), []byte("value")))
	require.NoError(t, batch.Commit())

	// Verify it exists
	val, closer, err := s.Get([]byte("once-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), val)
	require.NoError(t, closer.Close())

	// SingleDelete the key
	batch2 := s.NewBatch()
	require.NoError(t, batch2.SingleDeleteKey([]byte("once-key")))
	require.NoError(t, batch2.Commit())

	// Verify it's gone
	_, _, err = s.Get([]byte("once-key"))
	require.Error(t, err)
}

func TestBatch_SingleDeleteKeyAfterCommit(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	batch := s.NewBatch()
	require.NoError(t, batch.Commit())

	err := batch.SingleDeleteKey([]byte("key"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "already committed")
}

func TestBatch_DeleteRangeNoSync(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write keys
	batch := s.NewBatch()
	require.NoError(t, batch.SetBytes([]byte("x1"), []byte("a")))
	require.NoError(t, batch.SetBytes([]byte("x2"), []byte("b")))
	require.NoError(t, batch.SetBytes([]byte("x3"), []byte("c")))
	require.NoError(t, batch.Commit())

	// Delete range with NoSync
	batch2 := s.NewBatch()
	require.NoError(t, batch2.DeleteRangeNoSync([]byte("x1"), []byte("x3")))
	require.NoError(t, batch2.Commit())

	// x1, x2 should be deleted; x3 should remain
	_, _, err := s.Get([]byte("x1"))
	require.Error(t, err)
	_, _, err = s.Get([]byte("x2"))
	require.Error(t, err)

	val, closer, err := s.Get([]byte("x3"))
	require.NoError(t, err)
	require.Equal(t, []byte("c"), val)
	require.NoError(t, closer.Close())
}

func TestBatch_DeleteRangeNoSyncAfterCommit(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	batch := s.NewBatch()
	require.NoError(t, batch.Commit())

	err := batch.DeleteRangeNoSync([]byte("a"), []byte("z"))
	require.Error(t, err)
	require.Contains(t, err.Error(), "already committed")
}

func TestBatch_SetProtoAfterCommit(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	batch := s.NewBatch()
	require.NoError(t, batch.Commit())

	// This should fail because the batch is already committed.
	// We pass nil as the proto message because the committed check happens first.
	err := batch.SetProto([]byte("key"), nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already committed")
}

func TestBatch_DeleteRangeWithSet(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Pre-populate keys under prefix 0x01.
	batch1 := s.NewBatch()
	require.NoError(t, batch1.SetBytes([]byte{0x01, 'a'}, []byte("old-a")))
	require.NoError(t, batch1.SetBytes([]byte{0x01, 'b'}, []byte("old-b")))
	require.NoError(t, batch1.Commit())

	// In a single batch: DeleteRange the prefix, then Set new values.
	// This mirrors the cache rotation pattern: writeCacheRotation does
	// DeleteRange on the gen byte, then Merge writes new entries.
	batch2 := s.NewBatch()
	require.NoError(t, batch2.DeleteRangeNoSync([]byte{0x01}, []byte{0x02}))
	require.NoError(t, batch2.SetBytes([]byte{0x01, 'a'}, []byte("new-a")))
	require.NoError(t, batch2.SetBytes([]byte{0x01, 'c'}, []byte("new-c")))
	require.NoError(t, batch2.Commit())

	// 0x01,'a' should have the NEW value (Set overrides DeleteRange).
	val, closer, err := s.Get([]byte{0x01, 'a'})
	require.NoError(t, err)
	require.Equal(t, []byte("new-a"), val)
	require.NoError(t, closer.Close())

	// 0x01,'b' should be GONE (covered by DeleteRange, no new Set).
	_, _, err = s.Get([]byte{0x01, 'b'})
	require.Error(t, err)

	// 0x01,'c' should exist (new Set).
	val, closer, err = s.Get([]byte{0x01, 'c'})
	require.NoError(t, err)
	require.Equal(t, []byte("new-c"), val)
	require.NoError(t, closer.Close())
}

func TestBatch_MixedPrefixes(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	batch := s.NewBatch()

	// Simulate the mixed-prefix pattern from FSM apply:
	// 0xF0 (logs), 0xF1 (attributes), 0xFF (cache) — written interleaved.
	require.NoError(t, batch.SetBytes([]byte{0xFF, 0x00, 0x01}, []byte("cache-1")))
	require.NoError(t, batch.SetBytes([]byte{0xF1, 0x02, 0x01}, []byte("attr-1")))
	require.NoError(t, batch.SetBytes([]byte{0xF0, 0x00, 0x00, 0x01}, []byte("log-1")))
	require.NoError(t, batch.SetBytes([]byte{0xF1, 0x02, 0x02}, []byte("attr-2")))
	require.NoError(t, batch.SetBytes([]byte{0xFF, 0x00, 0x02}, []byte("cache-2")))
	require.NoError(t, batch.SetBytes([]byte{0xF0, 0x00, 0x00, 0x02}, []byte("log-2")))
	require.NoError(t, batch.Commit())

	// Verify all keys are present.
	for _, key := range [][]byte{
		{0xF0, 0x00, 0x00, 0x01},
		{0xF0, 0x00, 0x00, 0x02},
		{0xF1, 0x02, 0x01},
		{0xF1, 0x02, 0x02},
		{0xFF, 0x00, 0x01},
		{0xFF, 0x00, 0x02},
	} {
		_, closer, err := s.Get(key)
		require.NoError(t, err, "key %02X should exist", key)
		require.NoError(t, closer.Close())
	}
}

func benchmarkBatchCommit(b *testing.B, numEntries int) {
	b.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	dir := b.TempDir()

	s, err := NewStore(dir, logger, meter, DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}

	defer func() { _ = s.Close() }()

	for b.Loop() {
		batch := s.NewBatch()

		for i := range numEntries {
			prefix := byte(0xF0 + (i % 3))
			key := make([]byte, 40)
			key[0] = prefix
			key[1] = byte(i >> 8)
			key[2] = byte(i)

			for j := 3; j < 40; j++ {
				key[j] = byte((i*31 + j*17) & 0xFF)
			}

			val := make([]byte, 80)
			_ = batch.SetBytes(key, val)
		}

		if err := batch.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBatch_5(b *testing.B)     { benchmarkBatchCommit(b, 5) }
func BenchmarkBatch_100(b *testing.B)   { benchmarkBatchCommit(b, 100) }
func BenchmarkBatch_1000(b *testing.B)  { benchmarkBatchCommit(b, 1000) }
func BenchmarkBatch_10000(b *testing.B) { benchmarkBatchCommit(b, 10000) }

func TestBatch_NewIter(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write some data
	batch := s.NewBatch()
	require.NoError(t, batch.SetBytes([]byte("iter-key"), []byte("iter-val")))
	require.NoError(t, batch.Commit())

	// Create an iterator from a new batch
	batch2 := s.NewBatch()

	defer func() { _ = batch2.Cancel() }()

	iter, err := batch2.NewIter(&pebble.IterOptions{
		LowerBound: []byte("iter-"),
		UpperBound: []byte("iter-\xff"),
	})
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	require.True(t, iter.First())
	require.Equal(t, []byte("iter-key"), iter.Key())
}
