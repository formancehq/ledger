package dal

import (
	"io"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
)

func TestReadHandle_GetAndClose(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write data
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("rh-key"), []byte("rh-val")))
	require.NoError(t, batch.Commit())

	// Create read handle
	rh, err := s.NewReadHandle()
	require.NoError(t, err)

	val, closer, err := rh.Get([]byte("rh-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("rh-val"), val)
	require.NoError(t, closer.Close())

	// Missing key should error
	_, _, err = rh.Get([]byte("nonexistent"))
	require.Error(t, err)

	require.NoError(t, rh.Close())
}

func TestReadHandle_NewIter(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write data
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("rh-a"), []byte("1")))
	require.NoError(t, batch.SetBytes([]byte("rh-b"), []byte("2")))
	require.NoError(t, batch.Commit())

	// Create read handle and iterate
	rh, err := s.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = rh.Close() }()

	iter, err := rh.NewIter(&pebble.IterOptions{
		LowerBound: []byte("rh-"),
		UpperBound: []byte("rh-\xff"),
	})
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	var keys []string
	for iter.First(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	require.NoError(t, iter.Error())
	require.Equal(t, []string{"rh-a", "rh-b"}, keys)
}

func TestReadHandle_PointInTimeSnapshot(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write initial data
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("snap-k"), []byte("v1")))
	require.NoError(t, batch.Commit())

	// Create read handle (point-in-time snapshot)
	rh, err := s.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = rh.Close() }()

	// Write more data AFTER the read handle was created
	batch2 := s.OpenWriteSession()
	require.NoError(t, batch2.SetBytes([]byte("snap-k"), []byte("v2")))
	require.NoError(t, batch2.Commit())

	// Read handle should still see old value
	val, closer, err := rh.Get([]byte("snap-k"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), val)
	require.NoError(t, closer.Close())
}

func TestStore_Get(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write data
	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("store-get"), []byte("value")))
	require.NoError(t, batch.Commit())

	// Get via Store directly (PebbleReader interface)
	val, closer, err := s.Get([]byte("store-get"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), val)
	require.NoError(t, closer.Close())

	// Non-existent key
	_, _, err = s.Get([]byte("missing"))
	require.Error(t, err)
}

// errCloser is a test helper that records whether Close was called.
type errCloser struct {
	closed bool
}

func (c *errCloser) Close() error {
	c.closed = true

	return nil
}

func TestClosingCursor_ClosesInnerAndCloser(t *testing.T) {
	t.Parallel()

	inner := cursor.NewSliceCursor([]int{10, 20, 30})
	closer := &errCloser{}
	cursor := cursor.NewClosingCursor[int](inner, closer)

	// Read all items
	v, err := cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 10, v)

	v, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 20, v)

	v, err = cursor.Next()
	require.NoError(t, err)
	require.Equal(t, 30, v)

	_, err = cursor.Next()
	require.ErrorIs(t, err, io.EOF)

	// Close should close both inner and the closer
	require.NoError(t, cursor.Close())
	require.True(t, closer.closed)
}

func TestClosingCursor_EmptyInner(t *testing.T) {
	t.Parallel()

	inner := cursor.NewSliceCursor[string](nil)
	closer := &errCloser{}
	cursor := cursor.NewClosingCursor[string](inner, closer)

	_, err := cursor.Next()
	require.ErrorIs(t, err, io.EOF)

	require.NoError(t, cursor.Close())
	require.True(t, closer.closed)
}

// TestStoreGet_ResourceDoesNotOutliveTheReadLock pins EN-2072 bug 2.
//
// (*Store).Get releases dbMu.RLock when it returns, so it must not hand back
// Pebble's closer. On an SST-backed lookup that closer is a live
// *pebble.Iterator holding a file cache reference, and closing the DB while one
// is outstanding panics inside Pebble with "element has outstanding
// references" — observed in production shutdown via the fx stop hook.
func TestStoreGet_ResourceDoesNotOutliveTheReadLock(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("get-key"), []byte("get-val")))
	require.NoError(t, batch.Commit())

	// Force an SST. A memtable-backed lookup takes no file cache reference and
	// would not exercise the defect.
	require.NoError(t, s.Flush())

	val, closer, err := s.Get([]byte("get-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("get-val"), val)
	require.NotNil(t, closer, "callers must always be able to Close the result")

	// Deliberately leave the closer unreleased, as a caller preempted between
	// Get returning and its deferred Close would.
	require.NotPanics(t, func() {
		require.NoError(t, s.Close())
	}, "Store.Close must not panic while a Get closer is still unreleased")

	// The returned bytes are a copy, so they outlive the database.
	require.Equal(t, []byte("get-val"), val)
	require.NoError(t, closer.Close())
}

// Latest reads the store's current committed state while the handle keeps its
// own point-in-time view, and it needs no lock of its own: closing the store
// under an open handle waits for that handle, and a Latest taken meanwhile
// must not join the wait.
func TestReadHandle_LatestSeesLaterCommits(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	rh, err := s.NewReadHandle()
	require.NoError(t, err)

	batch := s.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte("late-key"), []byte("late-val")))
	require.NoError(t, batch.Commit())

	closed := make(chan error, 1)
	go func() { closed <- s.Close() }()

	latest := rh.Latest()

	_, _, err = rh.Get([]byte("late-key"))
	require.ErrorIs(t, err, pebble.ErrNotFound, "the handle keeps its pinned view")

	val, closer, err := latest.Get([]byte("late-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("late-val"), val)
	require.NoError(t, closer.Close())

	require.NoError(t, latest.Close())
	require.NoError(t, rh.Close())
	require.NoError(t, <-closed)
}
