package dal

import (
	"io"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"
)

func TestReadHandle_GetAndClose(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write data
	batch := s.NewBatch()
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
	batch := s.NewBatch()
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
	batch := s.NewBatch()
	require.NoError(t, batch.SetBytes([]byte("snap-k"), []byte("v1")))
	require.NoError(t, batch.Commit())

	// Create read handle (point-in-time snapshot)
	rh, err := s.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = rh.Close() }()

	// Write more data AFTER the read handle was created
	batch2 := s.NewBatch()
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
	batch := s.NewBatch()
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

	inner := NewSliceCursor([]int{10, 20, 30})
	closer := &errCloser{}
	cursor := NewClosingCursor[int](inner, closer)

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

	inner := NewSliceCursor[string](nil)
	closer := &errCloser{}
	cursor := NewClosingCursor[string](inner, closer)

	_, err := cursor.Next()
	require.ErrorIs(t, err, io.EOF)

	require.NoError(t, cursor.Close())
	require.True(t, closer.closed)
}
