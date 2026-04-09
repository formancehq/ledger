package dal

import (
	"io"
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

// PebbleReader provides read access for Pebble queries.
// Implemented by *pebble.DB, *pebble.Snapshot, *ReadHandle, and *Store.
type PebbleReader interface {
	Get(key []byte) ([]byte, io.Closer, error)
	NewIter(o *pebble.IterOptions) (*pebble.Iterator, error)
}

// ReadHandle provides point-in-time read access to the store via a Pebble snapshot.
// It holds dbMu.RLock for its lifetime to prevent RestoreCheckpoint/Close from
// closing the DB while the snapshot is in use. The caller must call Close() when done.
type ReadHandle struct {
	snap *pebble.Snapshot
	mu   *sync.RWMutex
}

// NewReadHandle creates a new ReadHandle backed by a Pebble snapshot.
// It holds dbMu.RLock until Close() is called, preventing DB lifecycle
// operations (RestoreCheckpoint, Close) from closing the DB while the
// snapshot is in use.
func (s *Store) NewReadHandle() (*ReadHandle, error) {
	s.dbMu.RLock()

	db := s.getDB()
	if db == nil {
		s.dbMu.RUnlock()

		return nil, ErrStoreClosed
	}

	return &ReadHandle{snap: db.NewSnapshot(), mu: &s.dbMu}, nil
}

func (h *ReadHandle) Get(key []byte) ([]byte, io.Closer, error) {
	return h.snap.Get(key)
}

func (h *ReadHandle) NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error) {
	return h.snap.NewIter(opts)
}

func (h *ReadHandle) Close() error {
	defer h.mu.RUnlock()

	return h.snap.Close()
}


// Get performs a raw key lookup on the underlying Pebble database.
// This makes *Store implement PebbleReader.
func (s *Store) Get(key []byte) ([]byte, io.Closer, error) {
	return s.getDB().Get(key)
}

// NewBoundedIter creates a Pebble iterator bounded by [lower, upper).
func NewBoundedIter(reader PebbleReader, lower, upper []byte) (*pebble.Iterator, error) {
	return reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
}

// ClosingCursor wraps a cursor and closes additional resources on Close.
type ClosingCursor[T any] struct {
	inner  Cursor[T]
	closer io.Closer
}

func (c *ClosingCursor[T]) Next() (T, error) {
	return c.inner.Next()
}

func (c *ClosingCursor[T]) Close() error {
	err := c.inner.Close()
	if closeErr := c.closer.Close(); err == nil {
		err = closeErr
	}

	return err
}

// NewClosingCursor creates a cursor that closes the given io.Closer when the cursor is closed.
func NewClosingCursor[T any](inner Cursor[T], closer io.Closer) Cursor[T] {
	return &ClosingCursor[T]{inner: inner, closer: closer}
}
