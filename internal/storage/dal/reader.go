package dal

import (
	"errors"
	"io"

	"github.com/cockroachdb/pebble/v2"
)

// PebbleReader provides read access for Pebble queries.
// Implemented by *pebble.DB, *pebble.Snapshot, *ReadHandle, and *Store.
type PebbleReader interface {
	Get(key []byte) ([]byte, io.Closer, error)
	NewIter(o *pebble.IterOptions) (*pebble.Iterator, error)
}

// ReadHandle provides point-in-time read access to the store via a Pebble snapshot.
// The caller must call Close() when done.
//
// If the underlying DB is closed (e.g. by RestoreCheckpoint), operations on
// the snapshot will panic with pebble.ErrClosed. Use RecoverablePebbleOp to
// convert these panics into ErrStoreClosed errors.
type ReadHandle struct {
	snap *pebble.Snapshot
}

// NewReadHandle creates a new ReadHandle backed by a Pebble snapshot.
// Returns ErrStoreClosed if the DB has been closed (e.g. by RestoreCheckpoint).
func (s *Store) NewReadHandle() (*ReadHandle, error) {
	snap, err := recoverPebbleClosedPanic(func() (*pebble.Snapshot, error) {
		return s.getDB().NewSnapshot(), nil
	})
	if err != nil {
		return nil, err
	}

	return &ReadHandle{snap: snap}, nil
}

func (h *ReadHandle) Get(key []byte) ([]byte, io.Closer, error) {
	return h.snap.Get(key)
}

func (h *ReadHandle) NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error) {
	return h.snap.NewIter(opts)
}

func (h *ReadHandle) Close() error {
	return h.snap.Close()
}

// recoverPebbleClosedPanic calls fn and recovers panics caused by
// pebble.ErrClosed, converting them to ErrStoreClosed.
func recoverPebbleClosedPanic[T any](fn func() (T, error)) (result T, err error) {
	defer func() {
		if r := recover(); r != nil {
			if rErr, ok := r.(error); ok && errors.Is(rErr, pebble.ErrClosed) {
				err = ErrStoreClosed
				return
			}

			panic(r) // re-panic for non-pebble-closed panics
		}
	}()

	return fn()
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
