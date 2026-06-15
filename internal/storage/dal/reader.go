package dal

import (
	"encoding/binary"
	"errors"
	"io"
	"sync"

	"github.com/cockroachdb/pebble/v2"
)

// PebbleGetter provides point-lookup access to Pebble.
// Implemented by *pebble.DB, *pebble.Snapshot, *ReadHandle, and *Store.
//
// *WriteSession deliberately does NOT implement this interface: hot-path
// writers must not read from Pebble.
//
// *Store holds dbMu.RLock only for the duration of the Get call, which is safe
// because Get is atomic and short-lived.
type PebbleGetter interface {
	Get(key []byte) ([]byte, io.Closer, error)
}

// PebbleReader provides full read access (point lookups + iteration).
// Implemented by *pebble.DB, *pebble.Snapshot, and *ReadHandle.
//
// *Store does NOT implement this interface. Callers that need iterators must
// use NewReadHandle() or NewDirectReadHandle() — these hold dbMu.RLock for
// their entire lifetime, preventing RestoreCheckpoint from closing the DB
// while iterators are active.
//
// *WriteSession deliberately does NOT implement this interface: hot-path
// writers must not read from Pebble.
type PebbleReader interface {
	PebbleGetter
	NewIter(o *pebble.IterOptions) (*pebble.Iterator, error)
}

// ReadHandle provides read access to the store, optionally via a Pebble snapshot.
// It holds dbMu.RLock for its lifetime to prevent RestoreCheckpoint/Close from
// closing the DB while reads are in progress. The caller must call Close() when done.
//
// Two modes:
//   - Snapshot mode (NewReadHandle): point-in-time consistency via *pebble.Snapshot.
//   - Direct mode (NewDirectReadHandle): reads from *pebble.DB directly. Iterators
//     share the DB's keySpanCache (no per-snapshot re-initialization) and do not
//     pin SSTs beyond iterator lifetime, so compactions are not blocked.
type ReadHandle struct {
	reader PebbleReader
	snap   *pebble.Snapshot // nil in direct mode
	mu     *sync.RWMutex
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

	snap := db.NewSnapshot()

	return &ReadHandle{reader: snap, snap: snap, mu: &s.dbMu}, nil
}

// NewDirectReadHandle creates a ReadHandle backed by the DB directly (no snapshot).
// Iterators share the DB's keySpanCache, avoiding the per-snapshot sync.Once
// initialization overhead in finishInitializingIter. This does not block
// compactions beyond each iterator's lifetime.
//
// Safe for sequential, forward-only readers (e.g. log tailing) where
// point-in-time snapshot consistency is not required.
func (s *Store) NewDirectReadHandle() (*ReadHandle, error) {
	s.dbMu.RLock()

	db := s.getDB()
	if db == nil {
		s.dbMu.RUnlock()

		return nil, ErrStoreClosed
	}

	return &ReadHandle{reader: db, mu: &s.dbMu}, nil
}

func (h *ReadHandle) Get(key []byte) ([]byte, io.Closer, error) {
	return h.reader.Get(key)
}

func (h *ReadHandle) NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error) {
	return h.reader.NewIter(opts)
}

func (h *ReadHandle) Close() error {
	defer h.mu.RUnlock()

	if h.snap != nil {
		return h.snap.Close()
	}

	return nil
}

// Get performs a raw key lookup on the underlying Pebble database.
// This makes *Store implement PebbleGetter.
func (s *Store) Get(key []byte) ([]byte, io.Closer, error) {
	s.dbMu.RLock()
	defer s.dbMu.RUnlock()

	db := s.getDB()
	if db == nil {
		return nil, nil, ErrStoreClosed
	}

	return db.Get(key)
}

// NewBoundedIter creates a Pebble iterator bounded by [lower, upper).
func NewBoundedIter(reader PebbleReader, lower, upper []byte) (*pebble.Iterator, error) {
	return reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
}

// GetValue reads a raw value from Pebble, returning nil if not found.
// The returned bytes are a copy safe to use after the function returns.
func GetValue(reader PebbleGetter, key []byte) ([]byte, error) {
	val, closer, err := reader.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}

		return nil, err
	}

	defer func() { _ = closer.Close() }()

	cp := make([]byte, len(val))
	copy(cp, val)

	return cp, nil
}

// ReadUint64 reads a big-endian uint64 from Pebble. Returns defaultValue if not found or too short.
func ReadUint64(reader PebbleGetter, key []byte, defaultValue uint64) (uint64, error) {
	val, err := GetValue(reader, key)
	if err != nil {
		return 0, err
	}

	if len(val) < 8 {
		return defaultValue, nil
	}

	return binary.BigEndian.Uint64(val[:8]), nil
}

// ReadUint32 reads a big-endian uint32 from Pebble. Returns defaultValue if not found or too short.
func ReadUint32(reader PebbleGetter, key []byte, defaultValue uint32) (uint32, error) {
	val, err := GetValue(reader, key)
	if err != nil {
		return 0, err
	}

	if len(val) < 4 {
		return defaultValue, nil
	}

	return binary.BigEndian.Uint32(val[:4]), nil
}

// ReadString reads a string value from Pebble. Returns "" if not found.
func ReadString(reader PebbleGetter, key []byte) (string, error) {
	val, err := GetValue(reader, key)
	if err != nil {
		return "", err
	}

	return string(val), nil
}

// ReadBool reads a boolean flag from Pebble (0x01 = true). Returns false if not found.
func ReadBool(reader PebbleGetter, key []byte) (bool, error) {
	val, err := GetValue(reader, key)
	if err != nil {
		return false, err
	}

	if len(val) == 0 {
		return false, nil
	}

	return val[0] == 0x01, nil
}
