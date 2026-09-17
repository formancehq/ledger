package grpc

import (
	"sync"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// checkpointStoreCache shares one read-only open of a query checkpoint's two
// directories across every concurrent reader of that checkpoint.
//
// Pebble takes a directory lock on open and keeps the held paths in a
// process-global table, so a second open of a directory this process already
// holds fails with "lock held by current process" — `ReadOnly` does not exempt
// it, because the lock exists to keep writers from corrupting each other. A
// query checkpoint is frozen and every one of these opens is read-only, so
// there is no writer to exclude and one open serves all of them.
//
// An entry exists only while at least one reader holds it: the last release
// closes both databases, so a cache at rest holds no handles and nothing has to
// be drained at shutdown.
type checkpointStoreCache struct {
	mu      sync.Mutex
	entries map[uint64]*checkpointStoreEntry
}

// openCheckpointFn opens both halves of one checkpoint. It owns unwinding its
// own partial state: a failure must leave nothing open.
type openCheckpointFn func() (*dal.Store, *readstore.Store, error)

type checkpointStoreEntry struct {
	// done is closed once open has run, successfully or not. Readers that find
	// an entry already installed wait on it rather than on the cache mutex, so
	// an open in flight never blocks a different checkpoint.
	done chan struct{}

	main    *dal.Store
	readIdx *readstore.Store
	err     error

	refs int
}

// acquire returns the checkpoint's shared stores, opening them through open if
// this is the first reader. The returned release must be called exactly once
// after the caller's last access; it is not returned when err is non-nil.
func (c *checkpointStoreCache) acquire(id uint64, open openCheckpointFn) (*dal.Store, *readstore.Store, func(), error) {
	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[uint64]*checkpointStoreEntry)
	}

	entry, joined := c.entries[id]
	if !joined {
		entry = &checkpointStoreEntry{done: make(chan struct{})}
		c.entries[id] = entry
	}
	entry.refs++
	c.mu.Unlock()

	if joined {
		<-entry.done
	} else {
		entry.main, entry.readIdx, entry.err = open()
		close(entry.done)
	}

	if entry.err != nil {
		// Every reader that reached a failed entry unwinds through the same
		// path, so the entry leaves the cache once the last of them has seen
		// the error and the next reader retries the open.
		c.release(id, entry)

		return nil, nil, nil, entry.err
	}

	return entry.main, entry.readIdx, func() { c.release(id, entry) }, nil
}

// release drops one reader's hold, closing both databases once the last one
// leaves.
//
// The close runs under the cache mutex, and the entry is removed in the same
// critical section. A reader arriving mid-close must take that mutex to see the
// entry is gone, so its own open cannot start until Pebble has released the
// directory lock.
func (c *checkpointStoreCache) release(id uint64, entry *checkpointStoreEntry) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry.refs--
	if entry.refs > 0 {
		return
	}

	if c.entries[id] == entry {
		delete(c.entries, id)
	}

	if entry.err != nil {
		return
	}

	// Best-effort: read-only close failures are non-actionable at request end.
	_ = entry.readIdx.Close()
	_ = entry.main.Close()
}
