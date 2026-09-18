package grpc

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// checkpointStoreCache shares one read-only open of a query checkpoint's two
// directories across every concurrent reader of that checkpoint.
//
// Pebble takes a directory lock on open and keeps the held paths in a
// process-global table, so a second open of a directory this process already
// holds fails with "lock held by current process"; `ReadOnly` does not exempt
// it. A query checkpoint is frozen and every one of these opens is read-only,
// so there is no writer to exclude and one open serves all of them.
//
// Readiness is settled and the caller's lease is held before the open runs, so
// a failed open is damage or a resource limit, not lag. The error goes to the
// reader that opened and to those already waiting on it; the entry is withdrawn
// as the error publishes, so a reader arriving after the failure opens again.
//
// An entry exists only while at least one reader holds it: the last release
// closes both databases, so a cache at rest holds no handles.
type checkpointStoreCache struct {
	mu      sync.Mutex
	entries map[uint64]*checkpointStoreEntry
}

// openCheckpointFn opens both halves of one checkpoint. It owns unwinding any
// handle it has taken, on a panic as well as on an error: one left open keeps
// Pebble's directory lock against every later reader. A panic inside
// pebble.Open itself is unwound by Pebble, which releases its directory lock
// before propagating it.
type openCheckpointFn func() (*dal.Store, *readstore.Store, error)

type checkpointStoreEntry struct {
	// done is closed once open has run, successfully or not. Joining readers
	// wait on it, so an open in flight never holds the cache mutex.
	done chan struct{}

	main    *dal.Store
	readIdx *readstore.Store
	err     error

	logger logging.Logger

	refs int
}

// openCheckpointDirs opens a checkpoint's main store and read index read-only.
// Both handles come back or neither does.
func openCheckpointDirs(mainPath, readIndexPath string, logger logging.Logger) (*dal.Store, *readstore.Store, error) {
	mainStore, err := dal.OpenReadOnly(mainPath, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("opening checkpoint main store: %w", err)
	}

	// Unwinds the main store if the read index fails to open or panics; leaking
	// it would keep Pebble's lock on a directory that is not damaged.
	handedOver := false
	defer func() {
		if !handedOver {
			closeSafe(logger, "checkpoint main store", mainStore.Close)
		}
	}()

	readIdx, err := readstore.OpenReadOnly(readIndexPath, logger)
	if err != nil {
		return nil, nil, fmt.Errorf("opening checkpoint read index: %w", err)
	}

	handedOver = true

	return mainStore, readIdx, nil
}

// acquire returns the checkpoint's shared stores, opening them through open if
// this is the first reader. The returned release must be called after the
// caller's last access; it is not returned when err is non-nil.
//
// A reader waiting on another reader's open honors ctx, so one slow open does
// not hold the others past their deadlines.
func (c *checkpointStoreCache) acquire(ctx context.Context, id uint64, logger logging.Logger, open openCheckpointFn) (*dal.Store, *readstore.Store, func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}

	c.mu.Lock()
	if c.entries == nil {
		c.entries = make(map[uint64]*checkpointStoreEntry)
	}

	entry, joined := c.entries[id]
	if !joined {
		entry = &checkpointStoreEntry{done: make(chan struct{}), logger: logger}
		c.entries[id] = entry
	}
	entry.refs++
	c.mu.Unlock()

	if joined {
		select {
		case <-entry.done:
		case <-ctx.Done():
			// The opener holds a ref of its own while its open runs, so this
			// one can be dropped without stranding handles still being opened.
			c.release(id, entry)

			return nil, nil, nil, ctx.Err()
		}
	} else {
		main, readIdx, err := openSafe(open, logger)

		// The entry is reachable from the map before the open starts and
		// release reads err under the mutex, so the results publish under it.
		// A failed entry is withdrawn in the same critical section, so a reader
		// arriving after the failure opens again.
		c.mu.Lock()
		entry.main, entry.readIdx, entry.err = main, readIdx, err
		if err != nil && c.entries[id] == entry {
			delete(c.entries, id)
		}
		c.mu.Unlock()

		close(entry.done)
	}

	c.mu.Lock()
	main, readIdx, openErr := entry.main, entry.readIdx, entry.err
	c.mu.Unlock()

	if openErr != nil {
		c.release(id, entry)

		return nil, nil, nil, openErr
	}

	// A second call would drop a hold this reader does not have.
	var once sync.Once

	return main, readIdx, func() {
		once.Do(func() { c.release(id, entry) })
	}, nil
}

// openSafe turns a panic out of open into the entry's error, so the entry is
// withdrawn and later readers open again; otherwise every reader of this
// checkpoint would wait forever on a done channel that is never closed. It
// recovers any panic, and the branch that produced one is still a bug.
//
// The stack is logged once here: the error is shared with every waiting reader
// and sanitized before it reaches a client.
func openSafe(open openCheckpointFn, logger logging.Logger) (main *dal.Store, readIdx *readstore.Store, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("Panic opening checkpoint stores: %v\n%s", r, debug.Stack())

			main, readIdx = nil, nil
			err = fmt.Errorf("panic opening checkpoint stores (recovered): %v", r)
		}
	}()

	return open()
}

// release drops one reader's hold, closing both databases once the last one
// leaves. The close runs in the critical section that removes the entry, so a
// reader arriving mid-close cannot start its own open before Pebble has
// released the directory lock; a slow close therefore delays acquisitions of
// every checkpoint.
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

	// Isolated from each other: pebble.DB.Close panics when a read resource on
	// it is still referenced, and a panic out of the first close would skip the
	// second, leaving that directory locked against every later reader.
	closeSafe(entry.logger, "checkpoint read index", entry.readIdx.Close)
	closeSafe(entry.logger, "checkpoint main store", entry.main.Close)
}

// closeSafe closes one store, logging a failure or a recovered panic out of
// Pebble's Close; neither is actionable at request end. dal.CloseSafe documents
// the panic, and that the directory lock is already released when it fires.
func closeSafe(logger logging.Logger, what string, closeStore func() error) {
	if err := dal.CloseSafe(closeStore); err != nil {
		logger.WithField("error", err).Errorf("Failed to close %s", what)
	}
}
