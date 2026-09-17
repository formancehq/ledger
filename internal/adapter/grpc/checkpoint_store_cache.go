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

// openCheckpointFn opens both halves of one checkpoint. It owns unwinding any
// handle it has taken, on a panic as well as on an error: one left open keeps
// Pebble's directory lock and locks out every later reader. A panic inside an
// open itself, before it returns a handle, is outside that reach — nothing has
// been handed over to close, and the directory stays locked until restart.
type openCheckpointFn func() (*dal.Store, *readstore.Store, error)

type checkpointStoreEntry struct {
	// done is closed once open has run, successfully or not. Readers that find
	// an entry already installed wait on it rather than on the cache mutex, so
	// an open in flight never blocks a different checkpoint.
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
		// Both markers are present and the caller's lease keeps a committed
		// deletion from unlinking under this open, so the directory the marker
		// vouched for is damaged rather than late; the error surfaces as-is,
		// like the read index's below.
		return nil, nil, fmt.Errorf("opening checkpoint main store: %w", err)
	}

	// Unwinds the main store on the read index's failure and on a panic out of
	// it. Leaking it would leave Pebble holding the main directory's lock, so
	// later readers would fail on a directory that is not damaged at all.
	// A panic inside dal.OpenReadOnly above is not covered: it returns no handle
	// to close.
	handedOver := false
	defer func() {
		if !handedOver {
			// Best-effort: this read-only store is only being unwound after a
			// failed open.
			_ = mainStore.Close()
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
// this is the first reader. The returned release must be called exactly once
// after the caller's last access; it is not returned when err is non-nil.
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
			// The opener holds a ref of its own for as long as its open runs,
			// so dropping this one can never strand the entry at zero refs with
			// handles still being opened.
			c.release(id, entry)

			return nil, nil, nil, ctx.Err()
		}
	} else {
		entry.main, entry.readIdx, entry.err = openSafe(open, logger)
		close(entry.done)
	}

	if entry.err != nil {
		// The entry leaves the cache once the last reader holding it has
		// released, and the reader after that opens again. A reader already
		// waiting when the open failed is served the same error: the readiness
		// markers are checked before the open, so the expected failure here is
		// damage, which a second open would hit identically. A transient cause
		// (a descriptor or space limit) is not distinguished, and costs those
		// waiting readers an error each until the entry clears.
		c.release(id, entry)

		return nil, nil, nil, entry.err
	}

	return entry.main, entry.readIdx, func() { c.release(id, entry) }, nil
}

// openSafe turns a panicking open into the entry's error, so the entry is
// evicted and a later reader opens again instead of every reader of this
// checkpoint waiting on a done channel that is never closed. A panic here is
// already contained per-request by the server's recovery interceptor; without
// this the first one would strand the checkpoint until the process restarts.
//
// open unwinds the handles it has taken on the way out, so the panic does not
// leave one holding the directory lock; a panic inside an open itself is outside
// that reach (see openCheckpointFn).
//
// The stack is logged here rather than carried in the error: the error is served
// to every reader sharing this open and is sanitized before it reaches a client,
// so the node log is the only place it would be readable, and logging it once
// keeps one panic from printing the same stack per waiting reader.
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
// leaves.
//
// The close runs under the cache mutex, and the entry is removed in the same
// critical section. A reader arriving mid-close must take that mutex to see the
// entry is gone, so its own open cannot start until Pebble has released the
// directory lock. The cost is that a slow close delays acquisitions of every
// other checkpoint; closing a read-only database is short, and the alternative
// is a handoff window where the bug reappears.
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

// closeSafe closes one store, containing a panic out of Pebble's Close.
// Close failures are not actionable at request end, so they are logged rather
// than returned.
func closeSafe(logger logging.Logger, what string, closeStore func() error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("Panic closing %s: %v\n%s", what, r, debug.Stack())
		}
	}()

	if err := closeStore(); err != nil {
		logger.WithField("error", err).Errorf("Failed to close %s", what)
	}
}
