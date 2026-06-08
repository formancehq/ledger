package dal

import (
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// OpenReadOnly opens a Pebble database at dirPath in read-only mode.
// It does not manage checkpoints.
// The returned Store implements PebbleReader and can be passed to free functions in state/ and events/.
// The caller must call Close() when done.
//
// Memory profile: tuned for short-lived secondary opens (e.g. reading a few
// well-known keys from a backup checkpoint while the primary store still
// holds its full working set). MaxOpenFiles is capped at 32 so Pebble does
// not warm up table metadata (block index + bloom filters) for every SST in
// large stores — on a 290 GB checkpoint that previously inflated the heap
// by several GiB and tipped the pod over its memory limit during full
// backups. The default 8 MiB block cache is left in place.
func OpenReadOnly(dirPath string, logger logging.Logger) (*Store, error) {
	opts := &pebble.Options{
		Logger:       NewPebbleLogger(logger),
		ReadOnly:     true,
		MaxOpenFiles: 32,
	}

	db, err := pebble.Open(dirPath, opts)
	if err != nil {
		return nil, fmt.Errorf("opening read-only pebble database at %s: %w", dirPath, err)
	}

	store := &Store{
		opts:    opts,
		logger:  logger.WithField("cmp", "pebble-readonly"),
		dataDir: dirPath,
	}
	store.db = db

	return store, nil
}

// OpenDirect opens a Pebble database at dirPath in read-write mode
// without checkpoint management. Used for backup compaction operations.
// The caller must call Close() when done.
func OpenDirect(dirPath string, logger logging.Logger) (*Store, error) {
	opts := &pebble.Options{
		Logger: NewPebbleLogger(logger),
	}

	db, err := pebble.Open(dirPath, opts)
	if err != nil {
		return nil, fmt.Errorf("opening pebble database at %s: %w", dirPath, err)
	}

	store := &Store{
		opts:    opts,
		logger:  logger.WithField("cmp", "pebble-direct"),
		dataDir: dirPath,
	}
	store.db = db

	return store, nil
}
