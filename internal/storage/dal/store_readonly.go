package dal

import (
	"fmt"

	pebble "github.com/formancehq/ledger/v3/internal/storage/kv"
	"github.com/linxGnu/grocksdb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// OpenReadOnly opens a Pebble database at dirPath in read-only mode.
// It does not manage checkpoints.
// The returned Store implements PebbleReader and can be passed to free functions in state/ and events/.
// The caller must call Close() when done.
//
// A single handle may be shared by concurrent readers; Pebble supports
// concurrent reads. RestoreCheckpoint would swap the database under them; its
// only caller is the IncomingRestoreFactory, built once over the live store at
// boot.
//
// Memory profile: tuned for short-lived secondary opens (e.g. reading a few
// well-known keys from a backup checkpoint while the primary store still
// holds its full working set). MaxOpenFiles is capped at 32 so Pebble does
// not warm up table metadata (block index + bloom filters) for every SST in
// large stores — on a 290 GB checkpoint that previously inflated the heap
// by several GiB and tipped the pod over its memory limit during full
// backups. The default 8 MiB block cache is left in place.
func OpenReadOnly(dirPath string, logger logging.Logger) (*Store, error) {
	opts := pebble.Options{
		ReadOnly:  true,
		Configure: func(o *grocksdb.Options) { o.SetMaxOpenFiles(32) },
	}

	db, err := pebble.Open(dirPath, opts)
	if err != nil {
		return nil, fmt.Errorf("opening read-only rocksdb database at %s: %w", dirPath, err)
	}

	store := &Store{
		opts:    opts,
		logger:  logger.WithField("cmp", "rocksdb-readonly"),
		dataDir: dirPath,
	}
	store.db = db

	return store, nil
}

// OpenDirect opens a Pebble database at dirPath in read-write mode
// without checkpoint management. Used for backup compaction operations.
// The caller must call Close() when done.
func OpenDirect(dirPath string, logger logging.Logger) (*Store, error) {
	opts := pebble.Options{}

	db, err := pebble.Open(dirPath, opts)
	if err != nil {
		return nil, fmt.Errorf("opening rocksdb database at %s: %w", dirPath, err)
	}

	store := &Store{
		opts:    opts,
		logger:  logger.WithField("cmp", "rocksdb-direct"),
		dataDir: dirPath,
	}
	store.db = db

	return store, nil
}
