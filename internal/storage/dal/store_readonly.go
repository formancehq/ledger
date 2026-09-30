package dal

import (
	"fmt"

	"github.com/linxGnu/grocksdb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/kv"
)

// OpenReadOnly opens a RocksDB database at dirPath in read-only mode.
// It does not manage checkpoints. The caller must close the returned store.
// Concurrent reads are supported. Secondary opens cap MaxOpenFiles at 32 to
// limit file metadata held while reading backup checkpoints.
func OpenReadOnly(dirPath string, logger logging.Logger) (*Store, error) {
	opts := kv.Options{
		ReadOnly:  true,
		Configure: func(o *grocksdb.Options) { o.SetMaxOpenFiles(32) },
	}

	db, err := kv.Open(dirPath, opts)
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

// OpenDirect opens a RocksDB database at dirPath in read-write mode
// without checkpoint management. Used for backup compaction operations.
// The caller must call Close() when done.
func OpenDirect(dirPath string, logger logging.Logger) (*Store, error) {
	opts := kv.Options{}

	db, err := kv.Open(dirPath, opts)
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
