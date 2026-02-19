package data

import (
	"fmt"

	"github.com/cockroachdb/pebble"
	"github.com/formancehq/go-libs/v3/logging"
)

// OpenReadOnly opens a Pebble database at dirPath in read-only mode.
// It does not manage checkpoints or CURRENT_CHECKPOINT files.
// The returned Store supports all read methods (GetLastAppliedIndex, ListLedgers, etc.).
// The caller must call Close() when done.
func OpenReadOnly(dirPath string, logger logging.Logger) (*Store, error) {
	opts := &pebble.Options{
		ReadOnly: true,
	}

	db, err := pebble.Open(dirPath, opts)
	if err != nil {
		return nil, fmt.Errorf("opening read-only pebble database at %s: %w", dirPath, err)
	}

	return &Store{
		db:      db,
		opts:    opts,
		logger:  logger.WithField("cmp", "pebble-readonly"),
		dataDir: dirPath,
	}, nil
}

// OpenDirect opens a Pebble database at dirPath in read-write mode
// without checkpoint management. Used for backup compaction operations.
// The caller must call Close() when done.
func OpenDirect(dirPath string, logger logging.Logger) (*Store, error) {
	opts := &pebble.Options{}

	db, err := pebble.Open(dirPath, opts)
	if err != nil {
		return nil, fmt.Errorf("opening pebble database at %s: %w", dirPath, err)
	}

	return &Store{
		db:      db,
		opts:    opts,
		logger:  logger.WithField("cmp", "pebble-direct"),
		dataDir: dirPath,
	}, nil
}
