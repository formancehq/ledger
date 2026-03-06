package readstore

import (
	"context"
	"fmt"
	"os"

	bolt "go.etcd.io/bbolt"
)

// Compact performs an online compaction of the bbolt read index.
// It copies live data to a temporary file via read transactions (the builder
// may write to the source database concurrently — this is safe), closes the
// current database, atomically swaps the files, and reopens.
//
// During the brief close→reopen window, new View/Update calls will receive a
// "database not open" error from bbolt. Callers should treat this as a
// transient error. In-flight transactions started before Close() will complete
// normally; bbolt waits for them before closing.
//
// Returns (sizeBefore, sizeAfter, err).
func (s *Store) Compact(_ context.Context) (sizeBefore, sizeAfter int64, err error) {
	tmpPath := s.path + ".tmp"

	// Measure size before compaction.
	if info, statErr := os.Stat(s.path); statErr == nil {
		sizeBefore = info.Size()
	}

	// Remove any leftover tmp file from a previous failed attempt.
	removeErr := os.Remove(tmpPath)
	if removeErr != nil && !os.IsNotExist(removeErr) {
		return 0, 0, fmt.Errorf("removing leftover tmp file: %w", removeErr)
	}

	// Open temporary destination database with no InitialMmapSize.
	tmpDB, err := bolt.Open(tmpPath, 0o600, &bolt.Options{
		NoSync:       true,
		FreelistType: bolt.FreelistMapType,
	})
	if err != nil {
		return 0, 0, fmt.Errorf("opening tmp database: %w", err)
	}

	// Copy live data via read transactions. bolt.Compact reads the source
	// using successive read transactions, so concurrent writes are safe.
	compactErr := bolt.Compact(tmpDB, s.db, 65536)
	if compactErr != nil {
		_ = tmpDB.Close()
		_ = os.Remove(tmpPath)

		return 0, 0, fmt.Errorf("compacting database: %w", compactErr)
	}

	closeErr := tmpDB.Close()
	if closeErr != nil {
		_ = os.Remove(tmpPath)

		return 0, 0, fmt.Errorf("closing tmp database: %w", closeErr)
	}

	// Close the current database. bbolt waits for in-flight transactions.
	closeErr = s.db.Close()
	if closeErr != nil {
		_ = os.Remove(tmpPath)

		return 0, 0, fmt.Errorf("closing current database: %w", closeErr)
	}

	// Atomically swap files.
	renameErr := os.Rename(tmpPath, s.path)
	if renameErr != nil {
		return sizeBefore, 0, fmt.Errorf("renaming tmp database: %w", renameErr)
	}

	// Reopen with original options.
	initialMmapSize := s.initialMmapSize
	if initialMmapSize == 0 {
		initialMmapSize = DefaultInitialMmapSize
	}

	s.db, err = bolt.Open(s.path, 0o600, &bolt.Options{
		NoSync:          true,
		FreelistType:    bolt.FreelistMapType,
		NoFreelistSync:  s.noFreelistSync,
		InitialMmapSize: initialMmapSize,
	})
	if err != nil {
		return sizeBefore, 0, fmt.Errorf("reopening database after compaction: %w", err)
	}

	if info, statErr := os.Stat(s.path); statErr == nil {
		sizeAfter = info.Size()
	}

	s.logger.WithFields(map[string]any{
		"sizeBefore": sizeBefore,
		"sizeAfter":  sizeAfter,
	}).Infof("Read index compaction complete")

	return sizeBefore, sizeAfter, nil
}
