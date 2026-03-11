package readstore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
)

// Compact performs an online compaction of the Pebble read index.
// Unlike Pebble, Pebble compaction is online and does not require closing
// the database. Returns (sizeBefore, sizeAfter, err).
func (s *Store) Compact(_ context.Context) (sizeBefore, sizeAfter int64, err error) {
	dbPath := filepath.Join(s.dir, "readindex")

	// Measure size before compaction.
	sizeBefore = dirSize(dbPath)

	// Compact the full key range.
	err = s.db.Compact([]byte{0x00}, []byte{0xFF}, true)
	if err != nil {
		return sizeBefore, 0, fmt.Errorf("compacting read index: %w", err)
	}

	sizeAfter = dirSize(dbPath)

	s.logger.WithFields(map[string]any{
		"sizeBefore": sizeBefore,
		"sizeAfter":  sizeAfter,
	}).Infof("Read index compaction complete")

	return sizeBefore, sizeAfter, nil
}

// dirSize returns the total size of files in a directory.
func dirSize(path string) int64 {
	var total int64

	entries, err := os.ReadDir(path)
	if err != nil {
		return 0
	}

	for _, entry := range entries {
		if entry.IsDir() {
			total += dirSize(filepath.Join(path, entry.Name()))
		} else {
			info, err := entry.Info()
			if err == nil {
				total += info.Size()
			}
		}
	}

	return total
}
