package readstore

import (
	"fmt"
	"os"
	"path/filepath"
)

// Keep temporary SST hard links on the read-index volume, including when
// --read-index-dir lives on a different device from the primary store.
const backupScratchDir = "ledger-backup-tmp"

// CaptureBackupCheckpoint returns an immutable, crash-resumable read index.
// The caller must release it after upload. New reclaims abandoned checkpoints
// on next open. A unique directory isolates concurrent backup destinations.
func (s *Store) CaptureBackupCheckpoint() (string, func(), error) {
	if !s.ReadProjectionHealthy() {
		return "", nil, ErrReadProjectionFailed
	}

	base := filepath.Join(s.dir, "readindex", backupScratchDir)
	if err := os.MkdirAll(base, 0o700); err != nil {
		return "", nil, fmt.Errorf("creating read index backup scratch directory: %w", err)
	}

	parent, err := os.MkdirTemp(base, "full-")
	if err != nil {
		return "", nil, fmt.Errorf("allocating read index backup scratch directory: %w", err)
	}
	release := func() {
		if err := os.RemoveAll(parent); err != nil {
			s.logger.Errorf("Removing temporary read index backup checkpoint %q: %v", parent, err)
		}
	}

	path := filepath.Join(parent, "readindex")
	// A backup runs outside the builder goroutine. It must not release
	// serving-transition marks: a promotion can be marked but not committed.
	if err := s.checkpointFlushedVersion(path); err != nil {
		release()

		return "", nil, fmt.Errorf("capturing read index backup checkpoint: %w", err)
	}
	if !s.ReadProjectionHealthy() {
		release()

		return "", nil, ErrReadProjectionFailed
	}

	return path, release, nil
}

// A WAL-less Pebble checkpoint only contains flushed SSTs. Flush first, then
// pin a single Pebble version. Concurrent commits after the flush either
// belong to a later complete flush in that version, or are absent. Cursors
// therefore describe the batches actually present in the frozen store.
func (s *Store) checkpointFlushedVersion(path string) error {
	if err := s.db.Flush(); err != nil {
		return fmt.Errorf("flushing read index before checkpoint: %w", err)
	}

	return s.db.Checkpoint(path)
}
