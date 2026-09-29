package dal

import (
	"fmt"
	"os"
	"path/filepath"
)

// checkpointReadyMarker is the sentinel file written into a RocksDB checkpoint
// directory as the final step, only after the whole directory has been
// atomically renamed into place. Its presence is the single authoritative
// per-replica readiness signal for both halves of a query checkpoint — the main
// store and the read index.
//
// A directory, or a manifest inside it, merely existing is NOT sufficient.
// A checkpoint producer may leave a partially populated directory if the
// process stops during creation. A directory or MANIFEST existing is not
// enough to prove that its SSTs and WAL are complete. Never recover from an
// unmarked checkpoint: discard it or fall back to an older ready checkpoint.
const checkpointReadyMarker = ".ready"

// CheckpointDirReady reports whether a checkpoint directory has been fully
// materialized on THIS replica, i.e. its producer wrote the readiness marker as
// the last step of an atomic materialization.
func CheckpointDirReady(dirPath string) bool {
	_, err := os.Stat(filepath.Join(dirPath, checkpointReadyMarker))

	return err == nil
}

// MarkCheckpointReady writes the readiness marker into a completed checkpoint
// directory and fsyncs both the marker and its parent directory so the marker
// is durable and cannot be observed before the directory content it vouches for.
func MarkCheckpointReady(dirPath string) error {
	markerPath := filepath.Join(dirPath, checkpointReadyMarker)

	f, err := os.OpenFile(markerPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o640)
	if err != nil {
		return fmt.Errorf("creating readiness marker: %w", err)
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()

		return fmt.Errorf("syncing readiness marker: %w", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("closing readiness marker: %w", err)
	}

	return FsyncDir(dirPath)
}

// FsyncDir fsyncs a directory so a rename/create inside it is durable.
func FsyncDir(dirPath string) error {
	d, err := os.Open(dirPath)
	if err != nil {
		return fmt.Errorf("opening dir for fsync: %w", err)
	}

	if err := d.Sync(); err != nil {
		_ = d.Close()

		return fmt.Errorf("fsync dir: %w", err)
	}

	return d.Close()
}
