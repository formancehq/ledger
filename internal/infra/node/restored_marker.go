package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const restoredMarkerFile = "RESTORED"

// RestoredMarker contains the metadata written by FinalizeRestore.
type RestoredMarker struct {
	// LastAppliedIndex is the raft index the restored genesis occupies in the
	// new log — the checkpoint's applied index, or 1 for a genesis checkpoint
	// (PrepareForBackup guarantees >= 1). Bootstrap plants the WAL snapshot at
	// this index so joiners must sync a checkpoint, and refuses a marker
	// carrying 0. A boundary label only: after a full + incremental restore
	// the restored state is NEWER than this index (exports are
	// sequence-keyed and do not advance it).
	LastAppliedIndex     uint64 `json:"lastAppliedIndex"`
	LastAppliedTimestamp uint64 `json:"lastAppliedTimestamp"`
}

// ReadRestoredMarker reads the RESTORED marker file from the data directory.
// Returns nil if the marker does not exist.
func ReadRestoredMarker(dataDir string) (*RestoredMarker, error) {
	markerPath := filepath.Join(dataDir, restoredMarkerFile)

	data, err := os.ReadFile(markerPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}

		return nil, fmt.Errorf("reading restored marker: %w", err)
	}

	var marker RestoredMarker
	if err := json.Unmarshal(data, &marker); err != nil {
		return nil, fmt.Errorf("parsing restored marker: %w", err)
	}

	return &marker, nil
}

// WriteRestoredMarker atomically writes the RESTORED marker (temp file +
// rename) to the data directory. Callers must place the restored checkpoint
// BEFORE writing the marker: the marker is the commit point of the restore —
// a marker observed without its checkpoint would make bootstrap plant the
// restore WAL snapshot over whatever store the next boot opens.
func WriteRestoredMarker(dataDir string, marker RestoredMarker) error {
	data, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("marshaling restored marker: %w", err)
	}

	tmpPath := filepath.Join(dataDir, restoredMarkerFile+".tmp")
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("writing restored marker temp file: %w", err)
	}

	if err := os.Rename(tmpPath, filepath.Join(dataDir, restoredMarkerFile)); err != nil {
		_ = os.Remove(tmpPath)

		return fmt.Errorf("renaming restored marker into place: %w", err)
	}

	return nil
}

// RemoveRestoredMarker removes the RESTORED marker file from the data directory.
func RemoveRestoredMarker(dataDir string) error {
	markerPath := filepath.Join(dataDir, restoredMarkerFile)

	err := os.Remove(markerPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("removing restored marker: %w", err)
	}

	return nil
}
