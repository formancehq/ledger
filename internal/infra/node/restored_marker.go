package node

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const restoredMarkerFile = "RESTORED"

// ErrRestoredMarkerPublished means RESTORED is already visible. Failure to
// sync its parent makes durability indeterminate, so callers must keep the
// checkpoint and secondary the marker names and surface the error.
var ErrRestoredMarkerPublished = errors.New("RESTORED published but directory sync failed")

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
	// ReadIndexGeneration identifies the prepared secondary directory, including
	// an empty directory when a legacy backup requires a cold rebuild.
	ReadIndexGeneration string `json:"readIndexGeneration,omitempty"`
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
	return writeRestoredMarker(dataDir, marker, dal.FsyncDir)
}

func writeRestoredMarker(dataDir string, marker RestoredMarker, syncParent func(string) error) (retErr error) {
	data, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("marshaling restored marker: %w", err)
	}

	temp := filepath.Join(dataDir, ".restored-marker-"+rand.Text())
	file, err := os.OpenFile(temp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer func() {
		if temp == "" {
			return
		}
		if err := os.Remove(temp); err != nil && !errors.Is(err, os.ErrNotExist) {
			retErr = errors.Join(retErr, err)
		}
	}()
	if _, err = file.Write(data); err == nil {
		err = file.Sync()
	}
	if err = errors.Join(err, file.Close()); err != nil {
		return fmt.Errorf("writing restored marker: %w", err)
	}
	if err := os.Rename(temp, filepath.Join(dataDir, restoredMarkerFile)); err != nil {
		return fmt.Errorf("renaming restored marker into place: %w", err)
	}
	temp = "" // The name now belongs to RESTORED. Only pre-rename errors own a temp.
	if err := syncParent(dataDir); err != nil {
		return errors.Join(ErrRestoredMarkerPublished, err)
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
