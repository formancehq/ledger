package ctrl

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

// scanCompletedFiles examines targetDir and returns the relative paths of files
// that are fully received: present (not .tmp suffixed) and whose SHA256 matches
// the manifest entry.
func scanCompletedFiles(targetDir string, manifest *snapshotpb.SnapshotManifest) ([]string, error) {
	if manifest == nil {
		return nil, nil
	}

	if err := validateSnapshotManifest(manifest); err != nil {
		return nil, err
	}

	root, err := os.OpenRoot(targetDir)
	if err != nil {
		return nil, fmt.Errorf("opening snapshot target directory: %w", err)
	}

	defer func() {
		_ = root.Close()
	}()

	var completed []string

	for _, entry := range manifest.GetFiles() {
		info, err := root.Stat(entry.GetPath())
		if err != nil {
			continue // file not present or not accessible
		}

		if info.Size() != int64(entry.GetSize()) {
			continue // size mismatch — incomplete or different file
		}

		hash, err := hashFileSHA256(root, entry.GetPath())
		if err != nil {
			continue // can't hash — treat as incomplete
		}

		if hash != entry.GetSha256() {
			continue // content mismatch — discard on next cleanup
		}

		completed = append(completed, entry.GetPath())
	}

	return completed, nil
}

// validateSnapshotManifest rejects paths that would escape the follower's
// staging root before any file is requested or written.
func validateSnapshotManifest(manifest *snapshotpb.SnapshotManifest) error {
	if manifest == nil {
		return nil
	}

	for i, entry := range manifest.GetFiles() {
		if !filepath.IsLocal(entry.GetPath()) {
			return fmt.Errorf("invalid snapshot path at manifest entry %d: %q", i, entry.GetPath())
		}
	}

	return nil
}

// manifestTotalSize returns the sum of all file sizes in the manifest.
func manifestTotalSize(manifest *snapshotpb.SnapshotManifest) uint64 {
	var total uint64
	for _, e := range manifest.GetFiles() {
		total += e.GetSize()
	}

	return total
}

// hashFileSHA256 computes the SHA256 hex digest of a file beneath root.
func hashFileSHA256(root *os.Root, path string) (string, error) {
	f, err := root.Open(path)
	if err != nil {
		return "", err
	}

	defer func() {
		_ = f.Close()
	}()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
