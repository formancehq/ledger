package ctrl

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
)

// scanCompletedFiles examines targetDir and returns the relative paths of files
// that are fully received: present (not .tmp suffixed) and whose SHA256 matches
// the manifest entry.
func scanCompletedFiles(targetDir string, manifest *snapshotpb.SnapshotManifest) ([]string, error) {
	if manifest == nil {
		return nil, nil
	}

	var completed []string

	for _, entry := range manifest.GetFiles() {
		fullPath := filepath.Join(targetDir, entry.GetPath())

		info, err := os.Stat(fullPath)
		if err != nil {
			continue // file not present or not accessible
		}

		if info.Size() != int64(entry.GetSize()) {
			continue // size mismatch — incomplete or different file
		}

		hash, err := hashFileSHA256(fullPath)
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

// manifestTotalSize returns the sum of all file sizes in the manifest.
func manifestTotalSize(manifest *snapshotpb.SnapshotManifest) uint64 {
	var total uint64
	for _, e := range manifest.GetFiles() {
		total += e.GetSize()
	}

	return total
}

// hashFileSHA256 computes the SHA256 hex digest of a file.
func hashFileSHA256(path string) (string, error) {
	f, err := os.Open(path)
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
