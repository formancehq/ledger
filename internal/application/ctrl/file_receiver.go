package ctrl

import (
	"fmt"
	"path/filepath"

	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

// validateSnapshotManifest rejects network-supplied paths that could escape
// the follower staging root before any file is requested or written.
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
