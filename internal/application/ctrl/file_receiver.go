package ctrl

import (
	"fmt"
	"path/filepath"

	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

// validateSnapshotManifest rejects network-supplied paths that could escape
// the follower staging root, name the same file twice, or collide with another
// entry's staging path. The whole manifest is validated before any file is
// requested or written.
func validateSnapshotManifest(manifest *snapshotpb.SnapshotManifest) error {
	if manifest == nil {
		return nil
	}

	files := manifest.GetFiles()

	// Both maps are keyed by the cleaned name the staging root resolves, so
	// that entries such as "a" and "./a" are recognized as the same file. The
	// staging key is built the way the fetcher builds it, from the raw path
	// plus the suffix, so it names the file the transfer really opens.
	finalPaths := make(map[string]int, len(files))
	stagingPaths := make(map[string]int, len(files))

	for i, entry := range files {
		if !filepath.IsLocal(entry.GetPath()) {
			return fmt.Errorf("invalid snapshot path at manifest entry %d: %q", i, entry.GetPath())
		}

		path := filepath.Clean(entry.GetPath())
		if first, ok := finalPaths[path]; ok {
			return fmt.Errorf("duplicate snapshot path at manifest entries %d and %d: %q", first, i, entry.GetPath())
		}

		finalPaths[path] = i
		stagingPaths[filepath.Clean(entry.GetPath()+stagingSuffix)] = i
	}

	// Downloads run in parallel and each one writes through path+stagingSuffix
	// before renaming that name onto its final path. With both "a" and "a.tmp"
	// in the manifest, the "a.tmp" transfer can rename its own bytes onto the
	// staging file the "a" transfer already opened; "a" would then be installed
	// from bytes that were never verified against its entry, and both transfers
	// would still report success.
	for i, entry := range files {
		path := filepath.Clean(entry.GetPath())
		if other, ok := stagingPaths[path]; ok {
			return fmt.Errorf("snapshot path at manifest entry %d collides with the staging path of entry %d: %q", i, other, entry.GetPath())
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
