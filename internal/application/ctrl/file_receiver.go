package ctrl

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	pathpkg "path"
	"path/filepath"

	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

// validateSnapshotManifest rejects network-supplied paths that could escape
// the follower staging root, name the same file twice, or collide with another
// entry's staging path. The whole manifest is validated before any file is
// requested or written.
func validateSnapshotManifest(manifest *snapshotpb.SnapshotManifest) error {
	if manifest == nil {
		return errors.New("snapshot manifest is missing")
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

// verifyStagingMatchesManifest fails unless targetDir holds exactly the files
// the manifest lists, plus the parent directories they need. Retries already
// reset staging before each session; this check enforces the result before
// the checkpoint is handed to Pebble, which would otherwise open a stray
// MANIFEST, WAL or marker left by any earlier writer. The walk goes through a
// root-scoped filesystem so a symlink cannot extend it outside staging.
func verifyStagingMatchesManifest(targetDir string, manifest *snapshotpb.SnapshotManifest) error {
	expected := make(map[string]struct{}, len(manifest.GetFiles()))
	parents := make(map[string]struct{})
	for _, entry := range manifest.GetFiles() {
		path := filepath.ToSlash(filepath.Clean(entry.GetPath()))
		expected[path] = struct{}{}
		for dir := pathpkg.Dir(path); dir != "."; dir = pathpkg.Dir(dir) {
			parents[dir] = struct{}{}
		}
	}

	root, err := os.OpenRoot(targetDir)
	if err != nil {
		return fmt.Errorf("opening snapshot staging root: %w", err)
	}
	defer func() { _ = root.Close() }()

	seen := 0
	err = fs.WalkDir(root.FS(), ".", func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == "." {
			return nil
		}

		if d.IsDir() {
			if _, ok := parents[path]; !ok {
				return fmt.Errorf("unexpected directory %q in snapshot staging", path)
			}

			return nil
		}

		if _, ok := expected[path]; !ok || !d.Type().IsRegular() {
			return fmt.Errorf("unexpected entry %q (mode %s) in snapshot staging", path, d.Type())
		}
		seen++

		return nil
	})
	if err != nil {
		return fmt.Errorf("verifying snapshot staging against manifest: %w", err)
	}

	if seen != len(expected) {
		return fmt.Errorf("snapshot staging holds %d of the %d manifest files", seen, len(expected))
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
