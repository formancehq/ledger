package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sync/errgroup"
)

// A portable read-store checkpoint consists of flat Pebble files. Accept the
// names Pebble emits on every platform and exclude local control files (e.g.
// ".ready") and aliases such as trailing dots, ".." and Windows path syntax.
func validReadIndexFilename(name string) bool {
	if !filepath.IsLocal(name) || strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") {
		return false
	}
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '-', c == '_':
		default:
			return false
		}
	}

	return true
}

// ReadIndexDownloadOptions controls parallel downloads and optional reporting.
// FileVerified may run concurrently and counts only completely verified files.
type ReadIndexDownloadOptions struct {
	Parallelism  int
	FileVerified func(size int64)
}

// DownloadReadIndex stages a versioned read-store seed in its own newly
// created directory. Each final open is confined by dest; O_EXCL prevents
// replacing files through existing names, symlinks or case-folding aliases.
// On error the caller must discard the entire directory before retrying.
func DownloadReadIndex(ctx context.Context, storage Storage, bucketID string, checkpoint *CheckpointManifest, dest *os.Root, options ReadIndexDownloadOptions) error {
	if checkpoint == nil || checkpoint.ReadIndex == nil {
		return nil
	}
	seed := checkpoint.ReadIndex
	if seed.Version != ReadIndexVersion {
		return fmt.Errorf("unsupported read index checkpoint version %d", seed.Version)
	}
	if len(seed.Files) == 0 {
		return errors.New("empty read index checkpoint file inventory")
	}
	if len(seed.Files) > MaxRestoreManifestFiles {
		return fmt.Errorf("read index backup declares %d files (limit %d)", len(seed.Files), MaxRestoreManifestFiles)
	}
	if len(checkpoint.Files) > MaxRestoreManifestFiles {
		return fmt.Errorf("primary backup declares %d files (limit %d)", len(checkpoint.Files), MaxRestoreManifestFiles)
	}
	if err := checkpoint.verifyReadIndexBinding(); err != nil {
		return err
	}
	if options.Parallelism <= 0 {
		return errors.New("read index download parallelism must be positive")
	}
	for name, cf := range seed.Files {
		if !validReadIndexFilename(name) {
			return fmt.Errorf("invalid read index checkpoint filename %q", name)
		}
		hash, ok := checkpointKeyHash(cf.Key)
		if !ok || cf.Size < 0 || cf.Key != CheckpointFileKey(bucketID, name, hash) {
			return fmt.Errorf("%w: invalid read index object for %q", ErrCheckpointFileCorrupted, name)
		}
	}

	group, gctx := errgroup.WithContext(ctx)
	group.SetLimit(options.Parallelism)
	for name, cf := range seed.Files {
		group.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}
			if err := downloadReadIndexFile(gctx, storage, dest, name, cf); err != nil {
				return fmt.Errorf("downloading read index file %q: %w", name, err)
			}
			if options.FileVerified != nil {
				options.FileVerified(cf.Size)
			}

			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return err
	}
	dir, err := dest.Open(".")
	if err != nil {
		return err
	}

	return errors.Join(dir.Sync(), dir.Close())
}

func downloadReadIndexFile(ctx context.Context, storage Storage, dest *os.Root, name string, cf CheckpointFile) error {
	src, err := storage.GetFile(ctx, cf.Key)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }() // CopyVerified reports read errors.
	out, err := dest.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	_, err = cf.CopyVerified(out, src)
	if err == nil {
		err = out.Sync()
	}

	return errors.Join(err, out.Close())
}
