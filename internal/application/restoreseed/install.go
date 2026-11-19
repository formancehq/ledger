package restoreseed

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"syscall"

	"github.com/google/uuid"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Install publishes the committed generation before either database or Raft
// opens. A sibling temporary directory keeps a cross-device copy invisible
// until complete. Retrying after publication recognizes the exact generation.
func Install(dataDir, indexDir, id string) error {
	if id == "" {
		return nil
	}
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.String() != id {
		return fmt.Errorf("invalid restored read-index generation %q", id)
	}
	dir := seedPath(dataDir)
	if err := os.MkdirAll(indexDir, 0o750); err != nil {
		return err
	}
	dest := filepath.Join(indexDir, "readindex")
	info, err := os.Lstat(dest)
	installed := err == nil && info.IsDir() && matchesGeneration(dest, id)
	if !installed {
		switch {
		case err == nil:
			// An empty mount subdirectory is fresh. Remove may only succeed for an
			// empty directory; never recursively clear a foreign read store.
			if !info.IsDir() || os.Remove(dest) != nil {
				return fmt.Errorf("read-index target %s already exists and does not belong to this restore; select a fresh read-index directory", dest)
			}
		case !errors.Is(err, os.ErrNotExist):
			return err
		}
		if !matchesGeneration(dir, id) {
			return fmt.Errorf("missing or mismatched read-index seed for restored generation %s", id)
		}
		if err := os.Rename(dir, dest); err != nil {
			if !errors.Is(err, syscall.EXDEV) {
				return fmt.Errorf("installing read-index seed: %w", err)
			}
			if err := copyAcrossDevices(dir, indexDir, id); err != nil {
				return fmt.Errorf("copying read-index seed to configured volume: %w", err)
			}
		}
	}
	if err := dal.FsyncDir(indexDir); err != nil {
		return err
	}
	if err := dal.FsyncDir(filepath.Dir(indexDir)); err != nil {
		return err
	}
	if err := Discard(dataDir); err != nil {
		return err
	}

	return dal.FsyncDir(dataDir)
}

func matchesGeneration(dir, id string) bool { return hasTag(dir, generationFile, id) }

func hasTag(dir, file, id string) bool {
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() {
		return false
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return false
	}
	defer func() { _ = root.Close() }()
	info, err = root.Lstat(file)
	if err != nil || !info.Mode().IsRegular() || info.Size() != int64(len(id)) {
		return false
	}
	got, err := root.ReadFile(file)

	return err == nil && string(got) == id
}

func copyAcrossDevices(srcDir, indexDir, id string) (retErr error) {
	temp := filepath.Join(indexDir, ".restore-"+id+".tmp")
	if _, err := os.Lstat(temp); errors.Is(err, os.ErrNotExist) {
		if err := reserveDirectory(indexDir, filepath.Base(temp), generationFile, id, ""); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else if !matchesGeneration(temp, id) {
		return fmt.Errorf("temporary read-index destination belongs to another generation: %s", temp)
	}
	src, err := os.OpenRoot(srcDir)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, src.Close()) }()
	dst, err := os.OpenRoot(temp)
	if err != nil {
		return err
	}
	err = errors.Join(syncOrCopyTree(context.Background(), src, dst), dst.Close())
	if err != nil {
		return err
	}

	return os.Rename(temp, filepath.Join(indexDir, "readindex"))
}

// Both trees are private. Operate through root capabilities even on individual
// Pebble filenames originating from object storage. Sync directories after
// their children; every file reaches disk before any later marker or rename.
func syncOrCopyTree(ctx context.Context, src, dst *os.Root) error {
	var dirs []string
	err := fs.WalkDir(src.FS(), ".", func(name string, d fs.DirEntry, walkErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if walkErr != nil {
			return walkErr
		}
		name = filepath.FromSlash(name)
		if d.IsDir() {
			dirs = append(dirs, name)
			if dst != nil && name != "." {
				return dst.MkdirAll(name, 0o750)
			}

			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("unexpected file type %s in read-index seed: %s", d.Type(), name)
		}
		if dst != nil {
			info, err := d.Info()
			if err != nil {
				return err
			}

			return copySeedFile(src, dst, name, info.Size())
		}
		f, err := src.Open(name)
		if err != nil {
			return err
		}

		return errors.Join(f.Sync(), f.Close())
	})
	if err != nil {
		return err
	}
	if dst == nil {
		dst = src
	}
	for _, dir := range slices.Backward(dirs) {
		if err := ctx.Err(); err != nil {
			return err
		}
		f, err := dst.Open(dir)
		if err != nil {
			return err
		}
		if err := errors.Join(f.Sync(), f.Close()); err != nil {
			return err
		}
	}

	return nil
}

// Each completed file is synced and atomically renamed. On a startup-probe
// restart the privately named generation can resume at the first missing SST.
// No partial file ever carries its final name.
func copySeedFile(src, dst *os.Root, name string, size int64) (retErr error) {
	existing, err := dst.Lstat(name)
	if err == nil {
		if !existing.Mode().IsRegular() || existing.Size() != size {
			return fmt.Errorf("incomplete or foreign read-index copy file %s", name)
		}

		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	const partial = ".restore-copy-partial"
	if err := dst.Remove(partial); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	in, err := src.Open(name)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, in.Close()) }()
	out, err := dst.OpenFile(partial, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	n, err := io.Copy(out, in)
	if n != size && err == nil {
		err = io.ErrUnexpectedEOF
	}
	if err = errors.Join(err, out.Sync(), out.Close()); err != nil {
		return errors.Join(err, dst.Remove(partial))
	}
	if err := dst.Rename(partial, name); err != nil {
		return errors.Join(err, dst.Remove(partial))
	}
	dir, err := dst.Open(filepath.Dir(name))
	if err != nil {
		return err
	}

	return errors.Join(dir.Sync(), dir.Close())
}
