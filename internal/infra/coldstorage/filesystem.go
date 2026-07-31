package coldstorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

// FilesystemStorage implements ColdStorage using the local filesystem.
// Intended for development and testing; production should use S3 or similar.
//
// Layout per chapter:
//
//	{basePath}/{bucketID}/chapters/{chapterID}/
//	  archive.sst             — data
//	  archive.sst.sha256      — 32-byte SHA-256 sidecar (presence = upload complete)
//
// Atomicity: both data and sidecar are written to .tmp paths, fsynced, then
// renamed into place. The data file is committed before the sidecar is
// touched, so the presence of the sidecar is a reliable "fully committed"
// marker.
type FilesystemStorage struct {
	basePath string
}

const (
	archiveDataName     = "archive.sst"
	archiveChecksumName = "archive.sst.sha256"
)

// NewFilesystemStorage creates a new FilesystemStorage rooted at basePath.
func NewFilesystemStorage(basePath string) *FilesystemStorage {
	return &FilesystemStorage{basePath: basePath}
}

func (f *FilesystemStorage) archiveDir(bucketID string, chapterID uint64) string {
	return filepath.Join(f.basePath, bucketID, "chapters", strconv.FormatUint(chapterID, 10))
}

func (f *FilesystemStorage) archivePath(bucketID string, chapterID uint64) string {
	return filepath.Join(f.archiveDir(bucketID, chapterID), archiveDataName)
}

func (f *FilesystemStorage) checksumPath(bucketID string, chapterID uint64) string {
	return filepath.Join(f.archiveDir(bucketID, chapterID), archiveChecksumName)
}

func (f *FilesystemStorage) Archive(_ context.Context, bucketID string, chapterID uint64, data io.Reader, sha256 []byte) error {
	if len(sha256) != ChecksumLength {
		return fmt.Errorf("archive: invalid checksum length %d, expected %d", len(sha256), ChecksumLength)
	}

	dir := f.archiveDir(bucketID, chapterID)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("creating archive directory: %w", err)
	}

	dataPath := filepath.Join(dir, archiveDataName)
	checksumPath := filepath.Join(dir, archiveChecksumName)

	if err := writeAndRename(dir, dataPath, func(w io.Writer) error {
		_, err := io.Copy(w, data)

		return err
	}); err != nil {
		return fmt.Errorf("writing archive data: %w", err)
	}

	if err := writeAndRename(dir, checksumPath, func(w io.Writer) error {
		_, err := w.Write(sha256)

		return err
	}); err != nil {
		return fmt.Errorf("writing archive checksum sidecar: %w", err)
	}

	return nil
}

// writeAndRename writes content via writeFn to a sibling .tmp file, fsyncs it
// and its containing directory, then atomically renames it to finalPath and
// fsyncs the directory again. Any pre-existing .tmp at the target path is
// truncated.
func writeAndRename(dir, finalPath string, writeFn func(io.Writer) error) error {
	tmpPath := finalPath + ".tmp"

	tmp, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("creating tmp file %s: %w", tmpPath, err)
	}

	if err := writeFn(tmp); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)

		return err
	}

	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)

		return fmt.Errorf("fsyncing tmp file %s: %w", tmpPath, err)
	}

	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)

		return fmt.Errorf("closing tmp file %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)

		return fmt.Errorf("renaming %s to %s: %w", tmpPath, finalPath, err)
	}

	if err := fsyncDir(dir); err != nil {
		return fmt.Errorf("fsyncing directory %s: %w", dir, err)
	}

	return nil
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}

	defer func() { _ = d.Close() }()

	return d.Sync()
}

func (f *FilesystemStorage) Exists(_ context.Context, bucketID string, chapterID uint64) (bool, error) {
	dataOK, err := fileExists(f.archivePath(bucketID, chapterID))
	if err != nil {
		return false, err
	}

	if !dataOK {
		return false, nil
	}

	return fileExists(f.checksumPath(bucketID, chapterID))
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}

		return false, fmt.Errorf("checking %s: %w", path, err)
	}

	return true, nil
}

func (f *FilesystemStorage) ExpectedChecksum(_ context.Context, bucketID string, chapterID uint64) ([]byte, error) {
	path := f.checksumPath(bucketID, chapterID)

	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrChecksumNotFound
		}

		return nil, fmt.Errorf("reading checksum sidecar %s: %w", path, err)
	}

	if len(b) != ChecksumLength {
		return nil, fmt.Errorf("%w: got %d bytes", ErrChecksumMalformed, len(b))
	}

	return b, nil
}

func (f *FilesystemStorage) Checksum(_ context.Context, bucketID string, chapterID uint64) ([]byte, error) {
	path := f.archivePath(bucketID, chapterID)

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening archive for checksum: %w", err)
	}

	defer func() { _ = file.Close() }()

	return ComputeSHA256(file)
}

func (f *FilesystemStorage) Fetch(_ context.Context, bucketID string, chapterID uint64) (io.ReadCloser, error) {
	path := f.archivePath(bucketID, chapterID)

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("opening archive file: %w", err)
	}

	return file, nil
}

// List implements ObjectCatalog with a deterministic lexical cursor. It lists
// archive data files even when the checksum sidecar is absent so interrupted
// filesystem publications remain discoverable for later reclamation.
func (f *FilesystemStorage) List(
	ctx context.Context,
	bucketPrefix string,
	cursor string,
	limit int,
) (ArchiveObjectPage, error) {
	if limit <= 0 {
		return ArchiveObjectPage{}, fmt.Errorf("cold storage list limit must be positive: %d", limit)
	}
	prefix, err := normalizeBucketPrefix(bucketPrefix)
	if err != nil {
		return ArchiveObjectPage{}, err
	}
	root := filepath.Join(f.basePath, filepath.FromSlash(prefix))
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ArchiveObjectPage{}, nil
		}

		return ArchiveObjectPage{}, fmt.Errorf("stating cold storage list prefix %s: %w", root, err)
	}

	type listedObject struct {
		key    string
		object ArchiveObject
	}
	objects := make([]listedObject, 0)
	err = filepath.WalkDir(root, func(filePath string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() != archiveDataName {
			return nil
		}

		relative, err := filepath.Rel(f.basePath, filePath)
		if err != nil {
			return fmt.Errorf("resolving cold storage archive path %s: %w", filePath, err)
		}
		object, ok := parseArchiveObjectKey(filepath.ToSlash(relative))
		if !ok || !bucketWithinPrefix(object.BucketID, prefix) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("stating cold storage archive object %s: %w", filePath, err)
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		object.Size = info.Size()
		object.LastModified = info.ModTime()
		objects = append(objects, listedObject{key: archiveObjectKey(object.BucketID, object.ChapterID), object: object})

		return nil
	})
	if err != nil {
		return ArchiveObjectPage{}, fmt.Errorf("listing filesystem cold storage prefix %s: %w", prefix, err)
	}

	sort.Slice(objects, func(i, j int) bool { return objects[i].key < objects[j].key })
	start := sort.Search(len(objects), func(index int) bool { return objects[index].key > cursor })
	end := min(start+limit, len(objects))
	page := ArchiveObjectPage{Objects: make([]ArchiveObject, 0, end-start)}
	for _, object := range objects[start:end] {
		page.Objects = append(page.Objects, object.object)
	}
	if end < len(objects) {
		page.NextCursor = objects[end-1].key
	}

	return page, nil
}

// Delete removes one exact chapter object. The checksum commit marker is
// removed first, making an interrupted deletion read as unavailable; retries
// then finish removing the remaining data and temporary files.
func (f *FilesystemStorage) Delete(ctx context.Context, bucketID string, chapterID uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	normalized, err := normalizeBucketPrefix(bucketID)
	if err != nil || normalized != bucketID {
		return fmt.Errorf("invalid cold storage bucket id %q", bucketID)
	}

	dir := f.archiveDir(bucketID, chapterID)
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return fmt.Errorf("stating cold storage object directory %s: %w", dir, err)
	}
	for _, filePath := range []string{f.checksumPath(bucketID, chapterID), f.checksumPath(bucketID, chapterID) + ".tmp"} {
		if err := removeFileIfExists(filePath); err != nil {
			return err
		}
	}
	if err := fsyncDir(dir); err != nil {
		return fmt.Errorf("syncing cold storage object directory after commit-marker deletion: %w", err)
	}
	for _, filePath := range []string{f.archivePath(bucketID, chapterID), f.archivePath(bucketID, chapterID) + ".tmp"} {
		if err := removeFileIfExists(filePath); err != nil {
			return err
		}
	}
	if err := fsyncDir(dir); err != nil {
		return fmt.Errorf("syncing cold storage object directory after data deletion: %w", err)
	}
	if err := removeDirIfEmpty(dir); err != nil {
		return err
	}
	if err := removeDirIfEmpty(filepath.Dir(dir)); err != nil {
		return err
	}

	return nil
}

func removeFileIfExists(filePath string) error {
	if err := os.Remove(filePath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("deleting cold storage object file %s: %w", filePath, err)
	}

	return nil
}

func removeDirIfEmpty(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}

		return fmt.Errorf("reading cold storage object directory %s: %w", dir, err)
	}
	if len(entries) != 0 {
		return nil
	}
	if err := os.Remove(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("deleting empty cold storage object directory %s: %w", dir, err)
	}
	parent := filepath.Dir(dir)
	if err := fsyncDir(parent); err != nil {
		return fmt.Errorf("syncing cold storage object parent directory %s: %w", parent, err)
	}

	return nil
}

// Ensure FilesystemStorage implements ColdStorage.
var _ ColdStorage = (*FilesystemStorage)(nil)
var _ ObjectCatalog = (*FilesystemStorage)(nil)
