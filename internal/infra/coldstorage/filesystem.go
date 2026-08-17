package coldstorage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

// Ensure FilesystemStorage implements ColdStorage.
var _ ColdStorage = (*FilesystemStorage)(nil)
