package coldstorage

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

// FilesystemStorage implements ColdStorage using the local filesystem.
// Intended for development and testing; production should use S3 or similar.
type FilesystemStorage struct {
	basePath string
}

// NewFilesystemStorage creates a new FilesystemStorage rooted at basePath.
func NewFilesystemStorage(basePath string) *FilesystemStorage {
	return &FilesystemStorage{basePath: basePath}
}

func (f *FilesystemStorage) archivePath(bucketID string, periodID uint64) string {
	return filepath.Join(f.basePath, bucketID, "periods", strconv.FormatUint(periodID, 10), "archive.tar.gz")
}

func (f *FilesystemStorage) Archive(_ context.Context, bucketID string, periodID uint64, data io.Reader) error {
	path := f.archivePath(bucketID, periodID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating archive directory: %w", err)
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating archive file: %w", err)
	}

	defer func() { _ = file.Close() }()

	if _, err := io.Copy(file, data); err != nil {
		return fmt.Errorf("writing archive data: %w", err)
	}

	return file.Close()
}

func (f *FilesystemStorage) Exists(_ context.Context, bucketID string, periodID uint64) (bool, error) {
	path := f.archivePath(bucketID, periodID)

	_, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, fmt.Errorf("checking archive existence: %w", err)
	}

	return true, nil
}

// Ensure FilesystemStorage implements ColdStorage.
var _ ColdStorage = (*FilesystemStorage)(nil)
