package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// FilesystemStorage implements Storage using the local filesystem.
type FilesystemStorage struct {
	basePath string
}

// NewFilesystemStorage creates a new FilesystemStorage rooted at basePath.
func NewFilesystemStorage(basePath string) *FilesystemStorage {
	return &FilesystemStorage{basePath: basePath}
}

func (f *FilesystemStorage) localPath(key string) string {
	return filepath.Join(f.basePath, filepath.FromSlash(key))
}

func (f *FilesystemStorage) PutFile(_ context.Context, key string, data io.Reader, _ int64) error {
	path := f.localPath(key)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("creating directory for %s: %w", key, err)
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("creating file %s: %w", key, err)
	}

	defer func() { _ = file.Close() }()

	if _, err := io.Copy(file, data); err != nil {
		return fmt.Errorf("writing file %s: %w", key, err)
	}

	return file.Close()
}

func (f *FilesystemStorage) GetFile(_ context.Context, key string) (io.ReadCloser, error) {
	file, err := os.Open(f.localPath(key))
	if err != nil {
		return nil, fmt.Errorf("opening file %s: %w", key, err)
	}

	return file, nil
}

func (f *FilesystemStorage) DeleteFile(_ context.Context, key string) error {
	if err := os.Remove(f.localPath(key)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("deleting file %s: %w", key, err)
	}

	return nil
}

var _ Storage = (*FilesystemStorage)(nil)
