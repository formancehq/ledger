package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// Storage provides file-level access to a backup destination.
// Keys use forward slashes as path separators regardless of the backend.
type Storage interface {
	PutFile(ctx context.Context, key string, data io.Reader, size int64) error
	GetFile(ctx context.Context, key string) (io.ReadCloser, error)
	DeleteFile(ctx context.Context, key string) error
}

// NewStorage creates a Storage implementation based on the driver name.
func NewStorage(driver, basePath, s3Bucket, s3Region, s3Endpoint string) (Storage, error) {
	switch driver {
	case "filesystem":
		if basePath == "" {
			return nil, errors.New("base_path is required for filesystem driver")
		}

		return NewFilesystemStorage(basePath), nil
	case "s3":
		if s3Bucket == "" {
			return nil, errors.New("s3_bucket is required for s3 driver")
		}

		return NewS3BackupStorage(s3Bucket, s3Region, s3Endpoint)
	default:
		return nil, fmt.Errorf("unknown backup driver: %q", driver)
	}
}
