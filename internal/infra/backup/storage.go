package backup

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// ErrFileNotFound is returned by Storage.GetFile when the key does not exist.
// Implementations must wrap their backend's not-found error so that
// errors.Is(err, ErrFileNotFound) holds.
var ErrFileNotFound = errors.New("backup: file not found")

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source storage.go -destination storage_generated_test.go -package backup . Storage

// Storage provides file-level access to a backup destination.
// Keys use forward slashes as path separators regardless of the backend.
type Storage interface {
	PutFile(ctx context.Context, key string, data io.Reader, size int64) error
	// GetFile returns the object at key. If the key does not exist, it returns
	// an error satisfying errors.Is(err, ErrFileNotFound).
	GetFile(ctx context.Context, key string) (io.ReadCloser, error)
	DeleteFile(ctx context.Context, key string) error
	// ListFiles returns the keys of every object whose key starts with prefix.
	// The order of returned keys is unspecified.
	ListFiles(ctx context.Context, prefix string) ([]string, error)
}

// NewStorage creates a Storage implementation based on the driver name.
func NewStorage(driver, basePath, s3Bucket, s3Region, s3Endpoint, s3AccessKeyID, s3SecretAccessKey string) (Storage, error) {
	switch driver {
	case "s3":
		if s3Bucket == "" {
			return nil, errors.New("s3_bucket is required for s3 driver")
		}

		return NewS3BackupStorage(s3Bucket, s3Region, s3Endpoint, s3AccessKeyID, s3SecretAccessKey)
	default:
		return nil, fmt.Errorf("unsupported backup driver: %q (only \"s3\" is supported)", driver)
	}
}
