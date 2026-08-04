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

//go:generate go tool mockgen -typed -write_source_comment=false -write_package_comment=false -source storage.go -destination storage_generated_test.go -package backup . Storage

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

// StorageConfig holds configuration for creating a Storage backend.
type StorageConfig struct {
	Driver string

	// S3 / S3-compatible (driver = "s3")
	S3Bucket          string
	S3Region          string
	S3Endpoint        string
	S3AccessKeyID     string
	S3SecretAccessKey string

	// Azure Blob Storage (driver = "azure")
	AzureAccountName string
	AzureAccountKey  string
	AzureContainer   string
	AzureEndpoint    string // overrides default; useful for Azurite
}

// NewStorage creates a Storage implementation based on cfg.Driver.
func NewStorage(cfg StorageConfig) (Storage, error) {
	switch cfg.Driver {
	case "s3":
		if cfg.S3Bucket == "" {
			return nil, errors.New("s3_bucket is required for s3 driver")
		}

		return NewS3BackupStorage(cfg.S3Bucket, cfg.S3Region, cfg.S3Endpoint, cfg.S3AccessKeyID, cfg.S3SecretAccessKey)
	case "azure":
		if cfg.AzureAccountName == "" {
			return nil, errors.New("azure_account_name is required for azure driver")
		}

		if cfg.AzureContainer == "" {
			return nil, errors.New("azure_container is required for azure driver")
		}

		return NewAzureBackupStorage(cfg.AzureAccountName, cfg.AzureAccountKey, cfg.AzureContainer, cfg.AzureEndpoint)
	default:
		return nil, fmt.Errorf("unsupported backup driver: %q (supported: \"s3\", \"azure\")", cfg.Driver)
	}
}
