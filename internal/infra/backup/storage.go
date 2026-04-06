package backup

import (
	"context"
	"io"
)

// Config configures the scheduled backup system.
type Config struct {
	Driver     string // "filesystem" or "s3" (empty = disabled)
	Schedule   string // cron expression for backup schedule
	BasePath   string // filesystem driver base path
	BucketID   string // namespace prefix (default: cluster-id)
	S3Bucket   string // S3 bucket name (required when driver=s3)
	S3Region   string // AWS region
	S3Endpoint string // custom S3 endpoint (for MinIO)
}

// Storage provides file-level access to a backup destination.
// Keys use forward slashes as path separators regardless of the backend.
type Storage interface {
	PutFile(ctx context.Context, key string, data io.Reader, size int64) error
	GetFile(ctx context.Context, key string) (io.ReadCloser, error)
	DeleteFile(ctx context.Context, key string) error
}
