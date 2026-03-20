package coldstorage

import (
	"context"
	"io"
)

// Config configures the cold storage backend for period archival.
type Config struct {
	Driver   string // "filesystem" (default) or "s3"
	BasePath string // filesystem driver base path
	BucketID string // shared namespace prefix for cold storage archives (default: cluster-id)
	CacheDir string // directory for cold reader cache (default: <data-dir>/cold-cache)
	// S3-specific
	S3Bucket   string // S3 bucket name (required when driver=s3)
	S3Region   string // AWS region
	S3Endpoint string // custom S3 endpoint (for MinIO)
}

// ColdStorage provides an abstraction for archiving period data to durable external storage.
// Implementations include filesystem (dev/test) and S3 (production, deferred).
type ColdStorage interface {
	// Archive writes period archive data (SST format) to cold storage.
	Archive(ctx context.Context, bucketID string, periodID uint64, data io.Reader) error

	// Exists checks whether an archive for the given period exists in cold storage.
	Exists(ctx context.Context, bucketID string, periodID uint64) (bool, error)

	// Fetch retrieves a previously archived SST file from cold storage.
	Fetch(ctx context.Context, bucketID string, periodID uint64) (io.ReadCloser, error)
}
