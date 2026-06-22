package coldstorage

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
)

// ChecksumLength is the byte length of a SHA-256 digest.
const ChecksumLength = sha256.Size

// ErrChecksumNotFound is returned by ExpectedChecksum when the archive's
// persisted checksum is missing — typically because the upload was interrupted
// before the sidecar was committed. Callers should treat this as "archive not
// fully committed" and re-upload.
var ErrChecksumNotFound = errors.New("cold archive checksum not found")

// ErrChecksumMalformed is returned by ExpectedChecksum when the persisted
// checksum exists but has an unexpected byte length (not ChecksumLength). It
// indicates tampering or corruption of the checksum sidecar itself.
var ErrChecksumMalformed = errors.New("cold archive checksum has unexpected length")

// ComputeSHA256 reads all data from r and returns its SHA-256 digest.
func ComputeSHA256(r io.Reader) ([]byte, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return nil, err
	}

	return h.Sum(nil), nil
}

// Config configures the cold storage backend for chapter archival.
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

// ColdStorage provides an abstraction for archiving chapter data to durable external storage.
// Implementations include filesystem (dev/test) and S3 (production).
type ColdStorage interface {
	// Archive uploads the chapter archive (SST format) and persists its SHA-256
	// checksum atomically with the data. After this returns nil, both the
	// data and the checksum are durable. Implementations must guarantee that
	// a crash before the checksum becomes visible leaves Exists returning
	// false for this chapter, so the leader re-uploads on retry.
	Archive(ctx context.Context, bucketID string, chapterID uint64, data io.Reader, sha256 []byte) error

	// Exists returns true only when both the archive data and its persisted
	// checksum are present — i.e., the archive is fully committed. A partial
	// upload returns false so the caller treats it as "not yet archived".
	Exists(ctx context.Context, bucketID string, chapterID uint64) (bool, error)

	// ExpectedChecksum returns the SHA-256 stored alongside the archive at
	// upload time. Used as the reference value for integrity verification at
	// crash-recovery time. Returns ErrChecksumNotFound when the checksum is
	// missing (incomplete upload) and ErrChecksumMalformed when the stored
	// value has the wrong length.
	ExpectedChecksum(ctx context.Context, bucketID string, chapterID uint64) ([]byte, error)

	// Checksum reads the current archive bytes and computes SHA-256 on the
	// fly. Used to detect tampering or corruption when compared against
	// ExpectedChecksum.
	Checksum(ctx context.Context, bucketID string, chapterID uint64) ([]byte, error)

	// Fetch retrieves a previously archived SST file from cold storage.
	Fetch(ctx context.Context, bucketID string, chapterID uint64) (io.ReadCloser, error)
}
