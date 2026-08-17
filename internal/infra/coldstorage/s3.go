//go:build s3

package coldstorage

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// s3ChecksumMetadataKey is the user-metadata key that carries the SHA-256
// of the archive object, stored hex-encoded. S3 normalizes user-metadata
// keys to lowercase on read; we use lowercase here to keep that explicit.
const s3ChecksumMetadataKey = "sha256"

// NewS3Client creates an S3 client.
// When accessKeyID and secretAccessKey are both non-empty, static credentials are used.
// Otherwise the default AWS credential chain is used (env vars, ~/.aws/credentials, IAM role).
// If endpoint is non-empty, it is used as a custom S3 endpoint (e.g. for MinIO).
func NewS3Client(region, endpoint, accessKeyID, secretAccessKey string) (*s3.Client, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
	}

	if accessKeyID != "" && secretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, ""),
		))
	}

	cfg, err := awsconfig.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("loading AWS config: %w", err)
	}

	var s3Opts []func(*s3.Options)
	if endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	return s3.NewFromConfig(cfg, s3Opts...), nil
}

// S3Storage implements ColdStorage using Amazon S3 (or S3-compatible stores like MinIO).
type S3Storage struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

// NewS3Storage creates a new S3Storage backed by the given S3 client and bucket.
func NewS3Storage(client *s3.Client, bucket string) *S3Storage {
	return &S3Storage{client: client, uploader: manager.NewUploader(client), bucket: bucket}
}

func (s *S3Storage) archiveKey(bucketID string, chapterID uint64) string {
	return fmt.Sprintf("%s/chapters/%d/archive.sst", bucketID, chapterID)
}

func (s *S3Storage) Archive(ctx context.Context, bucketID string, chapterID uint64, data io.Reader, sha256 []byte) error {
	if len(sha256) != ChecksumLength {
		return fmt.Errorf("archive: invalid checksum length %d, expected %d", len(sha256), ChecksumLength)
	}

	key := s.archiveKey(bucketID, chapterID)

	// Multipart upload: streams the archive in bounded parts, lifting the 5 GB
	// single-PutObject limit so multi-GB chapter SSTs upload without buffering.
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        data,
		ContentType: aws.String("application/octet-stream"),
		Metadata: map[string]string{
			s3ChecksumMetadataKey: hex.EncodeToString(sha256),
		},
	})
	if err != nil {
		return fmt.Errorf("s3 upload %s: %w", key, err)
	}

	return nil
}

// headObject returns the HeadObject output or nil/false if the object is
// absent. Any other S3 error is returned wrapped.
func (s *S3Storage) headObject(ctx context.Context, key string) (*s3.HeadObjectOutput, bool, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var (
			noSuchKey *types.NoSuchKey
			notFound  *types.NotFound
		)

		if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("s3 HeadObject %s: %w", key, err)
	}

	return out, true, nil
}

func (s *S3Storage) Exists(ctx context.Context, bucketID string, chapterID uint64) (bool, error) {
	key := s.archiveKey(bucketID, chapterID)

	out, ok, err := s.headObject(ctx, key)
	if err != nil || !ok {
		return false, err
	}

	// The object exists in S3, but treat it as "not committed" unless its
	// checksum metadata is also present — same semantics as the filesystem
	// backend with its sidecar.
	if _, hasChecksum := out.Metadata[s3ChecksumMetadataKey]; !hasChecksum {
		return false, nil
	}

	return true, nil
}

func (s *S3Storage) ExpectedChecksum(ctx context.Context, bucketID string, chapterID uint64) ([]byte, error) {
	key := s.archiveKey(bucketID, chapterID)

	out, ok, err := s.headObject(ctx, key)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, ErrChecksumNotFound
	}

	hexed, present := out.Metadata[s3ChecksumMetadataKey]
	if !present || hexed == "" {
		return nil, ErrChecksumNotFound
	}

	checksum, err := hex.DecodeString(hexed)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid hex %q: %w", ErrChecksumMalformed, hexed, err)
	}

	if len(checksum) != ChecksumLength {
		return nil, fmt.Errorf("%w: got %d bytes", ErrChecksumMalformed, len(checksum))
	}

	return checksum, nil
}

func (s *S3Storage) Checksum(ctx context.Context, bucketID string, chapterID uint64) ([]byte, error) {
	body, err := s.Fetch(ctx, bucketID, chapterID)
	if err != nil {
		return nil, fmt.Errorf("fetching archive for checksum: %w", err)
	}

	defer func() { _ = body.Close() }()

	return ComputeSHA256(body)
}

func (s *S3Storage) Fetch(ctx context.Context, bucketID string, chapterID uint64) (io.ReadCloser, error) {
	key := s.archiveKey(bucketID, chapterID)

	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 GetObject %s: %w", key, err)
	}

	return output.Body, nil
}

// Ensure S3Storage implements ColdStorage.
var _ ColdStorage = (*S3Storage)(nil)

// NewS3ColdStorage creates a ColdStorage backed by S3.
// This is the public entry point used by bootstrap; it hides S3-specific types.
func NewS3ColdStorage(bucket, region, endpoint string) (ColdStorage, error) {
	client, err := NewS3Client(region, endpoint, "", "")
	if err != nil {
		return nil, err
	}

	return NewS3Storage(client, bucket), nil
}
