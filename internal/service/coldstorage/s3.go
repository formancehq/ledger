package coldstorage

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// NewS3Client creates an S3 client using the default AWS credential chain.
// If endpoint is non-empty, it is used as a custom S3 endpoint (e.g. for MinIO).
func NewS3Client(region, endpoint string) (*s3.Client, error) {
	var opts []func(*awsconfig.LoadOptions) error
	if region != "" {
		opts = append(opts, awsconfig.WithRegion(region))
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
	client *s3.Client
	bucket string
}

// NewS3Storage creates a new S3Storage backed by the given S3 client and bucket.
func NewS3Storage(client *s3.Client, bucket string) *S3Storage {
	return &S3Storage{client: client, bucket: bucket}
}

func (s *S3Storage) archiveKey(bucketID string, periodID uint64) string {
	return fmt.Sprintf("%s/periods/%d/archive.tar.gz", bucketID, periodID)
}

func (s *S3Storage) Archive(ctx context.Context, bucketID string, periodID uint64, data io.Reader) error {
	key := s.archiveKey(bucketID, periodID)
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        data,
		ContentType: aws.String("application/gzip"),
	})
	if err != nil {
		return fmt.Errorf("s3 PutObject %s: %w", key, err)
	}
	return nil
}

func (s *S3Storage) Exists(ctx context.Context, bucketID string, periodID uint64) (bool, error) {
	key := s.archiveKey(bucketID, periodID)
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var noSuchKey *types.NoSuchKey
		var notFound *types.NotFound
		if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
			return false, nil
		}
		return false, fmt.Errorf("s3 HeadObject %s: %w", key, err)
	}
	return true, nil
}

// Ensure S3Storage implements ColdStorage.
var _ ColdStorage = (*S3Storage)(nil)
