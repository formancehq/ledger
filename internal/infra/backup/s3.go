//go:build s3

package backup

import (
	"context"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
)

// S3Storage implements Storage using Amazon S3 (or S3-compatible stores like MinIO).
type S3Storage struct {
	client *s3.Client
	bucket string
}

// NewS3Storage creates a new S3Storage backed by the given S3 client and bucket.
func NewS3Storage(client *s3.Client, bucket string) *S3Storage {
	return &S3Storage{
		client: client,
		bucket: bucket,
	}
}

func (s *S3Storage) PutFile(ctx context.Context, key string, data io.Reader, size int64) error {
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          data,
		ContentLength: aws.Int64(size),
		ContentType:   aws.String("application/octet-stream"),
	})
	if err != nil {
		return fmt.Errorf("s3 upload %s: %w", key, err)
	}

	return nil
}

func (s *S3Storage) GetFile(ctx context.Context, key string) (io.ReadCloser, error) {
	output, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("s3 GetObject %s: %w", key, err)
	}

	return output.Body, nil
}

func (s *S3Storage) DeleteFile(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("s3 DeleteObject %s: %w", key, err)
	}

	return nil
}

var _ Storage = (*S3Storage)(nil)

// NewS3BackupStorage creates a Storage backed by S3.
// It reuses the S3 client factory from coldstorage.
func NewS3BackupStorage(bucket, region, endpoint, accessKeyID, secretAccessKey string) (Storage, error) {
	client, err := coldstorage.NewS3Client(region, endpoint, accessKeyID, secretAccessKey)
	if err != nil {
		return nil, err
	}

	return NewS3Storage(client, bucket), nil
}
