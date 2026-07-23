//go:build s3

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
)

// s3UploadPartSize is the multipart part size for backup object uploads.
// The aws-sdk-go-v2 default is 5 MiB; at the 10,000-part ceiling that caps a
// single object at ~48.8 GiB and produces thousands of parts for a multi-GB
// checkpoint blob, widening the window for a single part failure to fail the
// whole CompleteMultipartUpload. 32 MiB cuts the part count ~6.4x (lifting the
// single-object ceiling to ~312 GiB) while keeping per-part memory bounded
// (partSize x concurrency).
const s3UploadPartSize = 32 << 20 // 32 MiB

// S3Storage implements Storage using Amazon S3 (or S3-compatible stores like MinIO).
type S3Storage struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

// NewS3Storage creates a new S3Storage backed by the given S3 client and bucket.
func NewS3Storage(client *s3.Client, bucket string) *S3Storage {
	return newS3StorageWithPartSize(client, bucket, s3UploadPartSize)
}

// newS3StorageWithPartSize is NewS3Storage with an explicit multipart part
// size. Production always uses s3UploadPartSize; tests use it to force a small
// part size so a modest payload still exercises the multipart path
// (CreateMultipartUpload / UploadPart / CompleteMultipartUpload) rather than a
// single PutObject, independent of the production default.
func newS3StorageWithPartSize(client *s3.Client, bucket string, partSize int64) *S3Storage {
	return &S3Storage{
		client: client,
		uploader: manager.NewUploader(client, func(u *manager.Uploader) {
			u.PartSize = partSize
		}),
		bucket: bucket,
	}
}

// PutFile uploads data via multipart upload. The size hint is ignored: the
// uploader streams data in bounded parts (partSize × concurrency memory) and
// lifts the 5 GB single-PutObject limit to the 5 TB multipart ceiling, so
// callers can stream an object of unknown length (e.g. an io.Pipe).
func (s *S3Storage) PutFile(ctx context.Context, key string, data io.Reader, _ int64) error {
	_, err := s.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(s.bucket),
		Key:         aws.String(key),
		Body:        data,
		ContentType: aws.String("application/octet-stream"),
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
		var (
			noSuchKey *types.NoSuchKey
			notFound  *types.NotFound
		)

		if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
			return nil, fmt.Errorf("s3 GetObject %s: %w", key, ErrFileNotFound)
		}

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

func (s *S3Storage) ListFiles(ctx context.Context, prefix string) ([]string, error) {
	var keys []string

	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("s3 ListObjectsV2 %s: %w", prefix, err)
		}

		for _, obj := range page.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
	}

	return keys, nil
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
