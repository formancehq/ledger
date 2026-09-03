//go:build s3

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// S3Storage implements Storage using Amazon S3 (or S3-compatible stores like MinIO).
type S3Storage struct {
	client   *s3.Client
	uploader *transfermanager.Client
	bucket   string
}

// NewS3Storage creates a new S3Storage backed by the given S3 client and bucket.
func NewS3Storage(client *s3.Client, bucket string) *S3Storage {
	return &S3Storage{
		client:   client,
		uploader: transfermanager.New(client),
		bucket:   bucket,
	}
}

// PutFile uploads data via multipart upload. The size hint is ignored: the
// uploader streams data in bounded parts (partSize × concurrency memory) and
// lifts the 5 GB single-PutObject limit to the 5 TB multipart ceiling, so
// callers can stream an object of unknown length (e.g. an io.Pipe).
func (s *S3Storage) PutFile(ctx context.Context, key string, data io.Reader, _ int64) error {
	_, err := s.uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
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
func NewS3BackupStorage(bucket, region, endpoint, accessKeyID, secretAccessKey string) (Storage, error) {
	client, err := newS3Client(region, endpoint, accessKeyID, secretAccessKey)
	if err != nil {
		return nil, err
	}

	return NewS3Storage(client, bucket), nil
}

// newS3Client creates an S3 client.
// When accessKeyID and secretAccessKey are both non-empty, static credentials are used.
// Otherwise the default AWS credential chain is used (env vars, ~/.aws/credentials, IAM role).
// If endpoint is non-empty, it is used as a custom S3 endpoint (e.g. for MinIO).
func newS3Client(region, endpoint, accessKeyID, secretAccessKey string) (*s3.Client, error) {
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
