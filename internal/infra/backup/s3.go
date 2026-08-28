//go:build s3

package backup

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
)

// s3UploadPartSize is the multipart part size for backup object uploads.
// transfermanager defaults to 8 MiB.
//
// For the incremental segment stream (an io.Pipe of unknown length,
// PutFile(..., -1) in manager.go), the part size is fixed and the
// 10,000-part cap directly bounds the object size: 32 MiB raises that
// ceiling from ~78 GiB to ~312 GiB (segments target ~4 GiB — see
// maxExportSegmentBytes in manager.go — well inside either).
//
// For the full-backup path (*os.File, seekable), transfermanager
// auto-upsizes the part size instead of hitting that cap (initSize in
// aws-sdk-go-v2/feature/s3/transfermanager), so there is no hard ceiling
// either way; 32 MiB just delays when upsizing kicks in, keeping part
// count lower across a wider size range.
//
// Either path pools Concurrency+1 part-sized buffers per active multipart
// upload (api_op_UploadObject.go) — at the default Concurrency of 5 that's
// ~192 MiB steady-state (~224 MiB transient, including the unpooled
// first-part read buffer) per upload. BackupJobsState only excludes per
// destination, so PutFile acquires coldstorage.AcquireS3UploadSlot to cap
// how many of these can be active across the whole process at once — see
// that function for the memory-budget sizing.
const s3UploadPartSize = 32 << 20 // 32 MiB

// S3Storage implements Storage using Amazon S3 (or S3-compatible stores like MinIO).
type S3Storage struct {
	client   *s3.Client
	uploader *transfermanager.Client
	bucket   string
}

// newUploader builds the transfermanager client with the production part
// size. Shared with tests so they exercise the exact configuration
// NewS3Storage uses instead of re-deriving it inline.
func newUploader(client transfermanager.S3APIClient) *transfermanager.Client {
	return transfermanager.New(client, func(o *transfermanager.Options) {
		o.PartSizeBytes = s3UploadPartSize
	})
}

// NewS3Storage creates a new S3Storage backed by the given S3 client and bucket.
func NewS3Storage(client *s3.Client, bucket string) *S3Storage {
	return &S3Storage{
		client:   client,
		uploader: newUploader(client),
		bucket:   bucket,
	}
}

// PutFile uploads data via multipart upload. The size hint is ignored: the
// uploader streams data in bounded parts (~192 MiB pooled per upload at the
// default part size/concurrency — see s3UploadPartSize) so callers can
// stream an object of unknown length (e.g. an io.Pipe). For that
// unknown-length case, the fixed part size and the 10,000-part cap bound the
// object at ~312 GiB; a seekable input has no such ceiling (transfermanager
// auto-upsizes the part size instead) up to S3's 5 TB absolute object limit.
func (s *S3Storage) PutFile(ctx context.Context, key string, data io.Reader, _ int64) error {
	release, err := coldstorage.AcquireS3UploadSlot(ctx)
	if err != nil {
		return fmt.Errorf("acquiring S3 upload slot: %w", err)
	}
	defer release()

	_, err = s.uploader.UploadObject(ctx, &transfermanager.UploadObjectInput{
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
