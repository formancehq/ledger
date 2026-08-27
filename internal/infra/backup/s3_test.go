//go:build s3

package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/transfermanager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	backupMinioAccessKey = "minioadmin"
	backupMinioSecretKey = "minioadmin"
	backupTestBucket     = "backup-storage-test"
)

// setupMinIOBackup starts a MinIO container and returns an S3 client with the
// test bucket created.
func setupMinIOBackup(t *testing.T) *s3.Client {
	t.Helper()

	ctx := context.Background()

	container, err := testcontainers.Run(ctx, "minio/minio:latest",
		testcontainers.WithEnv(map[string]string{
			"MINIO_ROOT_USER":     backupMinioAccessKey,
			"MINIO_ROOT_PASSWORD": backupMinioSecretKey,
		}),
		testcontainers.WithCmd("server", "/data"),
		testcontainers.WithExposedPorts("9000/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForHTTP("/minio/health/live").WithPort("9000/tcp").WithStartupTimeout(30*time.Second),
		),
	)
	require.NoError(t, err)

	t.Cleanup(func() { _ = container.Terminate(context.Background()) })

	endpoint, err := container.Endpoint(ctx, "http")
	require.NoError(t, err)

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			backupMinioAccessKey, backupMinioSecretKey, "",
		)),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(backupTestBucket),
	})
	require.NoError(t, err)

	return client
}

// recordingS3Client wraps a real *s3.Client and records which multipart
// operations were invoked, plus the checksum algorithm CreateMultipartUpload
// was called with. It lets multipart tests assert the actual API call
// sequence instead of only the round-tripped content, so a future default
// (part size, multipart threshold) change that silently routes a "large
// enough" payload through single-shot PutObject gets caught.
type recordingS3Client struct {
	*s3.Client

	mu                sync.Mutex
	operations        []string
	checksumAlgorithm types.ChecksumAlgorithm
}

func (r *recordingS3Client) record(op string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.operations = append(r.operations, op)
}

func (r *recordingS3Client) count(op string) int {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := 0

	for _, o := range r.operations {
		if o == op {
			n++
		}
	}

	return n
}

func (r *recordingS3Client) CreateMultipartUpload(ctx context.Context, in *s3.CreateMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CreateMultipartUploadOutput, error) {
	r.record("CreateMultipartUpload")

	r.mu.Lock()
	r.checksumAlgorithm = in.ChecksumAlgorithm
	r.mu.Unlock()

	return r.Client.CreateMultipartUpload(ctx, in, optFns...)
}

func (r *recordingS3Client) UploadPart(ctx context.Context, in *s3.UploadPartInput, optFns ...func(*s3.Options)) (*s3.UploadPartOutput, error) {
	r.record("UploadPart")

	return r.Client.UploadPart(ctx, in, optFns...)
}

func (r *recordingS3Client) CompleteMultipartUpload(ctx context.Context, in *s3.CompleteMultipartUploadInput, optFns ...func(*s3.Options)) (*s3.CompleteMultipartUploadOutput, error) {
	r.record("CompleteMultipartUpload")

	return r.Client.CompleteMultipartUpload(ctx, in, optFns...)
}

func (r *recordingS3Client) PutObject(ctx context.Context, in *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	r.record("PutObject")

	return r.Client.PutObject(ctx, in, optFns...)
}

// transfermanager's default multipart threshold (16 MiB) is larger than the
// old feature/s3/manager default (5 MiB); payloads here must exceed it so
// these tests keep exercising CreateMultipartUpload/UploadPart/
// CompleteMultipartUpload instead of silently falling back to a single
// PutObject.
const multipartTestObjectSize = 20 << 20

// TestS3Storage_PutFileMultipartLargeObject uploads an object larger than the
// transfermanager multipart threshold so the multipart path
// (CreateMultipartUpload / UploadPart* / CompleteMultipartUpload) is
// exercised end-to-end against MinIO — this is the exact call sequence that
// produced the CompleteMultipartUpload InvalidPart error being regression
// tested. A single PutObject cannot exceed 5 GB; multipart is what lifts that
// ceiling.
func TestS3Storage_PutFileMultipartLargeObject(t *testing.T) {
	t.Parallel()

	client := setupMinIOBackup(t)
	recorder := &recordingS3Client{Client: client}
	storage := &S3Storage{client: client, uploader: transfermanager.New(recorder), bucket: backupTestBucket}
	ctx := context.Background()

	const size = multipartTestObjectSize

	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}

	require.NoError(t, storage.PutFile(ctx, "big/object.bin", bytes.NewReader(payload), int64(size)))

	require.Positive(t, recorder.count("CreateMultipartUpload"), "expected a multipart upload for a payload above the multipart threshold")
	require.GreaterOrEqual(t, recorder.count("UploadPart"), 2, "expected at least two parts uploaded")
	require.Positive(t, recorder.count("CompleteMultipartUpload"))
	require.Zero(t, recorder.count("PutObject"), "a payload above the multipart threshold must not fall back to single-shot PutObject")
	require.Equal(t, types.ChecksumAlgorithmCrc32, recorder.checksumAlgorithm,
		"CreateMultipartUpload must declare the checksum algorithm up front so it agrees with UploadPart's auto-attached checksum")

	rc, err := storage.GetFile(ctx, "big/object.bin")
	require.NoError(t, err)

	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Len(t, got, size)
	require.Equal(t, sha256.Sum256(payload), sha256.Sum256(got))
}

// TestS3Storage_PutFileStreamsUnknownLengthReader mirrors the export path: a
// non-seekable io.Pipe of unknown length, larger than the multipart
// threshold, streamed straight into a multipart upload with a bounded
// footprint.
func TestS3Storage_PutFileStreamsUnknownLengthReader(t *testing.T) {
	t.Parallel()

	client := setupMinIOBackup(t)
	recorder := &recordingS3Client{Client: client}
	storage := &S3Storage{client: client, uploader: transfermanager.New(recorder), bucket: backupTestBucket}
	ctx := context.Background()

	const size = multipartTestObjectSize

	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i * 3)
	}

	pr, pw := io.Pipe()

	go func() {
		_, err := io.Copy(pw, bytes.NewReader(payload))
		_ = pw.CloseWithError(err)
	}()

	require.NoError(t, storage.PutFile(ctx, "streamed/object.bin", pr, -1))

	require.Positive(t, recorder.count("CreateMultipartUpload"), "expected a multipart upload for a payload above the multipart threshold")
	require.GreaterOrEqual(t, recorder.count("UploadPart"), 2, "expected at least two parts uploaded")
	require.Positive(t, recorder.count("CompleteMultipartUpload"))
	require.Zero(t, recorder.count("PutObject"), "a payload above the multipart threshold must not fall back to single-shot PutObject")
	require.Equal(t, types.ChecksumAlgorithmCrc32, recorder.checksumAlgorithm,
		"CreateMultipartUpload must declare the checksum algorithm up front so it agrees with UploadPart's auto-attached checksum")

	rc, err := storage.GetFile(ctx, "streamed/object.bin")
	require.NoError(t, err)

	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Len(t, got, size)
	require.Equal(t, sha256.Sum256(payload), sha256.Sum256(got))
}
