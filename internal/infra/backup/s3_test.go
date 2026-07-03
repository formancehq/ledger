//go:build s3

package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"io"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

// TestS3Storage_PutFileMultipartLargeObject uploads an object larger than the
// uploader's part size so the multipart path (CreateMultipartUpload /
// UploadPart* / CompleteMultipartUpload) is exercised end-to-end against MinIO.
// A single PutObject cannot exceed 5 GB; multipart is what lifts that ceiling.
func TestS3Storage_PutFileMultipartLargeObject(t *testing.T) {
	t.Parallel()

	client := setupMinIOBackup(t)
	storage := NewS3Storage(client, backupTestBucket)
	ctx := context.Background()

	// 12 MiB > the 5 MiB default part size → at least 3 parts.
	const size = 12 << 20

	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(i)
	}

	require.NoError(t, storage.PutFile(ctx, "big/object.bin", bytes.NewReader(payload), int64(size)))

	rc, err := storage.GetFile(ctx, "big/object.bin")
	require.NoError(t, err)

	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Len(t, got, size)
	require.Equal(t, sha256.Sum256(payload), sha256.Sum256(got))
}

// TestS3Storage_PutFileStreamsUnknownLengthReader mirrors the export path: a
// non-seekable io.Pipe of unknown length, larger than the part size, streamed
// straight into a multipart upload with a bounded footprint.
func TestS3Storage_PutFileStreamsUnknownLengthReader(t *testing.T) {
	t.Parallel()

	client := setupMinIOBackup(t)
	storage := NewS3Storage(client, backupTestBucket)
	ctx := context.Background()

	const size = 12 << 20

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

	rc, err := storage.GetFile(ctx, "streamed/object.bin")
	require.NoError(t, err)

	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Len(t, got, size)
	require.Equal(t, sha256.Sum256(payload), sha256.Sum256(got))
}
