//go:build s3

package coldstorage

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

const (
	minioAccessKey = "minioadmin"
	minioSecretKey = "minioadmin"
	minioBucket    = "test-cold-storage"
)

// setupMinIO starts a MinIO container and returns an S3 client + endpoint.
func setupMinIO(t *testing.T) (*s3.Client, string) {
	t.Helper()

	ctx := context.Background()

	container, err := testcontainers.Run(ctx, "minio/minio:latest",
		testcontainers.WithEnv(map[string]string{
			"MINIO_ROOT_USER":     minioAccessKey,
			"MINIO_ROOT_PASSWORD": minioSecretKey,
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
			minioAccessKey, minioSecretKey, "",
		)),
	)
	require.NoError(t, err)

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
	})

	// Create test bucket
	_, err = client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(minioBucket),
	})
	require.NoError(t, err)

	return client, endpoint
}

func TestS3Storage_ArchiveAndExists(t *testing.T) {
	t.Parallel()

	client, _ := setupMinIO(t)
	storage := NewS3Storage(client, minioBucket)
	ctx := context.Background()

	content := "archive content"
	err := storage.Archive(ctx, "bucket1", 42, strings.NewReader(content), ComputeSHA256OrPanic([]byte(content)))
	require.NoError(t, err)

	exists, err := storage.Exists(ctx, "bucket1", 42)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestS3Storage_ExistsNotFound(t *testing.T) {
	t.Parallel()

	client, _ := setupMinIO(t)
	storage := NewS3Storage(client, minioBucket)
	ctx := context.Background()

	exists, err := storage.Exists(ctx, "bucket1", 999)
	require.NoError(t, err)
	require.False(t, exists)
}

func TestS3Storage_FetchRoundtrip(t *testing.T) {
	t.Parallel()

	client, _ := setupMinIO(t)
	storage := NewS3Storage(client, minioBucket)
	ctx := context.Background()

	content := "archive data for fetch test"
	err := storage.Archive(ctx, "bucket", 7, strings.NewReader(content), ComputeSHA256OrPanic([]byte(content)))
	require.NoError(t, err)

	rc, err := storage.Fetch(ctx, "bucket", 7)
	require.NoError(t, err)

	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, content, string(data))
}

func TestS3Storage_FetchNotFound(t *testing.T) {
	t.Parallel()

	client, _ := setupMinIO(t)
	storage := NewS3Storage(client, minioBucket)
	ctx := context.Background()

	_, err := storage.Fetch(ctx, "bucket", 999)
	require.Error(t, err)
}

func TestS3Storage_ArchiveOverwrite(t *testing.T) {
	t.Parallel()

	client, _ := setupMinIO(t)
	storage := NewS3Storage(client, minioBucket)
	ctx := context.Background()

	require.NoError(t, storage.Archive(ctx, "bucket", 1, strings.NewReader("version1"), ComputeSHA256OrPanic([]byte("version1"))))
	require.NoError(t, storage.Archive(ctx, "bucket", 1, strings.NewReader("version2"), ComputeSHA256OrPanic([]byte("version2"))))

	rc, err := storage.Fetch(ctx, "bucket", 1)
	require.NoError(t, err)

	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "version2", string(data))
}

func TestS3Storage_DifferentBucketIDs(t *testing.T) {
	t.Parallel()

	client, _ := setupMinIO(t)
	storage := NewS3Storage(client, minioBucket)
	ctx := context.Background()

	require.NoError(t, storage.Archive(ctx, "bucket-a", 1, strings.NewReader("data-a"), ComputeSHA256OrPanic([]byte("data-a"))))

	exists, err := storage.Exists(ctx, "bucket-b", 1)
	require.NoError(t, err)
	require.False(t, exists)

	exists, err = storage.Exists(ctx, "bucket-a", 1)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestS3Storage_SSTRoundtripWithColdReader(t *testing.T) {
	t.Parallel()

	client, _ := setupMinIO(t)
	storage := NewS3Storage(client, minioBucket)
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	// Build a real SST with test data
	sstData := buildTestSST(t, [][2][]byte{
		{[]byte("key1"), []byte("value1")},
		{[]byte("key2"), []byte("value2")},
		{[]byte("key3"), []byte("value3")},
	})

	// Archive SST to S3
	require.NoError(t, storage.Archive(ctx, "test-bucket", 1, bytes.NewReader(sstData), ComputeSHA256OrPanic(sstData)))

	// Read back via ColdReader
	cacheDir := t.TempDir()
	reader := NewColdReader(storage, "test-bucket", cacheDir, 4, 0, logger)
	t.Cleanup(func() { _ = reader.Close() })

	pebbleReader, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)
	require.NotNil(t, pebbleReader)

	// Verify all keys are readable
	for i, expected := range []struct {
		key, val string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	} {
		val, closer, err := pebbleReader.Get([]byte(expected.key))
		require.NoError(t, err, "key %d", i)
		require.Equal(t, []byte(expected.val), val, "key %d", i)
		_ = closer.Close()
	}
}

func TestS3Storage_ColdReaderCacheEviction(t *testing.T) {
	t.Parallel()

	client, _ := setupMinIO(t)
	storage := NewS3Storage(client, minioBucket)
	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	// Archive 3 SSTs to S3
	for i := uint64(1); i <= 3; i++ {
		sstData := buildTestSST(t, [][2][]byte{
			{[]byte("key"), []byte("value")},
		})
		require.NoError(t, storage.Archive(ctx, "evict-bucket", i, bytes.NewReader(sstData), ComputeSHA256OrPanic(sstData)))
	}

	// ColdReader with max 2 cached
	cacheDir := t.TempDir()
	reader := NewColdReader(storage, "evict-bucket", cacheDir, 2, 0, logger)
	t.Cleanup(func() { _ = reader.Close() })

	// Load periods 1 and 2
	_, err := reader.GetReader(ctx, 1)
	require.NoError(t, err)
	_, err = reader.GetReader(ctx, 2)
	require.NoError(t, err)

	// Load period 3 → evicts period 1
	_, err = reader.GetReader(ctx, 3)
	require.NoError(t, err)

	// Period 3 should be readable (re-downloaded if needed)
	r3, err := reader.GetReader(ctx, 3)
	require.NoError(t, err)

	val, closer, err := r3.Get([]byte("key"))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), val)
	_ = closer.Close()
}

func TestS3Storage_ArchivePersistsChecksumInMetadata(t *testing.T) {
	t.Parallel()

	client, _ := setupMinIO(t)
	storage := NewS3Storage(client, minioBucket)
	ctx := context.Background()

	content := []byte("integrity-tracked body")
	expected := ComputeSHA256OrPanic(content)
	require.NoError(t, storage.Archive(ctx, "bucket", 1, bytes.NewReader(content), expected))

	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(minioBucket),
		Key:    aws.String(storage.archiveKey("bucket", 1)),
	})
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(expected), head.Metadata[s3ChecksumMetadataKey],
		"S3 user metadata must carry the SHA-256 (hex-encoded) under the sha256 key")
}

func TestS3Storage_ExistsRequiresMetadata(t *testing.T) {
	t.Parallel()

	client, _ := setupMinIO(t)
	storage := NewS3Storage(client, minioBucket)
	ctx := context.Background()

	key := storage.archiveKey("bucket", 2)

	// Bypass our Archive() helper to upload an object WITHOUT checksum
	// metadata. This simulates a legacy archive from before the fix.
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(minioBucket),
		Key:    aws.String(key),
		Body:   strings.NewReader("legacy bytes"),
	})
	require.NoError(t, err)

	exists, err := storage.Exists(ctx, "bucket", 2)
	require.NoError(t, err)
	require.False(t, exists, "object without checksum metadata must read as not-yet-committed")

	// Re-upload via the new Archive path → metadata is now there → Exists=true.
	require.NoError(t, storage.Archive(ctx, "bucket", 2, strings.NewReader("legacy bytes"),
		ComputeSHA256OrPanic([]byte("legacy bytes"))))

	exists, err = storage.Exists(ctx, "bucket", 2)
	require.NoError(t, err)
	require.True(t, exists)
}

func TestS3Storage_ExpectedChecksum(t *testing.T) {
	t.Parallel()

	client, _ := setupMinIO(t)
	storage := NewS3Storage(client, minioBucket)
	ctx := context.Background()

	content := []byte("hello s3")
	expected := ComputeSHA256OrPanic(content)
	require.NoError(t, storage.Archive(ctx, "bucket", 3, bytes.NewReader(content), expected))

	got, err := storage.ExpectedChecksum(ctx, "bucket", 3)
	require.NoError(t, err)
	require.Equal(t, expected, got)
}

func TestS3Storage_ExpectedChecksumMissingReturnsSentinel(t *testing.T) {
	t.Parallel()

	client, _ := setupMinIO(t)
	storage := NewS3Storage(client, minioBucket)
	ctx := context.Background()

	// Upload directly without metadata to reproduce the missing-checksum state.
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(minioBucket),
		Key:    aws.String(storage.archiveKey("bucket", 4)),
		Body:   strings.NewReader("no metadata"),
	})
	require.NoError(t, err)

	_, err = storage.ExpectedChecksum(ctx, "bucket", 4)
	require.True(t, errors.Is(err, ErrChecksumNotFound),
		"missing metadata must surface as ErrChecksumNotFound")
}

func TestS3Storage_OverwriteUpdatesMetadata(t *testing.T) {
	t.Parallel()

	client, _ := setupMinIO(t)
	storage := NewS3Storage(client, minioBucket)
	ctx := context.Background()

	c1 := ComputeSHA256OrPanic([]byte("v1"))
	c2 := ComputeSHA256OrPanic([]byte("v2"))
	require.NoError(t, storage.Archive(ctx, "bucket", 5, strings.NewReader("v1"), c1))
	require.NoError(t, storage.Archive(ctx, "bucket", 5, strings.NewReader("v2"), c2))

	got, err := storage.ExpectedChecksum(ctx, "bucket", 5)
	require.NoError(t, err)
	require.Equal(t, c2, got, "after overwrite, persisted metadata reflects the latest write")
}

func TestS3Storage_NewS3ColdStorage(t *testing.T) {
	// Cannot use t.Parallel() with t.Setenv
	_, endpoint := setupMinIO(t)

	// Set AWS credentials for NewS3ColdStorage (uses default credential chain)
	t.Setenv("AWS_ACCESS_KEY_ID", minioAccessKey)
	t.Setenv("AWS_SECRET_ACCESS_KEY", minioSecretKey)

	cs, err := NewS3ColdStorage(minioBucket, "us-east-1", endpoint)
	require.NoError(t, err)

	ctx := context.Background()

	require.NoError(t, cs.Archive(ctx, "bucket", 1, strings.NewReader("data"), ComputeSHA256OrPanic([]byte("data"))))

	exists, err := cs.Exists(ctx, "bucket", 1)
	require.NoError(t, err)
	require.True(t, exists)

	rc, err := cs.Fetch(ctx, "bucket", 1)
	require.NoError(t, err)

	defer func() { _ = rc.Close() }()

	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, "data", string(data))
}
