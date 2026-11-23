//go:build e2e

package e2e

import (
	"encoding/json"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestFullBackup_S3Objects verifies that the full backup chainsaw test
// actually created objects in MinIO. Run after chainsaw tests.
func TestFullBackup_S3Objects(t *testing.T) {
	bucket := "e2e-full-backup"

	out, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err, "listing objects in bucket %s", bucket)
	assert.NotEmpty(t, out.Contents, "full backup should have created objects in S3 bucket %s", bucket)

	for _, obj := range out.Contents {
		t.Logf("  %s (%d bytes)", *obj.Key, *obj.Size)
	}
}

// TestRepeatedCheckpointBackup_S3Objects verifies the second checkpoint
// left a restorable object inventory in MinIO.
func TestRepeatedCheckpointBackup_S3Objects(t *testing.T) {
	bucket := "e2e-incr-backup"
	firstSequence, err := strconv.ParseUint(os.Getenv("FIRST_CHECKPOINT_SEQUENCE"), 10, 64)
	require.NoError(t, err, "first BackupRun must report its checkpoint sequence")
	secondSequence, err := strconv.ParseUint(os.Getenv("SECOND_CHECKPOINT_SEQUENCE"), 10, 64)
	require.NoError(t, err, "second BackupRun must report its checkpoint sequence")
	require.Greater(t, secondSequence, firstSequence, "second BackupRun must advance beyond the first")

	out, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err, "listing objects in bucket %s", bucket)
	assert.NotEmpty(t, out.Contents, "checkpoint backup should have created objects in S3 bucket %s", bucket)
	var manifestKey string
	objects := make(map[string]struct{}, len(out.Contents))
	for _, obj := range out.Contents {
		objects[aws.ToString(obj.Key)] = struct{}{}
		if strings.HasSuffix(aws.ToString(obj.Key), "/manifest.json") {
			manifestKey = aws.ToString(obj.Key)
		}
	}
	require.NotEmpty(t, manifestKey, "checkpoint manifest missing")
	manifestObject, err := s3Client.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(manifestKey)})
	require.NoError(t, err)
	defer manifestObject.Body.Close()
	manifestBytes, err := io.ReadAll(manifestObject.Body)
	require.NoError(t, err)
	var manifest struct {
		Checkpoint struct {
			LastLogSequence uint64 `json:"lastLogSequence"`
			Files           map[string]struct {
				Key string `json:"key"`
			} `json:"files"`
		} `json:"checkpoint"`
		Exports []json.RawMessage `json:"exports"`
	}
	require.NoError(t, json.Unmarshal(manifestBytes, &manifest))
	require.Empty(t, manifest.Exports, "checkpoint-only manifest must not publish exports")
	require.Equal(t, secondSequence, manifest.Checkpoint.LastLogSequence, "manifest must publish the second BackupRun checkpoint")
	require.NotEmpty(t, manifest.Checkpoint.Files)
	for name, file := range manifest.Checkpoint.Files {
		_, ok := objects[file.Key]
		require.True(t, ok, "manifest file %s references missing object %s", name, file.Key)
	}

	for _, obj := range out.Contents {
		t.Logf("  %s (%d bytes)", *obj.Key, *obj.Size)
	}
}

// TestRestore_S3BackupExists verifies that the restore test's backup
// created objects in MinIO before the destroy phase.
func TestRestore_S3BackupExists(t *testing.T) {
	bucket := "e2e-restore"

	out, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err, "listing objects in bucket %s", bucket)
	assert.NotEmpty(t, out.Contents, "backup should have created objects in S3 bucket %s", bucket)

	for _, obj := range out.Contents {
		t.Logf("  %s (%d bytes)", *obj.Key, *obj.Size)
	}
}
