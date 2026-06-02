//go:build e2e

package e2e

import (
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

// TestIncrementalBackup_S3Segments verifies that incremental backup
// created additional segment objects in MinIO.
func TestIncrementalBackup_S3Segments(t *testing.T) {
	bucket := "e2e-incr-backup"

	out, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err, "listing objects in bucket %s", bucket)
	assert.NotEmpty(t, out.Contents, "incremental backup should have created objects in S3 bucket %s", bucket)

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
