package controller

import (
	"testing"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestNextRunTime_NoPreviousRun(t *testing.T) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse("0 * * * *") // every hour
	require.NoError(t, err)

	next := nextRunTime(sched, nil)
	assert.True(t, next.IsZero(), "should return zero time (run immediately) when no previous run")
}

func TestNextRunTime_WithPreviousRun(t *testing.T) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse("0 * * * *") // every hour
	require.NoError(t, err)

	lastRun := metav1.NewTime(time.Date(2025, 5, 11, 14, 0, 0, 0, time.UTC))
	next := nextRunTime(sched, &lastRun)

	expected := time.Date(2025, 5, 11, 15, 0, 0, 0, time.UTC)
	assert.Equal(t, expected, next)
}

func TestNextRunTime_WeeklySchedule(t *testing.T) {
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	sched, err := parser.Parse("0 2 * * 0") // every Sunday at 2am
	require.NoError(t, err)

	// Last run was Sunday May 11, 2025 at 2am
	lastRun := metav1.NewTime(time.Date(2025, 5, 11, 2, 0, 0, 0, time.UTC))
	next := nextRunTime(sched, &lastRun)

	// Next should be Sunday May 18, 2025 at 2am
	expected := time.Date(2025, 5, 18, 2, 0, 0, 0, time.UTC)
	assert.Equal(t, expected, next)
}

func TestBackupFlags_S3Full(t *testing.T) {
	dest := &ledgerv1alpha1.BackupDestination{
		Driver:   "s3",
		BucketID: "my-prefix",
		S3: &ledgerv1alpha1.S3Config{
			Bucket:   "my-bucket",
			Region:   "us-east-1",
			Endpoint: "http://minio:9000",
		},
	}

	flags := backupFlags(dest)
	assert.Equal(t, []string{
		"--driver", "s3",
		"--bucket-id", "my-prefix",
		"--s3-bucket", "my-bucket",
		"--s3-region", "us-east-1",
		"--s3-endpoint", "http://minio:9000",
	}, flags)
}

func TestBackupFlags_S3Minimal(t *testing.T) {
	dest := &ledgerv1alpha1.BackupDestination{
		Driver: "s3",
		S3: &ledgerv1alpha1.S3Config{
			Bucket: "my-bucket",
			Region: "eu-west-1",
		},
	}

	flags := backupFlags(dest)
	assert.Equal(t, []string{
		"--driver", "s3",
		"--s3-bucket", "my-bucket",
		"--s3-region", "eu-west-1",
	}, flags)
}

func TestBackupFlags_NoBucketID(t *testing.T) {
	dest := &ledgerv1alpha1.BackupDestination{
		Driver: "s3",
		S3: &ledgerv1alpha1.S3Config{
			Bucket: "b",
		},
	}

	flags := backupFlags(dest)
	assert.Equal(t, []string{
		"--driver", "s3",
		"--s3-bucket", "b",
	}, flags)
}

func TestBackupFlags_NilS3(t *testing.T) {
	dest := &ledgerv1alpha1.BackupDestination{
		Driver: "s3",
	}

	flags := backupFlags(dest)
	assert.Equal(t, []string{"--driver", "s3"}, flags)
}

func TestBackupFlags_WithCredentials(t *testing.T) {
	dest := &ledgerv1alpha1.BackupDestination{
		Driver: "s3",
		S3: &ledgerv1alpha1.S3Config{
			Bucket:   "my-bucket",
			Endpoint: "http://minio:9000",
		},
		S3AccessKeyID:     "AKID",
		S3SecretAccessKey: "SECRET",
	}

	flags := backupFlags(dest)
	assert.Equal(t, []string{
		"--driver", "s3",
		"--s3-bucket", "my-bucket",
		"--s3-endpoint", "http://minio:9000",
		"--s3-access-key-id", "AKID",
		"--s3-secret-access-key", "SECRET",
	}, flags)
}
