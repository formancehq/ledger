package cmd

import (
	"testing"

	"github.com/formancehq/ledger/internal/worker"
	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"
)

func TestWorkerCommand(t *testing.T) {
	cmd := NewWorkerCommand()
	require.NotNil(t, cmd)
	require.Equal(t, "worker", cmd.Use)
}

func TestWorkerFlags(t *testing.T) {
	cmd := &cobra.Command{}
	addWorkerFlags(cmd)

	bucketDeletionScheduleFlag := cmd.Flag(WorkerBucketDeletionScheduleFlag)
	require.NotNil(t, bucketDeletionScheduleFlag)
	require.Equal(t, "0 0 0 * * *", bucketDeletionScheduleFlag.DefValue)
	require.Contains(t, bucketDeletionScheduleFlag.Usage, "bucket deletion")

	bucketDeletionGraceDaysFlag := cmd.Flag(WorkerBucketDeletionGraceDaysFlag)
	require.NotNil(t, bucketDeletionGraceDaysFlag)
	require.Equal(t, "30", bucketDeletionGraceDaysFlag.DefValue)
	require.Contains(t, bucketDeletionGraceDaysFlag.Usage, "Grace period")

	asyncBlockHasherMaxBlockSizeFlag := cmd.Flag(WorkerAsyncBlockHasherMaxBlockSizeFlag)
	require.NotNil(t, asyncBlockHasherMaxBlockSizeFlag)
	require.Equal(t, "1000", asyncBlockHasherMaxBlockSizeFlag.DefValue)

	asyncBlockHasherScheduleFlag := cmd.Flag(WorkerAsyncBlockHasherScheduleFlag)
	require.NotNil(t, asyncBlockHasherScheduleFlag)
	require.Equal(t, "0 * * * * *", asyncBlockHasherScheduleFlag.DefValue)
}

func TestWorkerConfiguration(t *testing.T) {
	cfg := WorkerConfiguration{
		HashLogsBlockMaxSize:    2000,
		HashLogsBlockCRONSpec:   "*/5 * * * * *",
		BucketDeletionCRONSpec:  "0 0 12 * * *",
		BucketDeletionGraceDays: 45,
	}

	workerConfig := worker.ModuleConfig{
		MaxBlockSize:            cfg.HashLogsBlockMaxSize,
		Schedule:                cfg.HashLogsBlockCRONSpec,
		BucketDeletionSchedule:  cfg.BucketDeletionCRONSpec,
		BucketDeletionGraceDays: cfg.BucketDeletionGraceDays,
	}

	require.Equal(t, 2000, workerConfig.MaxBlockSize)
	require.Equal(t, "*/5 * * * * *", workerConfig.Schedule)
	require.Equal(t, "0 0 12 * * *", workerConfig.BucketDeletionSchedule)
	require.Equal(t, 45, workerConfig.BucketDeletionGraceDays)
}
