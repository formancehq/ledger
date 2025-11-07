package testserver

import (
	"context"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger/cmd"
	"strconv"
	"time"
)

func LogsHashBlockMaxSizeInstrumentation(size int) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--"+cmd.WorkerAsyncBlockHasherMaxBlockSizeFlag, strconv.Itoa(size))
		return nil
	}
}

func LogsHashBlockCRONSpecInstrumentation(spec string) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--"+cmd.WorkerAsyncBlockHasherScheduleFlag, spec)
		return nil
	}
}

func BucketCleanupRetentionPeriodInstrumentation(retentionPeriod time.Duration) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--"+cmd.WorkerBucketCleanupRetentionPeriodFlag, retentionPeriod.String())
		return nil
	}
}

func BucketCleanupCRONSpecInstrumentation(spec string) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--"+cmd.WorkerBucketCleanupScheduleFlag, spec)
		return nil
	}
}
