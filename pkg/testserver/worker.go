package testserver

import (
	"context"
	"strconv"

	"github.com/formancehq/go-libs/v3/testing/testservice"

	"github.com/formancehq/ledger/cmd"
	"time"
)

// LogsHashBlockMaxSizeInstrumentation returns an instrumentation function that appends the worker async block hasher max block size flag and the provided size to the run configuration's CLI arguments.
// The returned function adds the flag with the size formatted as a decimal string and always returns nil.
func LogsHashBlockMaxSizeInstrumentation(size int) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--"+cmd.WorkerAsyncBlockHasherMaxBlockSizeFlag, strconv.Itoa(size))
		return nil
	}
}

// LogsHashBlockCRONSpecInstrumentation returns an instrumentation function that appends the async block hasher CRON schedule flag and the given spec to a run configuration.
// The spec parameter is the CRON schedule expression to be passed as the value for the WorkerAsyncBlockHasherScheduleFlag.
func LogsHashBlockCRONSpecInstrumentation(spec string) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--"+cmd.WorkerAsyncBlockHasherScheduleFlag, spec)
		return nil
	}
}

// BucketCleanupRetentionPeriodInstrumentation creates an instrumentation function that appends the bucket cleanup retention period flag and its value to a test run configuration.
func BucketCleanupRetentionPeriodInstrumentation(retentionPeriod time.Duration) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--"+cmd.WorkerBucketCleanupRetentionPeriodFlag, retentionPeriod.String())
		return nil
	}
}

// BucketCleanupCRONSpecInstrumentation returns an instrumentation function that appends the bucket cleanup CRON schedule flag and the provided CRON spec to a test run configuration.
func BucketCleanupCRONSpecInstrumentation(spec string) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--"+cmd.WorkerBucketCleanupScheduleFlag, spec)
		return nil
	}
}