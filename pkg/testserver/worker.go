package testserver

import (
	"context"
	"strconv"

	"github.com/formancehq/go-libs/v3/testing/testservice"

	"github.com/formancehq/ledger/cmd"
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
