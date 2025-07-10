package testserver

import (
	"context"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"strconv"
)

func LogsHashBlockMaxSizeInstrumentation(size int) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--worker-async-block-hasher-max-block-size", strconv.Itoa(size))
		return nil
	}
}

func LogsHashBlockCRONSpecInstrumentation(spec string) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--worker-async-block-hasher-schedule", spec)
		return nil
	}
}
