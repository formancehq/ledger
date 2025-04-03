package testserver

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/testing/testservice"
	"github.com/formancehq/ledger/cmd"
)

func ExperimentalFeaturesInstrumentation() testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--" + cmd.ExperimentalFeaturesFlag)
		return nil
	}
}

func ExperimentalNumscriptRewriteInstrumentation() testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--" + cmd.NumscriptInterpreterFlag)
		return nil
	}
}

func MaxPageSizeInstrumentation(size uint64) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--"+cmd.MaxPageSizeFlag, fmt.Sprint(size))
		return nil
	}
}

func DefaultPageSizeInstrumentation(size uint64) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--"+cmd.DefaultPageSizeFlag, fmt.Sprint(size))
		return nil
	}
}
