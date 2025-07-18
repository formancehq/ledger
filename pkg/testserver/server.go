package testserver

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/testing/deferred"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger/cmd"
	"time"
)

func GetTestServerOptions(postgresConnectionOptions *deferred.Deferred[bunconnect.ConnectionOptions]) testservice.Option {
	return testservice.WithInstruments(
		testservice.AppendArgsInstrumentation("serve", "--"+cmd.BindFlag, ":0"),
		testservice.PostgresInstrumentation(postgresConnectionOptions),
		testservice.HTTPServerInstrumentation(),
	)
}

func NewTestServer(postgresConnectionOptions *deferred.Deferred[bunconnect.ConnectionOptions], options ...testservice.Option) *testservice.Service {
	return testservice.New(
		cmd.NewRootCommand,
		append([]testservice.Option{
			GetTestServerOptions(postgresConnectionOptions),
		}, options...)...,
	)
}

func ExperimentalFeaturesInstrumentation() testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--" + cmd.ExperimentalFeaturesFlag)
		return nil
	}
}

func ExperimentalExportersInstrumentation() testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--" + cmd.ExperimentalExporters)
		return nil
	}
}

func ExperimentalEnableWorker() testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--" + cmd.WorkerEnabledFlag)
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

func ExperimentalPipelinesPushRetryPeriodInstrumentation(duration time.Duration) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--"+cmd.WorkerPipelinesPushRetryPeriodFlag, fmt.Sprint(duration))
		return nil
	}
}

func ExperimentalPipelinesPullIntervalInstrumentation(duration time.Duration) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--"+cmd.WorkerPipelinesPullIntervalFlag, fmt.Sprint(duration))
		return nil
	}
}

func GRPCAddressInstrumentation(addr string) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--"+cmd.WorkerGRPCAddressFlag, addr)
		return nil
	}
}

func WorkerAddressInstrumentation(addr *deferred.Deferred[string]) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		address, err := addr.Wait(ctx)
		if err != nil {
			return fmt.Errorf("waiting for worker address: %w", err)
		}
		runConfiguration.AppendArgs("--"+cmd.WorkerGRPCAddressFlag, address)
		return nil
	}
}
