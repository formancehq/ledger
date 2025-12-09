package testserver

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/testing/deferred"
	"github.com/formancehq/go-libs/v3/testing/testservice"

	"github.com/formancehq/ledger/cmd"
)

func GetTestServerOptions(postgresConnectionOptions *deferred.Deferred[bunconnect.ConnectionOptions]) testservice.Option {
	return testservice.WithInstruments(
		testservice.AppendArgsInstrumentation("serve", "--"+cmd.BindFlag, ":0", "--schema-enforcement-mode", "strict"),
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

func ExperimentalPipelinesSyncPeriodInstrumentation(duration time.Duration) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		runConfiguration.AppendArgs("--"+cmd.WorkerPipelinesSyncPeriod, fmt.Sprint(duration))
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

// AuthInstrumentation enables authentication for testing
// This is used for integration tests to verify authentication works correctly
// The auth module from go-libs uses flags declared in auth/cli.go:
// - auth-enabled: Enable auth
// - auth-issuer: Issuer URL
// - auth-check-scopes: Check scopes
// - auth-service: Service name
func AuthInstrumentation(issuer *deferred.Deferred[string]) testservice.InstrumentationFunc {
	return func(ctx context.Context, runConfiguration *testservice.RunConfiguration) error {
		// Enable auth
		runConfiguration.AppendArgs("--auth-enabled")
		// Set issuer
		vIssuer, err := issuer.Wait(ctx)
		if err != nil {
			return fmt.Errorf("waiting for issuer: %w", err)
		}
		runConfiguration.AppendArgs("--auth-issuer", vIssuer)
		// Enable scope checking
		runConfiguration.AppendArgs("--auth-check-scopes")
		// Set service name
		runConfiguration.AppendArgs("--auth-service", "ledger")
		return nil
	}
}
