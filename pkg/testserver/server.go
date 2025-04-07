package testserver

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/testing/deferred"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger/cmd"
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
