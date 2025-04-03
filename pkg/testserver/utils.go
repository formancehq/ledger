package testserver

import (
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/testing/deferred"
	"github.com/formancehq/go-libs/v2/testing/testservice"
	"github.com/formancehq/ledger/cmd"
)

func GetTestServerOptions(postgresConnectionOptions *deferred.Deferred[bunconnect.ConnectionOptions]) testservice.Option {
	return testservice.WithInstruments(
		testservice.AppendArgsInstrumentation("serve", "--"+cmd.BindFlag, ":0"),
		testservice.PostgresInstrumentation(postgresConnectionOptions),
		testservice.HTTPServerInstrumentation(),
	)
}

func DeferTestServer(postgresConnectionOptions *deferred.Deferred[bunconnect.ConnectionOptions], options ...testservice.Option) *deferred.Deferred[*testservice.Service] {
	return testservice.DeferNew(
		cmd.NewRootCommand,
		append([]testservice.Option{
			GetTestServerOptions(postgresConnectionOptions),
		}, options...)...,
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

func DeferTestWorker(postgresConnectionOptions *deferred.Deferred[bunconnect.ConnectionOptions], options ...testservice.Option) *deferred.Deferred[*testservice.Service] {
	return testservice.DeferNew(
		cmd.NewRootCommand,
		append([]testservice.Option{
			testservice.WithInstruments(
				testservice.AppendArgsInstrumentation("worker"),
				testservice.PostgresInstrumentation(postgresConnectionOptions),
			),
		}, options...)...,
	)
}
