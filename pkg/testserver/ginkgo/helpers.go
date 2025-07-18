package ginkgo

import (
	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/testing/deferred"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	. "github.com/formancehq/go-libs/v3/testing/testservice/ginkgo"
	"github.com/formancehq/ledger/cmd"
	"github.com/formancehq/ledger/pkg/testserver"
)

func DeferTestServer(postgresConnectionOptions *deferred.Deferred[bunconnect.ConnectionOptions], options ...testservice.Option) *deferred.Deferred[*testservice.Service] {
	return DeferNew(
		cmd.NewRootCommand,
		append([]testservice.Option{
			testserver.GetTestServerOptions(postgresConnectionOptions),
		}, options...)...,
	)
}

func DeferTestWorker(postgresConnectionOptions *deferred.Deferred[bunconnect.ConnectionOptions], options ...testservice.Option) *deferred.Deferred[*testservice.Service] {
	return DeferNew(
		cmd.NewRootCommand,
		append([]testservice.Option{
			testservice.WithInstruments(
				testservice.GRPCServerInstrumentation(),
				testservice.AppendArgsInstrumentation("worker"),
				testservice.PostgresInstrumentation(postgresConnectionOptions),
				testserver.GRPCAddressInstrumentation(":0"),
			),
		}, options...)...,
	)
}
