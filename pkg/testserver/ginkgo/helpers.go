package ginkgo

import (
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/testing/deferred"
	"github.com/formancehq/go-libs/v2/testing/testservice"
	. "github.com/formancehq/go-libs/v2/testing/testservice/ginkgo"
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
				testservice.AppendArgsInstrumentation("worker"),
				testservice.PostgresInstrumentation(postgresConnectionOptions),
			),
		}, options...)...,
	)
}
