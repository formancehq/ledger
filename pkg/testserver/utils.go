package testserver

import (
	"github.com/formancehq/go-libs/v2/testing/testservice"
	"github.com/formancehq/go-libs/v2/testing/utils"
	"github.com/formancehq/ledger/cmd"
	"github.com/onsi/ginkgo/v2/dsl/core"
	"io"
)

func DeferTestServer(debug bool, output io.Writer, configurationProvider func() ServeConfiguration) *utils.Deferred[*Server] {
	return testservice.DeferNew[ServeConfiguration](
		cmd.NewRootCommand,
		func() testservice.Configuration[ServeConfiguration] {
			return testservice.Configuration[ServeConfiguration]{
				CommonConfiguration: testservice.CommonConfiguration{
					Debug:  debug,
					Output: output,
				},
				Configuration: configurationProvider(),
			}
		},
		testservice.WithLogger(core.GinkgoT()),
		testservice.WithInstruments(testservice.HTTPServerInstrumentation()),
	)
}

func NewTestServer(configuration testservice.Configuration[ServeConfiguration], options ...testservice.Option) *testservice.Service[ServeConfiguration] {
	return testservice.New[ServeConfiguration](
		cmd.NewRootCommand,
		configuration,
		append(options, testservice.WithInstruments(testservice.HTTPServerInstrumentation()))...,
	)
}

func DeferTestWorker(debug bool, output io.Writer, configurationProvider func() WorkerConfiguration) *utils.Deferred[*Worker] {
	return testservice.DeferNew[WorkerConfiguration](cmd.NewRootCommand, func() testservice.Configuration[WorkerConfiguration] {
		return testservice.Configuration[WorkerConfiguration]{
			CommonConfiguration: testservice.CommonConfiguration{
				Debug:  debug,
				Output: output,
			},
			Configuration: configurationProvider(),
		}
	})
}
