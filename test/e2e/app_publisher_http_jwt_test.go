//go:build it

package test_suite

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v5/pkg/storage/bun/connect"
	. "github.com/formancehq/go-libs/v5/pkg/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v5/pkg/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"

	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

// Regression test for TS-456.
//
// `authnfx.JWTModule` is loaded unconditionally by `cmd.NewServeCommand`
// (regardless of whether auth is enabled), and `messagingfx.HTTPModule` is
// loaded when --publisher-http-enabled is set. Both call
// fx.Supply(http.DefaultClient) at the parent fx scope; uber/fx rejects the
// duplicate provider and `ledger serve` fails to start with:
//
//	cannot provide *http.Client from [0]: already provided by "reflect".makeFuncStub
var _ = Context("Ledger startup with --publisher-http-enabled (TS-456)", func() {
	db := UseTemplatedDatabase()
	connectionOptions := DeferMap(db, func(from *pgtesting.Database) connect.ConnectionOptions {
		return connect.ConnectionOptions{
			DatabaseSourceName: from.ConnectionOptions().DatabaseSourceName,
			MaxOpenConns:       100,
		}
	})

	testServer := DeferTestServer(
		connectionOptions,
		testservice.WithInstruments(
			testservice.AppendArgsInstrumentation(
				"--publisher-http-enabled",
				"--publisher-topic-mapping", "*:http://127.0.0.1:1",
			),
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
		),
		testservice.WithLogger(GinkgoT()),
	)

	It("should boot without a duplicate *http.Client fx provider", func(specContext SpecContext) {
		_, err := testServer.Wait(specContext)
		Expect(err).To(BeNil())
	})
})
