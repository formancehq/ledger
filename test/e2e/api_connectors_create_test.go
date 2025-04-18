//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/v3/logging"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Connectors API tests", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	testServer := DeferTestServer(
		DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(
			testservice.NatsInstrumentation(DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)),
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
			ExperimentalFeaturesInstrumentation(),
			ExperimentalConnectorsInstrumentation(),
			ExperimentalEnableWorker(),
		),
		testservice.WithLogger(GinkgoT()),
	)

	When("creating a new connector", func() {
		var (
			createConnectorRequest  components.V2ConnectorConfiguration
			createConnectorResponse *operations.V2CreateConnectorResponse
			err                     error
		)
		BeforeEach(func() {
			createConnectorRequest = components.V2ConnectorConfiguration{
				Driver: "clickhouse",
				Config: map[string]any{
					"dsn": "clickhouse://localhost:9000",
				},
			}
		})
		BeforeEach(func(specContext SpecContext) {
			createConnectorResponse, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateConnector(ctx, createConnectorRequest)
		})
		It("should be ok", func() {
			Expect(err).To(BeNil())
		})
		Context("then deleting it", func() {
			BeforeEach(func(specContext SpecContext) {
				Expect(err).To(BeNil())
				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteConnector(ctx, operations.V2DeleteConnectorRequest{
					ConnectorID: createConnectorResponse.V2CreateConnectorResponse.Data.ID,
				})
			})
			It("Should be ok", func() {
				Expect(err).To(BeNil())
			})
		})
	})
})
