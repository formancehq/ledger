//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Connectors API tests", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	testServer := NewTestServer(func() Configuration {
		return Configuration{
			PostgresConfiguration: db.GetValue().ConnectionOptions(),
			Output:                GinkgoWriter,
			Debug:                 debug,
			NatsURL:               natsServer.GetValue().ClientURL(),
			ExperimentalFeatures:  true,
		}
	})
	When("creating a new connector", func() {
		var (
			createConnectorRequest  components.V2CreateConnectorRequest
			createConnectorResponse *components.V2CreateConnectorResponse
			err                     error
		)
		BeforeEach(func() {
			createConnectorRequest = components.V2CreateConnectorRequest{
				Driver: "clickhouse",
				Config: map[string]any{
					"dsn": "clickhouse://localhost:9000",
				},
			}
		})
		BeforeEach(func() {
			createConnectorResponse, err = CreateConnector(ctx, testServer.GetValue(), createConnectorRequest)
		})
		It("should be ok", func() {
			Expect(err).To(BeNil())
		})
		Context("then deleting it", func() {
			BeforeEach(func() {
				err = DeleteConnector(ctx, testServer.GetValue(), operations.V2DeleteConnectorRequest{
					ConnectorID: createConnectorResponse.Data.ID,
				})
			})
			It("Should be ok", func() {
				Expect(err).To(BeNil())
			})
		})
	})
})
