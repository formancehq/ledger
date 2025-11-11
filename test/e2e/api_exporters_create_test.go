//go:build it

package test_suite

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/api"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"

	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

var _ = Context("Exporters creation API tests", func() {
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
			ExperimentalExportersInstrumentation(),
			ExperimentalEnableWorker(),
		),
		testservice.WithLogger(GinkgoT()),
	)

	When("creating a new exporter", func() {
		var (
			createExporterRequest  components.V2ExporterConfiguration
			createExporterResponse *operations.V2CreateExporterResponse
			err                    error
		)
		JustBeforeEach(func(specContext SpecContext) {
			createExporterResponse, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateExporter(ctx, createExporterRequest)
		})
		Context("with invalid configuration", func() {
			BeforeEach(func() {
				createExporterRequest = components.V2ExporterConfiguration{
					Driver: "clickhouse",
				}
			})
			It("should return an error", func() {
				Expect(err).ToNot(BeNil())
				Expect(err).To(api.HaveErrorCode(string(components.V2ErrorsEnumValidation)))
			})
		})
		Context("with valid configuration", func() {
			BeforeEach(func() {
				createExporterRequest = components.V2ExporterConfiguration{
					Driver: "clickhouse",
					Config: map[string]any{
						"dsn": "clickhouse://localhost:9000",
					},
				}
			})
			It("should be ok", func() {
				Expect(err).To(BeNil())
			})
			Context("then deleting it", func() {
				BeforeEach(func(specContext SpecContext) {
					Expect(err).To(BeNil())
					_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteExporter(ctx, operations.V2DeleteExporterRequest{
						ExporterID: createExporterResponse.V2CreateExporterResponse.Data.ID,
					})
				})
				It("Should be ok", func() {
					Expect(err).To(BeNil())
				})
			})
		})
	})
})
