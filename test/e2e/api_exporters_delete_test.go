//go:build it

package test_suite

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v3/logging"
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

	When("creating a new exporter, a ledger and a pipeline", func() {
		var (
			createExporterResponse *operations.V2CreateExporterResponse
			err                    error
		)
		BeforeEach(func(specContext SpecContext) {
			createExporterResponse, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateExporter(ctx, components.V2ExporterConfiguration{
				Driver: "http",
				Config: map[string]any{
					"url": "http://example.com",
				},
			})
			Expect(err).To(BeNil())

			Expect(err).To(BeNil())
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: "default",
			})
			Expect(err).To(BeNil())

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreatePipeline(ctx, operations.V2CreatePipelineRequest{
				Ledger: "default",
				V2CreatePipelineRequest: &components.V2CreatePipelineRequest{
					ExporterID: createExporterResponse.V2CreateExporterResponse.Data.ID,
				},
			})
			Expect(err).To(BeNil())
		})
		Context("then deleting the exporter", func() {
			BeforeEach(func(specContext SpecContext) {
				Expect(err).To(BeNil())
				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteExporter(ctx, operations.V2DeleteExporterRequest{
					ExporterID: createExporterResponse.V2CreateExporterResponse.Data.ID,
				})
			})
			It("should be ok", func() {
				Expect(err).To(BeNil())
			})
			Context("then trying to delete the pipeline", func() {
				BeforeEach(func(specContext SpecContext) {
					Expect(err).To(BeNil())
					_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeletePipeline(ctx, operations.V2DeletePipelineRequest{
						Ledger:     "default",
						PipelineID: createExporterResponse.V2CreateExporterResponse.Data.ID,
					})
				})
				It("should fail", func() {
					Expect(err).To(HaveOccurred())
				})
			})
		})
	})
})
