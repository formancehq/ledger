//go:build it

package test_suite

import (
	"time"

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

var _ = Context("Pipelines API tests", func() {
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
			ExperimentalPipelinesPullIntervalInstrumentation(100*time.Millisecond),
			ExperimentalPipelinesPushRetryPeriodInstrumentation(100*time.Millisecond),
		),
		testservice.WithLogger(GinkgoT()),
	)

	var (
		exporters []*operations.V2CreateExporterResponse
	)
	const countExporters = 3
	BeforeEach(func(specContext SpecContext) {
		for range countExporters {
			// Create an exporter
			exporter, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateExporter(ctx, components.V2ExporterConfiguration{
				Driver: "http",
				Config: map[string]any{
					"url": "http://localhost:8080",
				},
			})
			Expect(err).To(BeNil())

			exporters = append(exporters, exporter)
		}

		// Create a ledger
		_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())

		// Create pipelines
		for i := 0; i < countExporters; i++ {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreatePipeline(ctx, operations.V2CreatePipelineRequest{
				Ledger: "default",
				V2CreatePipelineRequest: &components.V2CreatePipelineRequest{
					ExporterID: exporters[i].V2CreateExporterResponse.Data.ID,
				},
			})
			Expect(err).To(BeNil())
		}
	})
	It("should be ok when listing pipelines", func(specContext SpecContext) {
		// List pipelines
		pipelines, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListPipelines(ctx, operations.V2ListPipelinesRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())
		Expect(pipelines.V2ListPipelinesResponse.Cursor.Data).To(HaveLen(countExporters))
	})
})
