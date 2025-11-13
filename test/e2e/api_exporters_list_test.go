//go:build it

package test_suite

import (
	"fmt"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v3/logging"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"

	"github.com/formancehq/ledger/pkg/client/models/components"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

var _ = Context("Exporters list API tests", func() {
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

	const count = 3
	When(fmt.Sprintf("creating %d new exporters", count), func() {
		BeforeEach(func(specContext SpecContext) {
			for range count {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateExporter(ctx, components.V2ExporterConfiguration{
					Driver: "http",
					Config: map[string]any{
						"url": "http://localhost:8080",
					},
				})
				Expect(err).To(BeNil())
			}
		})
		When("listing them", func() {
			It("should be ok", func(specContext SpecContext) {
				exporters, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListExporters(ctx)
				Expect(err).To(BeNil())
				Expect(exporters.V2ListExportersResponse.Cursor.Data).To(HaveLen(count))
			})
		})
	})
})
