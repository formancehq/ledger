//go:build it

package test_suite

import (
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/api"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"

	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

var _ = Context("Exporters update API tests", func() {
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
			ExperimentalPipelinesSyncPeriodInstrumentation(500*time.Millisecond),
		),
		testservice.WithLogger(GinkgoT()),
	)

	When("updating an exporter", func() {
		var (
			createExporterResponse *operations.V2CreateExporterResponse
			updateExporterRequest  components.V2ExporterConfiguration
			err                    error
		)

		BeforeEach(func(specContext SpecContext) {
			// Create an exporter first
			createExporterResponse, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateExporter(ctx, components.V2ExporterConfiguration{
				Driver: "clickhouse",
				Config: map[string]any{
					"dsn": "clickhouse://localhost:9000",
				},
			})
			Expect(err).To(BeNil())
		})

		Context("with invalid configuration", func() {
			BeforeEach(func() {
				updateExporterRequest = components.V2ExporterConfiguration{
					Driver: "clickhouse",
					// Missing required config
				}
			})
			JustBeforeEach(func(specContext SpecContext) {
				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.UpdateExporter(ctx, operations.V2UpdateExporterRequest{
					ExporterID:              createExporterResponse.V2CreateExporterResponse.Data.ID,
					V2ExporterConfiguration: updateExporterRequest,
				})
			})
			It("should return an error", func() {
				Expect(err).ToNot(BeNil())
				Expect(err).To(api.HaveErrorCode(string(components.V2ErrorsEnumValidation)))
			})
		})

		Context("with non-existent exporter ID", func() {
			JustBeforeEach(func(specContext SpecContext) {
				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.UpdateExporter(ctx, operations.V2UpdateExporterRequest{
					ExporterID: "non-existent-id",
					V2ExporterConfiguration: components.V2ExporterConfiguration{
						Driver: "clickhouse",
						Config: map[string]any{
							"dsn": "clickhouse://localhost:9000",
						},
					},
				})
			})
			It("should return a not found error", func() {
				Expect(err).ToNot(BeNil())
				Expect(err).To(api.HaveErrorCode(string(components.V2ErrorsEnumNotFound)))
			})
		})

		Context("with valid configuration", func() {
			BeforeEach(func() {
				updateExporterRequest = components.V2ExporterConfiguration{
					Driver: "clickhouse",
					Config: map[string]any{
						"dsn": "clickhouse://localhost:9001",
					},
				}
			})
			JustBeforeEach(func(specContext SpecContext) {
				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.UpdateExporter(ctx, operations.V2UpdateExporterRequest{
					ExporterID:              createExporterResponse.V2CreateExporterResponse.Data.ID,
					V2ExporterConfiguration: updateExporterRequest,
				})
			})
			It("should be ok", func() {
				Expect(err).To(BeNil())
			})
			Context("then getting the exporter", func() {
				var getExporterResponse *operations.V2GetExporterStateResponse
				JustBeforeEach(func(specContext SpecContext) {
					Expect(err).To(BeNil())
					getExporterResponse, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.GetExporterState(ctx, operations.V2GetExporterStateRequest{
						ExporterID: createExporterResponse.V2CreateExporterResponse.Data.ID,
					})
				})
				It("should return the updated configuration", func() {
					Expect(err).To(BeNil())
					Expect(getExporterResponse.V2GetExporterStateResponse).ToNot(BeNil())
					Expect(getExporterResponse.V2GetExporterStateResponse.Data.Driver).To(Equal("clickhouse"))
					Expect(getExporterResponse.V2GetExporterStateResponse.Data.Config).To(HaveKey("dsn"))
					Expect(getExporterResponse.V2GetExporterStateResponse.Data.Config["dsn"]).To(Equal("clickhouse://localhost:9001"))
				})
			})
			Context("then updating with a different driver", func() {
				BeforeEach(func() {
					updateExporterRequest = components.V2ExporterConfiguration{
						Driver: "http",
						Config: map[string]any{
							"url": "http://example.com",
						},
					}
				})
				JustBeforeEach(func(specContext SpecContext) {
					Expect(err).To(BeNil())
					_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.UpdateExporter(ctx, operations.V2UpdateExporterRequest{
						ExporterID:              createExporterResponse.V2CreateExporterResponse.Data.ID,
						V2ExporterConfiguration: updateExporterRequest,
					})
				})
				It("should be ok", func() {
					Expect(err).To(BeNil())
				})
				Context("then getting the exporter", func() {
					var getExporterResponse *operations.V2GetExporterStateResponse
					JustBeforeEach(func(specContext SpecContext) {
						Expect(err).To(BeNil())
						getExporterResponse, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.GetExporterState(ctx, operations.V2GetExporterStateRequest{
							ExporterID: createExporterResponse.V2CreateExporterResponse.Data.ID,
						})
					})
					It("should return the updated driver and configuration", func() {
						Expect(err).To(BeNil())
						Expect(getExporterResponse.V2GetExporterStateResponse).ToNot(BeNil())
						Expect(getExporterResponse.V2GetExporterStateResponse.Data.Driver).To(Equal("http"))
						Expect(getExporterResponse.V2GetExporterStateResponse.Data.Config).To(HaveKey("url"))
						Expect(getExporterResponse.V2GetExporterStateResponse.Data.Config["url"]).To(Equal("http://example.com"))
					})
				})
			})
		})
	})

	When("updating an exporter with running pipelines", func() {
		var (
			firstCollector         *Collector
			secondCollector        *Collector
			firstHTTPDriver        Driver
			secondHTTPDriver       Driver
			createExporterResponse *operations.V2CreateExporterResponse
			err                    error
		)

		BeforeEach(func() {
			// Create two HTTP collectors and drivers
			firstCollector = NewCollector()
			firstHTTPDriver = NewHTTPDriver(GinkgoT(), firstCollector)

			secondCollector = NewCollector()
			secondHTTPDriver = NewHTTPDriver(GinkgoT(), secondCollector)
		})

		BeforeEach(func(specContext SpecContext) {
			// Create exporter pointing to first HTTP server
			createExporterResponse, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateExporter(ctx, components.V2ExporterConfiguration{
				Driver: "http",
				Config: map[string]any{
					"url": firstHTTPDriver.Config()["url"],
					"batching": map[string]any{
						"maxItems": 1,
					},
				},
			})
			Expect(err).To(BeNil())

			// Create ledger
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: "default",
			})
			Expect(err).To(BeNil())

			// Create pipeline
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreatePipeline(ctx, operations.V2CreatePipelineRequest{
				Ledger: "default",
				V2CreatePipelineRequest: &components.V2CreatePipelineRequest{
					ExporterID: createExporterResponse.V2CreateExporterResponse.Data.ID,
				},
			})
			Expect(err).To(BeNil())
		})

		Context("creating a transaction", func() {
			BeforeEach(func(specContext SpecContext) {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
					Ledger: "default",
					V2PostTransaction: components.V2PostTransaction{
						Postings: []components.V2Posting{{
							Amount:      big.NewInt(100),
							Asset:       "USD",
							Destination: "bank",
							Source:      "world",
						}},
					},
				})
				Expect(err).To(Succeed())
			})

			It("should be forwarded to the first HTTP server", func() {
				Eventually(func(g Gomega) []drivers.LogWithLedger {
					messages, err := firstHTTPDriver.ReadMessages(ctx)
					g.Expect(err).To(BeNil())
					return messages
				}).
					WithTimeout(10 * time.Second).
					WithPolling(100 * time.Millisecond).
					Should(HaveLen(1))

				// Verify second server has no messages
				messages, err := secondHTTPDriver.ReadMessages(ctx)
				Expect(err).To(BeNil())
				Expect(messages).To(HaveLen(0))
			})

			Context("then updating the exporter to point to the second HTTP server", func() {
				BeforeEach(func(specContext SpecContext) {
					// Wait for first message to be received
					Eventually(func(g Gomega) []drivers.LogWithLedger {
						messages, err := firstHTTPDriver.ReadMessages(ctx)
						g.Expect(err).To(BeNil())
						return messages
					}).
						WithTimeout(10 * time.Second).
						WithPolling(100 * time.Millisecond).
						Should(HaveLen(1))

					// Clear first collector to verify new messages go to second server
					Expect(firstHTTPDriver.Clear(ctx)).To(BeNil())

					// Update exporter to point to second HTTP server
					_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.UpdateExporter(ctx, operations.V2UpdateExporterRequest{
						ExporterID: createExporterResponse.V2CreateExporterResponse.Data.ID,
						V2ExporterConfiguration: components.V2ExporterConfiguration{
							Driver: "http",
							Config: map[string]any{
								"url": secondHTTPDriver.Config()["url"],
								"batching": map[string]any{
									"maxItems": 1,
								},
							},
						},
					})
					Expect(err).To(BeNil())
				})

				It("should succeed", func() {
					Expect(err).To(BeNil())
				})

				Context("then creating a new transaction", func() {
					BeforeEach(func(specContext SpecContext) {

						_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
							Ledger: "default",
							V2PostTransaction: components.V2PostTransaction{
								Postings: []components.V2Posting{{
									Amount:      big.NewInt(200),
									Asset:       "EUR",
									Destination: "bank",
									Source:      "world",
								}},
							},
						})
						Expect(err).To(Succeed())
					})

					It("should be forwarded to the second HTTP server", func() {
						Eventually(func(g Gomega) []drivers.LogWithLedger {
							messages, err := secondHTTPDriver.ReadMessages(ctx)
							g.Expect(err).To(BeNil())
							return messages
						}).
							WithTimeout(10 * time.Second).
							WithPolling(100 * time.Millisecond).
							Should(HaveLen(1))

						// Verify first server still has no new messages (was cleared)
						messages, err := firstHTTPDriver.ReadMessages(ctx)
						Expect(err).To(BeNil())
						Expect(messages).To(HaveLen(0))
					})
				})
			})
		})
	})
})
