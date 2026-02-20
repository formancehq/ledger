//go:build it

package test_suite

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v4/grpcserver"
	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/testing/deferred"
	. "github.com/formancehq/go-libs/v4/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v4/testing/platform/clickhousetesting"
	"github.com/formancehq/go-libs/v4/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v4/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v4/testing/testservice"

	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

var (
	defaultEnabledReplicationDrivers = []string{"http", "clickhouse"}
)

func enabledReplicationDrivers() []string {
	fromEnv := os.Getenv("REPLICATION_DRIVERS")
	if fromEnv == "" {
		return defaultEnabledReplicationDrivers
	}
	return strings.Split(fromEnv, ",")
}

func withDriver(name string, exporterFactory func() Driver, fn func(p *deferred.Deferred[Driver])) {
	Context(fmt.Sprintf("with driver '%s'", name), func() {
		ret := deferred.New[Driver]()
		BeforeEach(func() {
			ret.Reset()
			ret.SetValue(exporterFactory())
		})
		fn(ret)
	})
}

// driversSetup allow to define a ginkgo node factory function for each exporter
// This allows to configure the environment for the exporter
var driversSetup = map[string]func(func(d *deferred.Deferred[Driver])){
	"http": func(fn func(d *deferred.Deferred[Driver])) {
		withDriver("http", func() Driver {
			return NewHTTPDriver(GinkgoT(), NewCollector())
		}, fn)
	},
	"clickhouse": func(fn func(d *deferred.Deferred[Driver])) {
		clickhousetesting.WithNewDatabase(clickhouseServer, func(db *deferred.Deferred[*clickhousetesting.Database]) {
			withDriver("clickhouse", func() Driver {
				return NewClickhouseDriver(logger, db.GetValue().ConnString())
			}, fn)
		})
	},
}

var _ = Context("Pipelines API tests", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	Context("With single instance and worker enabled", func() {
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
		runPipelinesTests(ctx, testServer)
	})

	Context("With a single instance, and worker on a separate process", func() {
		connectionOptions := DeferMap(db, (*pgtesting.Database).ConnectionOptions)

		worker := DeferTestWorker(
			connectionOptions,
			testservice.WithInstruments(
				testservice.DebugInstrumentation(debug),
				testservice.OutputInstrumentation(GinkgoWriter),
				ExperimentalFeaturesInstrumentation(),
				ExperimentalExportersInstrumentation(),
				ExperimentalPipelinesPullIntervalInstrumentation(100*time.Millisecond),
				ExperimentalPipelinesPushRetryPeriodInstrumentation(100*time.Millisecond),
			),
			testservice.WithLogger(GinkgoT()),
		)

		testServer := DeferTestServer(
			connectionOptions,
			testservice.WithInstruments(
				testservice.NatsInstrumentation(DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)),
				testservice.DebugInstrumentation(debug),
				testservice.OutputInstrumentation(GinkgoWriter),
				ExperimentalFeaturesInstrumentation(),
				ExperimentalExportersInstrumentation(),
				WorkerAddressInstrumentation(DeferMap(worker, func(from *testservice.Service) string {
					return grpcserver.Address(from.GetContext())
				})),
			),
			testservice.WithLogger(GinkgoT()),
		)

		runPipelinesTests(ctx, testServer)
	})
})

func runPipelinesTests(ctx context.Context, testServer *deferred.Deferred[*testservice.Service]) {
	for _, driverName := range enabledReplicationDrivers() {
		setup, ok := driversSetup[driverName]
		if !ok {
			Fail(fmt.Sprintf("Driver '%s' not exists", driverName))
		}
		setup(func(driver *deferred.Deferred[Driver]) {
			When("creating a new exporter", func() {
				var (
					createExporterRequest  components.V2ExporterConfiguration
					createExporterResponse *operations.V2CreateExporterResponse
					err                    error
				)
				BeforeEach(func() {
					config := driver.GetValue().Config()
					// Set batching to 1 to make testing easier
					config["batching"] = map[string]any{
						"maxItems": 1,
					}
					createExporterRequest = components.V2ExporterConfiguration{
						Driver: driverName,
						Config: config,
					}
				})
				BeforeEach(func(specContext SpecContext) {
					createExporterResponse, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateExporter(ctx, createExporterRequest)
				})
				It("should be ok", func() {
					Expect(err).To(BeNil())
				})
				Context("then creating a ledger and a pipeline", func() {
					var (
						createPipelineResponse *operations.V2CreatePipelineResponse
					)
					BeforeEach(func(specContext SpecContext) {
						Expect(err).To(BeNil())
						_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
							Ledger: "default",
						})
						Expect(err).To(BeNil())

						createPipelineResponse, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreatePipeline(ctx, operations.V2CreatePipelineRequest{
							Ledger: "default",
							V2CreatePipelineRequest: &components.V2CreatePipelineRequest{
								ExporterID: createExporterResponse.V2CreateExporterResponse.Data.ID,
							},
						})
					})
					It("Should be ok", func() {
						Expect(err).To(BeNil())
					})
					Context("then deleting it", func() {
						BeforeEach(func(specContext SpecContext) {
							Expect(err).To(Succeed())
							_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeletePipeline(ctx, operations.V2DeletePipelineRequest{
								Ledger:     "default",
								PipelineID: createPipelineResponse.V2CreatePipelineResponse.Data.ID,
							})
						})
						It("Should be ok", func() {
							Expect(err).To(BeNil())
						})
					})
					Context("then creating a transaction", func() {
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
						shouldHaveMessage := func(i int) {
							GinkgoHelper()

							Eventually(func(g Gomega) []drivers.LogWithLedger {
								messages, err := driver.GetValue().ReadMessages(ctx)
								g.Expect(err).To(BeNil())

								return messages
							}).
								WithTimeout(10 * time.Second).
								WithPolling(100 * time.Millisecond).
								Should(HaveLen(i))
						}
						It("should be forwarded to the driver", func() {
							shouldHaveMessage(1)
						})
						Context("then resetting the pipeline", func() {
							BeforeEach(func(specContext SpecContext) {
								shouldHaveMessage(1)
								Expect(driver.GetValue().Clear(ctx)).To(BeNil())
								shouldHaveMessage(0)

								_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.ResetPipeline(ctx, operations.V2ResetPipelineRequest{
									Ledger:     "default",
									PipelineID: createPipelineResponse.V2CreatePipelineResponse.Data.ID,
								})
								Expect(err).To(BeNil())
							})
							It("should be forwarded again to the driver", func() {
								shouldHaveMessage(1)
							})
						})
						Context("then stopping the pipeline", func() {
							BeforeEach(func(specContext SpecContext) {
								shouldHaveMessage(1)

								_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.StopPipeline(ctx, operations.V2StopPipelineRequest{
									Ledger:     "default",
									PipelineID: createPipelineResponse.V2CreatePipelineResponse.Data.ID,
								})

								// As we don't actually have a way to ensure the pipeline is stopped
								// todo: find a better way to check for the pipeline termination
								<-time.After(500 * time.Millisecond)
							})
							It("should be ok", func() {
								Expect(err).To(BeNil())
							})
							Context("then creating a new tx", func() {
								BeforeEach(func(specContext SpecContext) {
									Expect(err).To(BeNil())

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
								It("should not be exported", func() {
									Consistently(func(g Gomega) []drivers.LogWithLedger {
										messages, err := driver.GetValue().ReadMessages(ctx)
										if err != nil {
											return nil
										}

										return messages
									}).
										WithTimeout(time.Second).
										WithPolling(100 * time.Millisecond).
										Should(HaveLen(1))
								})
								Context("then restarting the pipeline", func() {
									BeforeEach(func(specContext SpecContext) {
										_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.StartPipeline(ctx, operations.V2StartPipelineRequest{
											Ledger:     "default",
											PipelineID: createPipelineResponse.V2CreatePipelineResponse.Data.ID,
										})
									})
									It("should be exported", func() {
										Expect(err).To(BeNil())
										Eventually(func(g Gomega) []drivers.LogWithLedger {
											messages, err := driver.GetValue().ReadMessages(ctx)
											if err != nil {
												return nil
											}

											return messages
										}).
											WithTimeout(10 * time.Second).
											WithPolling(100 * time.Millisecond).
											Should(HaveLen(2))
									})
								})
							})
						})
					})
				})
			})
		})
	}
}
