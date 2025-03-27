//go:build it

package test_suite

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/testing/platform/clickhousetesting"
	. "github.com/formancehq/go-libs/v2/testing/utils"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"math/big"
	"os"
	"strings"
	"time"
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

func withDriver(name string, connectorFactory func() Driver, fn func(p *Deferred[Driver])) {
	Context(fmt.Sprintf("with driver '%s'", name), func() {
		ret := NewDeferred[Driver]()
		BeforeEach(func() {
			ret.Reset()
			ret.SetValue(connectorFactory())
		})
		fn(ret)
	})
}

// driversSetup allow to define a ginkgo node factory function for each connector
// This allows to configure the environment for the connector
var driversSetup = map[string]func(func(d *Deferred[Driver])){
	"http": func(fn func(d *Deferred[Driver])) {
		withDriver("http", func() Driver {
			return NewHTTPDriver(GinkgoT(), NewCollector())
		}, fn)
	},
	"clickhouse": func(fn func(d *Deferred[Driver])) {
		clickhousetesting.WithNewDatabase(clickhouseServer, func(db *Deferred[*clickhousetesting.Database]) {
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
		testServer := NewTestServer(func() Configuration {
			return Configuration{
				CommonConfiguration: CommonConfiguration{
					PostgresConfiguration:  db.GetValue().ConnectionOptions(),
					Output:                 GinkgoWriter,
					Debug:                  debug,
					ExperimentalFeatures:   true,
					ExperimentalConnectors: true,
				},
				NatsURL: natsServer.GetValue().ClientURL(),
				WorkerConfiguration: &WorkerConfiguration{
					PipelinesSyncPeriod:      100 * time.Millisecond,
					PipelinesPushRetryPeriod: 100 * time.Millisecond,
					PipelinesPullInterval:    100 * time.Millisecond,
				},
				WorkerEnabled: true,
			}
		})
		runPipelinesTests(ctx, testServer)
	})

	Context("With a single instance, and worker on a separate process", func() {
		testServer := NewTestServer(func() Configuration {
			return Configuration{
				CommonConfiguration: CommonConfiguration{
					PostgresConfiguration:  db.GetValue().ConnectionOptions(),
					Output:                 GinkgoWriter,
					Debug:                  debug,
					ExperimentalFeatures:   true,
					ExperimentalConnectors: true,
				},
				NatsURL: natsServer.GetValue().ClientURL(),
			}
		})
		_ = NewTestWorker(func() WorkerServiceConfiguration {
			return WorkerServiceConfiguration{
				CommonConfiguration: CommonConfiguration{
					PostgresConfiguration:  db.GetValue().ConnectionOptions(),
					Output:                 GinkgoWriter,
					Debug:                  debug,
					ExperimentalFeatures:   true,
					ExperimentalConnectors: true,
				},
				WorkerConfiguration: WorkerConfiguration{
					PipelinesSyncPeriod:      100 * time.Millisecond,
					PipelinesPushRetryPeriod: 100 * time.Millisecond,
					PipelinesPullInterval:    100 * time.Millisecond,
				},
			}
		})
		runPipelinesTests(ctx, testServer)
	})
})

func runPipelinesTests(ctx context.Context, testServer *Deferred[*Server]) {
	for _, driverName := range enabledReplicationDrivers() {
		setup, ok := driversSetup[driverName]
		if !ok {
			Fail(fmt.Sprintf("Driver '%s' not exists", driverName))
		}
		setup(func(driver *Deferred[Driver]) {
			When("creating a new connector", func() {
				var (
					createConnectorRequest  components.V2CreateConnectorRequest
					createConnectorResponse *components.V2CreateConnectorResponse
					err                     error
				)
				BeforeEach(func() {
					config := driver.GetValue().Config()
					// Set batching to 1 to make testing easier
					config["batching"] = map[string]any{
						"maxItems": 1,
					}
					createConnectorRequest = components.V2CreateConnectorRequest{
						Driver: driverName,
						Config: config,
					}
				})
				BeforeEach(func() {
					createConnectorResponse, err = CreateConnector(ctx, testServer.GetValue(), createConnectorRequest)
				})
				It("should be ok", func() {
					Expect(err).To(BeNil())
				})
				Context("then creating a ledger and a pipeline", func() {
					var (
						createPipelineResponse *components.V2CreatePipelineResponse
					)
					BeforeEach(func() {
						Expect(err).To(BeNil())
						err = CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
							Ledger: "default",
						})
						Expect(err).To(BeNil())

						createPipelineResponse, err = CreatePipeline(ctx, testServer.GetValue(), operations.V2CreatePipelineRequest{
							Ledger: "default",
							V2CreatePipelineRequest: &components.V2CreatePipelineRequest{
								ConnectorID: createConnectorResponse.Data.ID,
							},
						})
					})
					It("Should be ok", func() {
						Expect(err).To(BeNil())
					})
					Context("then deleting it", func() {
						BeforeEach(func() {
							Expect(err).To(Succeed())
							err = DeletePipeline(ctx, testServer.GetValue(), operations.V2DeletePipelineRequest{
								Ledger:     "default",
								PipelineID: createPipelineResponse.Data.ID,
							})
						})
						It("Should be ok", func() {
							Expect(err).To(BeNil())
						})
					})
					Context("then creating a transaction", func() {
						BeforeEach(func() {
							_, err := CreateTransaction(ctx, testServer.GetValue(), operations.V2CreateTransactionRequest{
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
							BeforeEach(func() {
								shouldHaveMessage(1)
								Expect(driver.GetValue().Clear(ctx)).To(BeNil())
								shouldHaveMessage(0)

								err = ResetPipeline(ctx, testServer.GetValue(), operations.V2ResetPipelineRequest{
									Ledger:     "default",
									PipelineID: createPipelineResponse.Data.ID,
								})
								Expect(err).To(BeNil())
							})
							It("should be forwarded again to the driver", func() {
								shouldHaveMessage(1)
							})
						})
						Context("then stopping the pipeline", func() {
							BeforeEach(func() {
								shouldHaveMessage(1)

								err = StopPipeline(ctx, testServer.GetValue(), operations.V2StopPipelineRequest{
									Ledger:     "default",
									PipelineID: createPipelineResponse.Data.ID,
								})

								// As we don't actually have a way to ensure the pipeline is stopped
								// todo: find a better way to check for the pipeline termination
								<-time.After(500 * time.Millisecond)
							})
							It("should be ok", func() {
								Expect(err).To(BeNil())
							})
							Context("then creating a new tx", func() {
								BeforeEach(func() {
									Expect(err).To(BeNil())

									_, err := CreateTransaction(ctx, testServer.GetValue(), operations.V2CreateTransactionRequest{
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
									BeforeEach(func() {
										err = StartPipeline(ctx, testServer.GetValue(), operations.V2StartPipelineRequest{
											Ledger:     "default",
											PipelineID: createPipelineResponse.Data.ID,
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
