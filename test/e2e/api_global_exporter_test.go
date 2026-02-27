//go:build it

package test_suite

import (
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/testing/deferred"
	. "github.com/formancehq/go-libs/v4/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v4/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v4/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v4/testing/testservice"

	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

var _ = Context("Global logs exporter tests", func() {
	var (
		ctx = logging.TestingContext()
	)

	When("using the global exporter with http driver", func() {
		var (
			collector  *Collector
			httpDriver Driver
		)
		httpDriverDeferred := deferred.New[Driver]()

		BeforeEach(func() {
			httpDriverDeferred.Reset()
			collector = NewCollector()
			httpDriver = NewHTTPDriver(GinkgoT(), collector)
			httpDriverDeferred.SetValue(httpDriver)
		})

		Context("creating transactions", func() {
			db := UseTemplatedDatabase()
			testServer := DeferTestServer(
				DeferMap(db, (*pgtesting.Database).ConnectionOptions),
				testservice.WithInstruments(
					testservice.NatsInstrumentation(DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)),
					testservice.DebugInstrumentation(debug),
					testservice.OutputInstrumentation(GinkgoWriter),
					ExperimentalGlobalExporterInstrumentation(httpDriverDeferred),
				),
				testservice.WithLogger(GinkgoT()),
			)

			BeforeEach(func(specContext SpecContext) {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
					Ledger: "default",
				})
				Expect(err).To(BeNil())
			})

			When("a transaction is created", func() {
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

				It("should be forwarded to the HTTP exporter", func() {
					Eventually(func(g Gomega) []drivers.LogWithLedger {
						messages, err := httpDriver.ReadMessages(ctx)
						g.Expect(err).To(BeNil())
						return messages
					}).
						WithTimeout(10 * time.Second).
						WithPolling(100 * time.Millisecond).
						Should(HaveLen(1))
				})

				When("a second transaction is created", func() {
					BeforeEach(func(specContext SpecContext) {
						// Wait for first message first
						Eventually(func(g Gomega) []drivers.LogWithLedger {
							messages, err := httpDriver.ReadMessages(ctx)
							g.Expect(err).To(BeNil())
							return messages
						}).
							WithTimeout(10 * time.Second).
							WithPolling(100 * time.Millisecond).
							Should(HaveLen(1))

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

					It("should have both logs exported", func() {
						Eventually(func(g Gomega) []drivers.LogWithLedger {
							messages, err := httpDriver.ReadMessages(ctx)
							g.Expect(err).To(BeNil())
							return messages
						}).
							WithTimeout(10 * time.Second).
							WithPolling(100 * time.Millisecond).
							Should(HaveLen(2))
					})
				})
			})
		})

		Context("catch-up after restart", func() {
			db := UseTemplatedDatabase()
			connectionOptions := DeferMap(db, (*pgtesting.Database).ConnectionOptions)

			var (
				firstCollector  *Collector
				firstHTTPDriver Driver
			)
			firstHTTPDriverDeferred := deferred.New[Driver]()

			BeforeEach(func() {
				firstHTTPDriverDeferred.Reset()
				firstCollector = NewCollector()
				firstHTTPDriver = NewHTTPDriver(GinkgoT(), firstCollector)
				firstHTTPDriverDeferred.SetValue(firstHTTPDriver)
			})

			// Start with a first server instance that creates some transactions
			firstServer := DeferTestServer(
				connectionOptions,
				testservice.WithInstruments(
					testservice.NatsInstrumentation(DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)),
					testservice.DebugInstrumentation(debug),
					testservice.OutputInstrumentation(GinkgoWriter),
					ExperimentalGlobalExporterInstrumentation(firstHTTPDriverDeferred),
				),
				testservice.WithLogger(GinkgoT()),
			)

			BeforeEach(func(specContext SpecContext) {
				// Create ledger and a transaction on first server
				_, err := Wait(specContext, DeferClient(firstServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
					Ledger: "default",
				})
				Expect(err).To(BeNil())

				_, err = Wait(specContext, DeferClient(firstServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
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

				// Wait for the first export to complete
				Eventually(func(g Gomega) []drivers.LogWithLedger {
					messages, err := firstHTTPDriver.ReadMessages(ctx)
					g.Expect(err).To(BeNil())
					return messages
				}).
					WithTimeout(10 * time.Second).
					WithPolling(100 * time.Millisecond).
					Should(HaveLen(1))

				// Stop the first server
				srv, err := firstServer.Wait(specContext)
				Expect(err).To(BeNil())
				Expect(srv.Stop(ctx)).To(BeNil())
			})

			When("a new server starts with the same database and a new exporter target", func() {
				secondServer := DeferTestServer(
					connectionOptions,
					testservice.WithInstruments(
						testservice.NatsInstrumentation(DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)),
						testservice.DebugInstrumentation(debug),
						testservice.OutputInstrumentation(GinkgoWriter),
						ExperimentalGlobalExporterInstrumentation(httpDriverDeferred),
					),
					testservice.WithLogger(GinkgoT()),
				)

				It("should not re-export already exported logs", func(specContext SpecContext) {
					// Give the server time to start and run catch-up
					_, err := Wait(specContext, DeferClient(secondServer)).Ledger.GetInfo(ctx)
					Expect(err).To(BeNil())

					// The new collector should have 0 messages — logs already exported
					Consistently(func(g Gomega) []drivers.LogWithLedger {
						messages, err := httpDriver.ReadMessages(ctx)
						g.Expect(err).To(BeNil())
						return messages
					}).
						WithTimeout(5 * time.Second).
						WithPolling(100 * time.Millisecond).
						Should(HaveLen(0))
				})

				When("a new transaction is created on the second server", func() {
					BeforeEach(func(specContext SpecContext) {
						_, err := Wait(specContext, DeferClient(secondServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
							Ledger: "default",
							V2PostTransaction: components.V2PostTransaction{
								Postings: []components.V2Posting{{
									Amount:      big.NewInt(300),
									Asset:       "GBP",
									Destination: "bank",
									Source:      "world",
								}},
							},
						})
						Expect(err).To(Succeed())
					})

					It("should export only the new log", func() {
						Eventually(func(g Gomega) []drivers.LogWithLedger {
							messages, err := httpDriver.ReadMessages(ctx)
							g.Expect(err).To(BeNil())
							return messages
						}).
							WithTimeout(10 * time.Second).
							WithPolling(100 * time.Millisecond).
							Should(HaveLen(1))
					})
				})
			})

			When("a new server starts with the reset flag", func() {
				resetServer := DeferTestServer(
					connectionOptions,
					testservice.WithInstruments(
						testservice.NatsInstrumentation(DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)),
						testservice.DebugInstrumentation(debug),
						testservice.OutputInstrumentation(GinkgoWriter),
						ExperimentalGlobalExporterInstrumentation(httpDriverDeferred),
						GlobalExporterResetInstrumentation(),
					),
					testservice.WithLogger(GinkgoT()),
				)

				It("should re-export all logs from the beginning", func(specContext SpecContext) {
					// Wait for the reset server to start and catch up
					_, err := Wait(specContext, DeferClient(resetServer)).Ledger.GetInfo(ctx)
					Expect(err).To(BeNil())

					Eventually(func(g Gomega) []drivers.LogWithLedger {
						messages, err := httpDriver.ReadMessages(ctx)
						g.Expect(err).To(BeNil())
						return messages
					}).
						WithTimeout(10 * time.Second).
						WithPolling(100 * time.Millisecond).
						Should(HaveLen(1))
				})
			})
		})
	})
})
