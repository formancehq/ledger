//go:build it

package test_suite

import (
	"context"
	"database/sql"
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerevents "github.com/formancehq/ledger/pkg/events"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/uptrace/bun"
	"math/big"
)

var _ = Context("Ledger application lifecycle tests", func() {
	var (
		db  = pgtesting.UsePostgresDatabase(pgServer)
		ctx = logging.TestingContext()
	)

	Context("Pending transaction should be fully processed before stopping or restarting the server", func() {
		testServer := NewTestServer(func() Configuration {
			return Configuration{
				PostgresConfiguration: db.GetValue().ConnectionOptions(),
				Output:                GinkgoWriter,
				Debug:                 debug,
				NatsURL:               natsServer.GetValue().ClientURL(),
			}
		})
		var events chan *nats.Msg
		BeforeEach(func() {
			events = testServer.GetValue().Subscribe()
		})

		When("starting the service", func() {
			It("should be ok", func() {
				info, err := testServer.GetValue().Client().Ledger.GetInfo(ctx)
				Expect(err).NotTo(HaveOccurred())
				Expect(info.V2ConfigInfoResponse.Version).To(Equal("develop"))
			})
		})
		When("restarting the service", func() {
			BeforeEach(func(ctx context.Context) {
				testServer.GetValue().Restart(ctx)
			})
			It("should be ok", func() {})
		})
		When("having some in flight transactions on a ledger", func() {
			var (
				sqlTx                bun.Tx
				countTransactions    = 80
				serverRestartTimeout = 10 * time.Second
			)
			BeforeEach(func() {
				err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
					Ledger: "foo",
				})
				Expect(err).ToNot(HaveOccurred())

				// lock logs table to block transactions creation requests
				// the first tx will block on the log insertion
				// the next transaction will block earlier on advisory lock acquirement for accounts
				db := testServer.GetValue().Database()
				sqlTx, err = db.BeginTx(ctx, &sql.TxOptions{})
				Expect(err).To(BeNil())
				DeferCleanup(func() {
					_ = sqlTx.Rollback()
				})

				_, err = sqlTx.NewRaw("lock table _default.logs").Exec(ctx)
				Expect(err).To(BeNil())

				// Create transactions in go routines
				for i := 0; i < countTransactions; i++ {
					go func() {
						defer GinkgoRecover()

						_, err := CreateTransaction(ctx, testServer.GetValue(), operations.V2CreateTransactionRequest{
							Ledger: "foo",
							V2PostTransaction: components.V2PostTransaction{
								Postings: []components.V2Posting{{
									Amount:      big.NewInt(100),
									Asset:       "USD",
									Destination: "bank",
									Source:      "world",
								}},
							},
						})
						Expect(err).To(BeNil())
					}()
				}

				// check postgres locks
				Eventually(func(g Gomega) int {
					count, err := db.NewSelect().
						Table("pg_stat_activity").
						Where("state <> 'idle' and pid <> pg_backend_pid()").
						Where(`query like 'INSERT INTO "_default".accounts%'`).
						Count(ctx)
					g.Expect(err).To(BeNil())
					return count
				}).
					WithTimeout(10 * time.Second).
					// Once all the transactions are in pending state, we should have one lock
					// for the first tx, trying to write a new log.
					// And, we should also have countTransactions-1 pending lock for the 'bank' account
					Should(BeNumerically("==", countTransactions-1)) // -1 for the first one
			})
			When("restarting the service", func() {
				BeforeEach(func() {
					// We will restart the server in a separate gorouting
					// the server should not restart until all pending transactions creation requests are fully completed
					restarted := make(chan struct{})
					go func() {
						defer GinkgoRecover()
						defer func() {
							close(restarted)
						}()
						By("restart server", func() {
							ctx, cancel := context.WithTimeout(ctx, serverRestartTimeout)
							DeferCleanup(cancel)

							testServer.GetValue().Restart(ctx)
						})
					}()

					// Once the server is restarting, it should not accept any new connection
					Eventually(func() error {
						_, err := GetInfo(ctx, testServer.GetValue())
						return err
					}).ShouldNot(BeNil())

					// by rollback sql transactions, we allow the blocked routines (which create transactions) to resume.
					By("rollback tx", func() {
						_ = sqlTx.Rollback()
					})

					Eventually(restarted).
						WithTimeout(serverRestartTimeout).
						Should(BeClosed())
				})
				It("in flight transactions should be correctly terminated before", func() {
					transactions, err := ListTransactions(ctx, testServer.GetValue(), operations.V2ListTransactionsRequest{
						Ledger:   "foo",
						PageSize: pointer.For(int64(countTransactions)),
					})
					Expect(err).To(BeNil())
					Expect(transactions.Data).To(HaveLen(countTransactions))

					By("all events should have been properly sent", func() {
						for range countTransactions {
							Eventually(events).Should(Receive(Event(ledgerevents.EventTypeCommittedTransactions)))
						}
					})
				})
			})
		})
	})

	Context("Ledger should respond correctly as well as the minimal schema version is respected", func() {
		var (
			ledgerName = "default"
		)
		BeforeEach(func() {
			bunDB, err := bunconnect.OpenSQLDB(ctx, db.GetValue().ConnectionOptions())
			Expect(err).To(BeNil())

			Expect(driver.Migrate(ctx, bunDB)).To(BeNil())

			_, err = bunDB.NewInsert().
				Model(pointer.For(ledger.MustNewWithDefault(ledgerName))).
				Exec(ctx)
			Expect(err).To(BeNil())

			migrator := bucket.GetMigrator(ledger.DefaultBucket)
			for i := 0; i < bucket.MinimalSchemaVersion; i++ {
				Expect(migrator.UpByOne(ctx, bunDB)).To(BeNil())
			}
		})
		testServer := NewTestServer(func() Configuration {
			return Configuration{
				PostgresConfiguration: db.GetValue().ConnectionOptions(),
				Output:                GinkgoWriter,
				Debug:                 debug,
				NatsURL:               natsServer.GetValue().ClientURL(),
				DisableAutoUpgrade:    true,
			}
		})
		It("should be ok", func() {
			By("we should be able to create a new transaction", func() {
				_, err := CreateTransaction(ctx, testServer.GetValue(), operations.V2CreateTransactionRequest{
					Ledger: ledgerName,
					V2PostTransaction: components.V2PostTransaction{
						Metadata: map[string]string{},
						Postings: []components.V2Posting{
							{
								Amount:      big.NewInt(100),
								Asset:       "USD",
								Source:      "world",
								Destination: "alice",
							},
						},
					},
				})
				Expect(err).To(BeNil())
			})
			By("we should be able to list transactions", func() {
				transactions, err := ListTransactions(ctx, testServer.GetValue(), operations.V2ListTransactionsRequest{
					Ledger: ledgerName,
				})
				Expect(err).To(BeNil())
				Expect(transactions.Data).To(HaveLen(1))
			})
		})
	})
})
