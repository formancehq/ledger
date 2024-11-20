//go:build it

package test_suite

import (
	"database/sql"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"
	. "github.com/formancehq/go-libs/v2/testing/api"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/features"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/uptrace/bun"
	"io"
	"math/big"
)

var _ = Context("Ledger engine tests", func() {
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
	When("creating a new ledger", func() {
		var (
			createLedgerRequest operations.V2CreateLedgerRequest
			err                 error
		)
		BeforeEach(func() {
			createLedgerRequest = operations.V2CreateLedgerRequest{
				Ledger: "foo",
				V2CreateLedgerRequest: &components.V2CreateLedgerRequest{
					Features: features.MinimalFeatureSet,
				},
			}
		})
		JustBeforeEach(func() {
			err = CreateLedger(ctx, testServer.GetValue(), createLedgerRequest)
		})
		Context("with a set of all possible actions", func() {
			JustBeforeEach(func() {
				Expect(err).To(BeNil())

				firstTX, err := CreateTransaction(ctx, testServer.GetValue(), operations.V2CreateTransactionRequest{
					Ledger: createLedgerRequest.Ledger,
					V2PostTransaction: components.V2PostTransaction{
						Script: &components.V2PostTransactionScript{
							Plain: `send [COIN 100] (
								source = @world
								destination = @bob
							)
							set_account_meta(@world, "foo", "bar")
							`,
						},
					},
				})
				Expect(err).To(BeNil())

				// add a tx with a dry run to trigger a hole in ids
				_, err = CreateTransaction(ctx, testServer.GetValue(), operations.V2CreateTransactionRequest{
					Ledger: createLedgerRequest.Ledger,
					DryRun: pointer.For(true),
					V2PostTransaction: components.V2PostTransaction{
						Script: &components.V2PostTransactionScript{
							Plain: `send [COIN 100] (
								source = @world
								destination = @bob
							)
							set_account_meta(@world, "foo", "bar")
							`,
						},
					},
				})
				Expect(err).To(BeNil())

				thirdTx, err := CreateTransaction(ctx, testServer.GetValue(), operations.V2CreateTransactionRequest{
					Ledger: createLedgerRequest.Ledger,
					V2PostTransaction: components.V2PostTransaction{
						Script: &components.V2PostTransactionScript{
							Plain: `send [COIN 100] (
								source = @world
								destination = @bob
							)
							set_account_meta(@world, "foo", "bar")
							`,
						},
					},
				})
				Expect(err).To(BeNil())

				Expect(AddMetadataToTransaction(ctx, testServer.GetValue(), operations.V2AddMetadataOnTransactionRequest{
					Ledger: createLedgerRequest.Ledger,
					ID:     firstTX.ID,
					RequestBody: map[string]string{
						"foo": "bar",
					},
				})).To(BeNil())

				Expect(AddMetadataToTransaction(ctx, testServer.GetValue(), operations.V2AddMetadataOnTransactionRequest{
					Ledger: createLedgerRequest.Ledger,
					ID:     thirdTx.ID,
					RequestBody: map[string]string{
						"foo": "baz",
					},
				})).To(BeNil())

				Expect(AddMetadataToAccount(ctx, testServer.GetValue(), operations.V2AddMetadataToAccountRequest{
					Ledger:  createLedgerRequest.Ledger,
					Address: "bank",
					RequestBody: map[string]string{
						"foo": "bar",
					},
				})).To(BeNil())

				Expect(DeleteTransactionMetadata(ctx, testServer.GetValue(), operations.V2DeleteTransactionMetadataRequest{
					Ledger: createLedgerRequest.Ledger,
					ID:     firstTX.ID,
					Key:    "foo",
				})).To(BeNil())

				Expect(DeleteAccountMetadata(ctx, testServer.GetValue(), operations.V2DeleteAccountMetadataRequest{
					Ledger:  createLedgerRequest.Ledger,
					Address: "world",
					Key:     "foo",
				})).To(BeNil())

				_, err = RevertTransaction(ctx, testServer.GetValue(), operations.V2RevertTransactionRequest{
					Ledger: createLedgerRequest.Ledger,
					ID:     firstTX.ID,
				})
				Expect(err).To(BeNil())
			})
			When("exporting the logs", func() {
				var (
					reader io.Reader
					err    error
				)
				JustBeforeEach(func() {
					reader, err = Export(ctx, testServer.GetValue(), operations.V2ExportLogsRequest{
						Ledger: createLedgerRequest.Ledger,
					})
					Expect(err).To(BeNil())
				})
				It("should be ok", func() {})
				When("then create a new ledger", func() {
					var ledgerCopyName string
					JustBeforeEach(func() {
						ledgerCopyName = createLedgerRequest.Ledger + "-copy"
						err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
							Ledger: ledgerCopyName,
							V2CreateLedgerRequest: &components.V2CreateLedgerRequest{
								Features: features.MinimalFeatureSet,
							},
						})
						Expect(err).To(BeNil())
					})

					importLogs := func() error {
						GinkgoHelper()

						data, err := io.ReadAll(reader)
						Expect(err).To(BeNil())

						return Import(ctx, testServer.GetValue(), operations.V2ImportLogsRequest{
							Ledger:      ledgerCopyName,
							RequestBody: pointer.For(string(data)),
						})
					}

					When("importing data", func() {
						It("should be ok", func() {
							Expect(importLogs()).To(Succeed())

							logsFromOriginalLedger, err := ListLogs(ctx, testServer.GetValue(), operations.V2ListLogsRequest{
								Ledger: createLedgerRequest.Ledger,
							})
							Expect(err).To(Succeed())

							logsFromNewLedger, err := ListLogs(ctx, testServer.GetValue(), operations.V2ListLogsRequest{
								Ledger: ledgerCopyName,
							})
							Expect(err).To(Succeed())

							Expect(logsFromOriginalLedger.Data).To(Equal(logsFromNewLedger.Data))

							transactionsFromOriginalLedger, err := ListTransactions(ctx, testServer.GetValue(), operations.V2ListTransactionsRequest{
								Ledger: createLedgerRequest.Ledger,
							})
							Expect(err).To(Succeed())

							transactionsFromNewLedger, err := ListTransactions(ctx, testServer.GetValue(), operations.V2ListTransactionsRequest{
								Ledger: ledgerCopyName,
							})
							Expect(err).To(Succeed())

							Expect(transactionsFromOriginalLedger.Data).To(Equal(transactionsFromNewLedger.Data))

							accountsFromOriginalLedger, err := ListAccounts(ctx, testServer.GetValue(), operations.V2ListAccountsRequest{
								Ledger: createLedgerRequest.Ledger,
							})
							Expect(err).To(Succeed())

							accountsFromNewLedger, err := ListAccounts(ctx, testServer.GetValue(), operations.V2ListAccountsRequest{
								Ledger: ledgerCopyName,
							})
							Expect(err).To(Succeed())

							Expect(accountsFromOriginalLedger.Data).To(Equal(accountsFromNewLedger.Data))
						})
					})
					Context("with state to 'in-use'", func() {
						JustBeforeEach(func() {
							_, err := CreateTransaction(ctx, testServer.GetValue(), operations.V2CreateTransactionRequest{
								Ledger: ledgerCopyName,
								V2PostTransaction: components.V2PostTransaction{
									Postings: []components.V2Posting{{
										Source:      "world",
										Destination: "dst",
										Asset:       "USD",
										Amount:      big.NewInt(100),
									}},
								},
							})
							Expect(err).To(BeNil())
						})
						When("importing data", func() {
							It("Should fail with IMPORT code", func() {
								err := importLogs()
								Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumImport)))
							})
						})
					})
					Context("with concurrent transaction creation", func() {
						var (
							sqlTx         bun.Tx
							importErrChan chan error
							db            *bun.DB
						)
						// the import process is relying on the ledger state
						// it the ledger already has some logs, it is considered as in use and import must fails.
						// as the sdk does not allow to control the stream passed to the Import function
						// we take a lock on the ledgers table to force the process to wait
						// while we will make a concurrent request
						JustBeforeEach(func() {
							db = ConnectToDatabase(GinkgoT(), testServer.GetValue())
							sqlTx, err = db.BeginTx(ctx, &sql.TxOptions{})
							Expect(err).To(BeNil())

							DeferCleanup(func() {
								_ = sqlTx.Rollback()
							})

							_, err := sqlTx.NewRaw("lock table _default.logs").Exec(ctx)
							Expect(err).To(BeNil())

							go func() {
								defer GinkgoRecover()

								// should block
								_, err := CreateTransaction(ctx, testServer.GetValue(), operations.V2CreateTransactionRequest{
									Ledger: ledgerCopyName,
									Force:  pointer.For(true),
									V2PostTransaction: components.V2PostTransaction{
										Postings: []components.V2Posting{{
											Source:      "a",
											Destination: "b",
											Asset:       "USD",
											Amount:      big.NewInt(100),
										}},
									},
								})
								Expect(err).To(BeNil())
							}()

							// check postgres locks
							// we wait for two active locks. The first is the one we took in the test.
							// the second is the one took by the call to the CreateTransaction
							// Once we have the two locks, we know that the CreateTransaction is in a sql tx.
							Eventually(func(g Gomega) int {
								count, err := db.NewSelect().
									Table("pg_stat_activity").
									Where("state <> 'idle' and pid <> pg_backend_pid()").
									Where(`query like 'lock table _default.logs%' or query like 'INSERT INTO "_default".logs%'`).
									Count(ctx)
								g.Expect(err).To(BeNil())
								return count
							}).Should(Equal(2))

							importErrChan = make(chan error, 1)
							go func() {
								defer GinkgoRecover()

								// the call on importLogs() should block too since the logs table is locked
								importErrChan <- importLogs()
							}()
						})
						It("should fail", func() {
							Expect(sqlTx.Rollback()).To(Succeed())
							Eventually(importErrChan).Should(Receive(HaveErrorCode(string(components.V2ErrorsEnumImport))))
						})
					})
				})
			})
		})
	})
})
