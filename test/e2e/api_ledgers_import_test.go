//go:build it

package test_suite

import (
	"database/sql"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/api"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/features"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/uptrace/bun"
	"math/big"
)

var _ = Context("Ledger engine tests", func() {
	var (
		db                = UseTemplatedDatabase()
		ctx               = logging.TestingContext()
		connectionOptions = DeferMap(db, (*pgtesting.Database).ConnectionOptions)
	)

	testServer := DeferTestServer(
		connectionOptions,
		testservice.WithInstruments(
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
			ExperimentalFeaturesInstrumentation(),
		),
		testservice.WithLogger(GinkgoT()),
	)

	When("creating a new ledger", func() {
		var (
			createLedgerRequest operations.V2CreateLedgerRequest
			err                 error
		)
		BeforeEach(func() {
			createLedgerRequest = operations.V2CreateLedgerRequest{
				Ledger: "foo",
				V2CreateLedgerRequest: components.V2CreateLedgerRequest{
					Features: features.MinimalFeatureSet,
				},
			}
		})
		JustBeforeEach(func(specContext SpecContext) {
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, createLedgerRequest)
		})
		When("importing data in two steps", func() {
			It("should be ok", func(specContext SpecContext) {
				firstBatch := `{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"world","destination":"payments:1234","amount":10000,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:07:41.522336Z","id":0,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:07:41.534898Z","idempotencyKey":"","id":0,"hash":"g489GFReBqquboEjkB95X3OU6mheMzgiu63PdSTfMuM="}
{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"payments:1234","destination":"platform","amount":1500,"asset":"EUR/2"},{"source":"payments:1234","destination":"merchants:777","amount":8500,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:07:55.145802Z","id":1,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:07:55.170731Z","idempotencyKey":"","id":1,"hash":"T+2SGiCeC8tagt1tf5E/L7r98wB8tm6EbNd+OJ7ZvCI="}`

				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.ImportLogs(ctx, operations.V2ImportLogsRequest{
					Ledger:              createLedgerRequest.Ledger,
					V2ImportLogsRequest: []byte(firstBatch),
				})
				Expect(err).To(BeNil())

				secondBatch := `{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"merchants:777","destination":"payouts:987","amount":8500,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:08:24.955784Z","id":2,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:08:24.985834Z","idempotencyKey":"","id":2,"hash":"WgOIXsh8x0pGSi//jHjQ78RF9YnFRslsbp2aOHiG43U="}
{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"platform","destination":"refunds:4567","amount":5000,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:08:39.301709Z","id":3,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:08:39.330919Z","idempotencyKey":"","id":3,"hash":"JblhzL91s+DTcd53YTV2laC4QBRe5oDDoz9CzsX5Pro="}
{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"refunds:4567","destination":"world","amount":5000,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:11:02.413499Z","id":4,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:11:02.434078Z","idempotencyKey":"","id":4,"hash":"Y8TBz5GhxTWW9D/wRXHPcIlrYFPQjroiIBWX1q6SJJo="}`

				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.ImportLogs(ctx, operations.V2ImportLogsRequest{
					Ledger:              createLedgerRequest.Ledger,
					V2ImportLogsRequest: []byte(secondBatch),
				})
				Expect(err).To(BeNil())

				logsFromOriginalLedger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(ctx, operations.V2ListLogsRequest{
					Ledger: createLedgerRequest.Ledger,
				})
				Expect(err).To(Succeed())
				Expect(logsFromOriginalLedger.V2LogsCursorResponse.Cursor.Data).To(HaveLen(5))
			})
		})
		When("importing data from 2.1", func() {
			importLogs := func(specContext SpecContext) error {
				GinkgoHelper()

				logs := `{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"world","destination":"payments:1234","amount":10000,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:07:41.522336Z","id":0,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:07:41.534898Z","idempotencyKey":"","id":0,"hash":"g489GFReBqquboEjkB95X3OU6mheMzgiu63PdSTfMuM="}
{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"payments:1234","destination":"platform","amount":1500,"asset":"EUR/2"},{"source":"payments:1234","destination":"merchants:777","amount":8500,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:07:55.145802Z","id":1,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:07:55.170731Z","idempotencyKey":"","id":1,"hash":"T+2SGiCeC8tagt1tf5E/L7r98wB8tm6EbNd+OJ7ZvCI="}
{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"merchants:777","destination":"payouts:987","amount":8500,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:08:24.955784Z","id":2,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:08:24.985834Z","idempotencyKey":"","id":2,"hash":"WgOIXsh8x0pGSi//jHjQ78RF9YnFRslsbp2aOHiG43U="}
{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"platform","destination":"refunds:4567","amount":5000,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:08:39.301709Z","id":3,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:08:39.330919Z","idempotencyKey":"","id":3,"hash":"JblhzL91s+DTcd53YTV2laC4QBRe5oDDoz9CzsX5Pro="}
{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"refunds:4567","destination":"world","amount":5000,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:11:02.413499Z","id":4,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:11:02.434078Z","idempotencyKey":"","id":4,"hash":"Y8TBz5GhxTWW9D/wRXHPcIlrYFPQjroiIBWX1q6SJJo="}`

				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ImportLogs(ctx, operations.V2ImportLogsRequest{
					Ledger:              createLedgerRequest.Ledger,
					V2ImportLogsRequest: []byte(logs),
				})
				return err
			}

			It("should be ok", func(specContext SpecContext) {
				Expect(importLogs(specContext)).To(Succeed())

				logsFromOriginalLedger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(ctx, operations.V2ListLogsRequest{
					Ledger: createLedgerRequest.Ledger,
				})
				Expect(err).To(Succeed())

				logsFromNewLedger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(ctx, operations.V2ListLogsRequest{
					Ledger: createLedgerRequest.Ledger,
				})
				Expect(err).To(Succeed())

				Expect(logsFromOriginalLedger.V2LogsCursorResponse.Cursor.Data).To(Equal(logsFromNewLedger.V2LogsCursorResponse.Cursor.Data))

				transactionsFromOriginalLedger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
					Ledger: createLedgerRequest.Ledger,
				})
				Expect(err).To(Succeed())

				transactionsFromNewLedger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
					Ledger: createLedgerRequest.Ledger,
				})
				Expect(err).To(Succeed())

				Expect(transactionsFromOriginalLedger.V2TransactionsCursorResponse.Cursor.Data).To(Equal(transactionsFromNewLedger.V2TransactionsCursorResponse.Cursor.Data))

				accountsFromOriginalLedger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
					Ledger: createLedgerRequest.Ledger,
				})
				Expect(err).To(Succeed())

				accountsFromNewLedger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
					Ledger: createLedgerRequest.Ledger,
				})
				Expect(err).To(Succeed())

				Expect(accountsFromOriginalLedger.V2AccountsCursorResponse.Cursor.Data).To(Equal(accountsFromNewLedger.V2AccountsCursorResponse.Cursor.Data))
			})
		})
		When("importing data with errors", func() {
			JustBeforeEach(func(specContext SpecContext) {
				// Third log as an invalid id (== 0)
				logs := `{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"world","destination":"payments:1234","amount":10000,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:07:41.522336Z","id":0,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:07:41.534898Z","idempotencyKey":"","id":0,"hash":"g489GFReBqquboEjkB95X3OU6mheMzgiu63PdSTfMuM="}
{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"payments:1234","destination":"platform","amount":1500,"asset":"EUR/2"},{"source":"payments:1234","destination":"merchants:777","amount":8500,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:07:55.145802Z","id":1,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:07:55.170731Z","idempotencyKey":"","id":1,"hash":"T+2SGiCeC8tagt1tf5E/L7r98wB8tm6EbNd+OJ7ZvCI="}
{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"merchants:777","destination":"payouts:987","amount":8500,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:08:24.955784Z","id":2,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:08:24.985834Z","idempotencyKey":"","id":0,"hash":"WgOIXsh8x0pGSi//jHjQ78RF9YnFRslsbp2aOHiG43U="}
{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"platform","destination":"refunds:4567","amount":5000,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:08:39.301709Z","id":3,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:08:39.330919Z","idempotencyKey":"","id":3,"hash":"JblhzL91s+DTcd53YTV2laC4QBRe5oDDoz9CzsX5Pro="}
{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"refunds:4567","destination":"world","amount":5000,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:11:02.413499Z","id":4,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:11:02.434078Z","idempotencyKey":"","id":4,"hash":"Y8TBz5GhxTWW9D/wRXHPcIlrYFPQjroiIBWX1q6SJJo="}`

				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ImportLogs(ctx, operations.V2ImportLogsRequest{
					Ledger:              createLedgerRequest.Ledger,
					V2ImportLogsRequest: []byte(logs),
				})
				Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumImport)))
			})
			It("should fail but should insert first logs", func(specContext SpecContext) {
				list, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(ctx, operations.V2ListLogsRequest{
					Ledger: createLedgerRequest.Ledger,
				})
				Expect(err).To(Succeed())
				Expect(list.V2LogsCursorResponse.Cursor.Data).To(HaveLen(2))
			})
			Context("then when resuming with correct logs", func() {
				It("Should be ok", func(specContext SpecContext) {
					// restart from the failed log
					logs := `{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"merchants:777","destination":"payouts:987","amount":8500,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:08:24.955784Z","id":2,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:08:24.985834Z","idempotencyKey":"","id":2,"hash":"WgOIXsh8x0pGSi//jHjQ78RF9YnFRslsbp2aOHiG43U="}
{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"platform","destination":"refunds:4567","amount":5000,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:08:39.301709Z","id":3,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:08:39.330919Z","idempotencyKey":"","id":3,"hash":"JblhzL91s+DTcd53YTV2laC4QBRe5oDDoz9CzsX5Pro="}
{"type":"NEW_TRANSACTION","data":{"transaction":{"postings":[{"source":"refunds:4567","destination":"world","amount":5000,"asset":"EUR/2"}],"metadata":{},"timestamp":"2025-02-17T12:11:02.413499Z","id":4,"reverted":false},"accountMetadata":{}},"date":"2025-02-17T12:11:02.434078Z","idempotencyKey":"","id":4,"hash":"Y8TBz5GhxTWW9D/wRXHPcIlrYFPQjroiIBWX1q6SJJo="}`

					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ImportLogs(ctx, operations.V2ImportLogsRequest{
						Ledger:              createLedgerRequest.Ledger,
						V2ImportLogsRequest: []byte(logs),
					})
					Expect(err).To(Succeed())
				})
			})
		})
		Context("with a set of all possible actions", func() {
			JustBeforeEach(func(specContext SpecContext) {
				Expect(err).To(BeNil())

				firstTX, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
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
				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
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

				thirdTx, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
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

				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(ctx, operations.V2AddMetadataOnTransactionRequest{
					Ledger: createLedgerRequest.Ledger,
					ID:     firstTX.V2CreateTransactionResponse.Data.ID,
					RequestBody: map[string]string{
						"foo": "bar",
					},
				})
				Expect(err).To(BeNil())

				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(ctx, operations.V2AddMetadataOnTransactionRequest{
					Ledger: createLedgerRequest.Ledger,
					ID:     thirdTx.V2CreateTransactionResponse.Data.ID,
					RequestBody: map[string]string{
						"foo": "baz",
					},
				})
				Expect(err).To(BeNil())

				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(ctx, operations.V2AddMetadataToAccountRequest{
					Ledger:  createLedgerRequest.Ledger,
					Address: "bank",
					RequestBody: map[string]string{
						"foo": "bar",
					},
				})
				Expect(err).To(BeNil())

				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteTransactionMetadata(ctx, operations.V2DeleteTransactionMetadataRequest{
					Ledger: createLedgerRequest.Ledger,
					ID:     firstTX.V2CreateTransactionResponse.Data.ID,
					Key:    "foo",
				})
				Expect(err).To(BeNil())

				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteAccountMetadata(ctx, operations.V2DeleteAccountMetadataRequest{
					Ledger:  createLedgerRequest.Ledger,
					Address: "world",
					Key:     "foo",
				})
				Expect(err).To(BeNil())

				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.RevertTransaction(ctx, operations.V2RevertTransactionRequest{
					Ledger: createLedgerRequest.Ledger,
					ID:     firstTX.V2CreateTransactionResponse.Data.ID,
				})
				Expect(err).To(BeNil())
			})
			When("exporting the logs", func() {
				var (
					response *operations.V2ExportLogsResponse
					err      error
				)
				JustBeforeEach(func(specContext SpecContext) {
					response, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.ExportLogs(ctx, operations.V2ExportLogsRequest{
						Ledger: createLedgerRequest.Ledger,
					})
					Expect(err).To(BeNil())
				})
				It("should be ok", func() {})
				When("then create a new ledger", func() {
					var ledgerCopyName string
					JustBeforeEach(func(specContext SpecContext) {
						ledgerCopyName = createLedgerRequest.Ledger + "-copy"
						_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
							Ledger: ledgerCopyName,
							V2CreateLedgerRequest: components.V2CreateLedgerRequest{
								Features: features.MinimalFeatureSet,
							},
						})
						Expect(err).To(BeNil())
					})

					importLogs := func(specContext SpecContext) error {
						GinkgoHelper()

						_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ImportLogs(ctx, operations.V2ImportLogsRequest{
							Ledger:              ledgerCopyName,
							V2ImportLogsRequest: response.HTTPMeta.Response.Body,
						})
						return err
					}

					When("importing data", func() {
						It("should be ok", func(specContext SpecContext) {
							Expect(importLogs(specContext)).To(Succeed())

							logsFromOriginalLedger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(ctx, operations.V2ListLogsRequest{
								Ledger: createLedgerRequest.Ledger,
							})
							Expect(err).To(Succeed())

							logsFromNewLedger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLogs(ctx, operations.V2ListLogsRequest{
								Ledger: ledgerCopyName,
							})
							Expect(err).To(Succeed())

							Expect(logsFromOriginalLedger.V2LogsCursorResponse.Cursor.Data).To(Equal(logsFromNewLedger.V2LogsCursorResponse.Cursor.Data))

							transactionsFromOriginalLedger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
								Ledger: createLedgerRequest.Ledger,
							})
							Expect(err).To(Succeed())

							transactionsFromNewLedger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
								Ledger: ledgerCopyName,
							})
							Expect(err).To(Succeed())

							Expect(transactionsFromOriginalLedger.V2TransactionsCursorResponse.Cursor.Data).To(Equal(transactionsFromNewLedger.V2TransactionsCursorResponse.Cursor.Data))

							accountsFromOriginalLedger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
								Ledger: createLedgerRequest.Ledger,
							})
							Expect(err).To(Succeed())

							accountsFromNewLedger, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
								Ledger: ledgerCopyName,
							})
							Expect(err).To(Succeed())

							Expect(accountsFromOriginalLedger.V2AccountsCursorResponse.Cursor.Data).To(Equal(accountsFromNewLedger.V2AccountsCursorResponse.Cursor.Data))
						})
					})
					Context("with state to 'in-use'", func() {
						JustBeforeEach(func(specContext SpecContext) {
							_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
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
							It("Should fail with IMPORT code", func(specContext SpecContext) {
								err := importLogs(specContext)
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
						// as the sdk does not allow to control the stream passed to the ImportLogs function
						// we take a lock on the ledgers table to force the process to wait
						// while we will make a concurrent request
						JustBeforeEach(func(specContext SpecContext) {
							db = ConnectToDatabase(ctx, connectionOptions)
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
								_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
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

							// At this point, and since the ledger is in 'initializing' state,
							// the transaction creation should have taken an advisory lock
							Eventually(func(g Gomega) int {
								count, err := db.NewSelect().
									Table("pg_locks").
									Where("locktype = 'advisory'").
									Count(ctx)
								g.Expect(err).To(BeNil())
								return count
							}).Should(Equal(1))

							// check postgres locks
							// since we have locked the 'logs' table, the insertion of the log must block
							Eventually(func(g Gomega) int {
								count, err := db.NewSelect().
									Table("pg_stat_activity").
									Where("state <> 'idle' and pid <> pg_backend_pid()").
									Where(`query like 'INSERT INTO "_default".logs%'`).
									Count(ctx)
								g.Expect(err).To(BeNil())
								return count
							}).Should(Equal(1))

							importErrChan = make(chan error, 1)
							go func() {
								defer GinkgoRecover()

								// the call on ImportLogs() should block too since the logs table is locked
								importErrChan <- importLogs(specContext)
							}()

							// At this point, the import should block when trying to acquire the advisory lock taken
							// by the transaction creation parallel (and blocked) request.
							// We should have two taken advisory locks in the pg_locks table
							// One with waiting status, and one granted.
							Eventually(func(g Gomega) int {
								count, err := db.NewSelect().
									Table("pg_locks").
									Where("locktype = 'advisory'").
									Count(ctx)
								g.Expect(err).To(BeNil())
								return count
							}).Should(Equal(2))
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
