//go:build it

package test_suite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/time"
	"github.com/formancehq/ledger/internal/bus"
	"github.com/nats-io/nats.go"
	"io"
	"math/big"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/pointer"
	. "github.com/formancehq/go-libs/testing/api"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	ledger "github.com/formancehq/ledger/internal"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/uptrace/bun"
)

var _ = Context("Ledger integration tests", func() {
	var (
		db  = pgtesting.UsePostgresDatabase(pgServer)
		ctx = logging.TestingContext()
	)

	testServer := NewTestServer(func() Configuration {
		return Configuration{
			PostgresConfiguration: db.GetValue().ConnectionOptions(),
			Output:                GinkgoWriter,
			Debug:                 debug,
			NatsURL:               natsServer.GetValue().ClientURL(),
		}
	})
	When("starting the service", func() {
		It("should be ok", func() {
			info, err := testServer.GetValue().Client().Ledger.V2.GetInfo(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(info.V2ConfigInfoResponse.Version).To(Equal("develop"))
		})
	})
	When("creating 10 ledger", func() {
		BeforeEach(func() {
			for i := range 10 {
				err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
					Ledger: fmt.Sprintf("ledger%d", i),
				})
				Expect(err).To(BeNil())
			}
		})
		It("should be listable", func() {
			ledgers, err := ListLedgers(ctx, testServer.GetValue(), operations.V2ListLedgersRequest{})
			Expect(err).To(BeNil())
			Expect(ledgers.Data).To(HaveLen(10))
		})
	})
	When("creating a new ledger", func() {
		var (
			createLedgerRequest operations.V2CreateLedgerRequest
			err                 error
		)
		BeforeEach(func() {
			createLedgerRequest = operations.V2CreateLedgerRequest{
				Ledger: "foo",
			}
		})
		JustBeforeEach(func() {
			err = CreateLedger(ctx, testServer.GetValue(), createLedgerRequest)
		})
		It("should be ok", func() {
			Expect(err).To(BeNil())
		})
		Context("with invalid ledger name", func() {
			BeforeEach(func() {
				createLedgerRequest.Ledger = "invalid\\name\\contains\\some\\backslash"
			})
			It("should fail", func() {
				Expect(err).NotTo(BeNil())
				Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
			})
		})
		Context("with invalid bucket name", func() {
			BeforeEach(func() {
				createLedgerRequest.V2CreateLedgerRequest = &components.V2CreateLedgerRequest{
					Bucket: pointer.For("invalid\\name\\contains\\some\\backslash"),
				}
			})
			It("should fail", func() {
				Expect(err).NotTo(BeNil())
				Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
			})
		})
		When("creating a new transaction", func() {
			var (
				createTransactionRequest operations.V2CreateTransactionRequest
				tx                       *components.V2Transaction
			)
			BeforeEach(func() {
				createTransactionRequest = operations.V2CreateTransactionRequest{
					Ledger: createLedgerRequest.Ledger,
				}
			})
			JustBeforeEach(func() {
				Expect(err).To(BeNil())
				tx, err = CreateTransaction(ctx, testServer.GetValue(), createTransactionRequest)
			})
			Context("from world to bank", func() {
				BeforeEach(func() {
					createTransactionRequest.V2PostTransaction.Postings = []components.V2Posting{{
						Amount:      big.NewInt(100),
						Asset:       "USD/2",
						Destination: "bank",
						Source:      "world",
					}}
				})
				checkTx := func() {
					GinkgoHelper()
					Expect(tx.ID).To(Equal(big.NewInt(1)))
					Expect(tx.Postings).To(Equal(createTransactionRequest.V2PostTransaction.Postings))
					Expect(tx.Timestamp).NotTo(BeZero())
					Expect(tx.Metadata).NotTo(BeNil())
					Expect(tx.Reverted).To(BeFalse())
					Expect(tx.Reference).To(BeNil())
				}
				It("should be ok", func() {
					Expect(err).To(BeNil())
					checkTx()
				})
				It("should be ok", func() {
					Expect(err).To(BeNil())
					checkTx()
				})
				Context("when listening on event", func() {
					var msgs chan *nats.Msg
					BeforeEach(func() {
						msgs = testServer.GetValue().Subscribe()
					})
					It("should receive an event", func() {
						Eventually(msgs).Should(Receive(Event(bus.CommittedTransactions{
							Ledger: "foo",
							Transactions: []ledger.Transaction{{
								TransactionData: ledger.TransactionData{
									Postings: []ledger.Posting{
										ledger.NewPosting("world", "bank", "USD/2", big.NewInt(100)),
									},
									Timestamp:  time.New(tx.Timestamp),
									InsertedAt: time.New(tx.Timestamp),
									Metadata:   metadata.Metadata{},
								},
								ID: 1,
								PostCommitVolumes: ledger.PostCommitVolumes{
									"world": {
										"USD/2": ledger.NewVolumesInt64(0, 100),
									},
									"bank": {
										"USD/2": ledger.NewVolumesInt64(100, 0),
									},
								},
								PostCommitEffectiveVolumes: ledger.PostCommitVolumes{
									"world": {
										"USD/2": ledger.NewVolumesInt64(0, 100),
									},
									"bank": {
										"USD/2": ledger.NewVolumesInt64(100, 0),
									},
								},
							}},
							AccountMetadata: ledger.AccountMetadata{},
						})))
					})
				})
				It("should be listable on api", func() {
					txs, err := ListTransactions(ctx, testServer.GetValue(), operations.V2ListTransactionsRequest{
						Ledger: createLedgerRequest.Ledger,
					})
					Expect(err).To(BeNil())
					Expect(txs.Data).To(HaveLen(1))
				})
				Context("with some metadata", func() {
					BeforeEach(func() {
						createTransactionRequest.V2PostTransaction.Metadata = map[string]string{
							"foo": "bar",
						}
					})
					It("should be ok and metadata should be registered", func() {
						Expect(err).To(BeNil())
						Expect(tx.Metadata).To(HaveKeyWithValue("foo", "bar"))

						transactionFromAPI, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
							Ledger: createLedgerRequest.Ledger,
							ID:     tx.ID,
						})
						Expect(err).To(BeNil())
						Expect(&components.V2Transaction{
							Timestamp: transactionFromAPI.Timestamp,
							Postings:  transactionFromAPI.Postings,
							Reference: transactionFromAPI.Reference,
							Metadata:  transactionFromAPI.Metadata,
							ID:        transactionFromAPI.ID,
							Reverted:  transactionFromAPI.Reverted,
						}).To(Equal(tx))
					})
				})
				When("trying to import on the ledger", func() {
					It("should fail", func() {
						data, err := json.Marshal(ledger.NewTransactionLog(ledger.NewTransaction(), ledger.AccountMetadata{}))
						Expect(err).To(BeNil())

						err = Import(ctx, testServer.GetValue(), operations.V2ImportLogsRequest{
							Ledger:      createLedgerRequest.Ledger,
							RequestBody: pointer.For(string(data)),
						})
						Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumImport)))
					})
				})
				Context("with an IK", func() {
					BeforeEach(func() {
						createTransactionRequest.IdempotencyKey = pointer.For("ik")
					})
					It("should be ok", func() {
						Expect(err).To(BeNil())
					})
					When("trying to commit the same transaction", func() {
						var (
							newTx *components.V2Transaction
						)
						JustBeforeEach(func() {
							newTx, err = CreateTransaction(ctx, testServer.GetValue(), createTransactionRequest)
							Expect(err).To(BeNil())
						})
						It("should respond with the same tx as previously", func() {
							Expect(newTx).To(Equal(tx))
						})
					})
				})
				When("adding a metadata on the transaction", func() {
					JustBeforeEach(func() {
						Expect(AddMetadataToTransaction(ctx, testServer.GetValue(), operations.V2AddMetadataOnTransactionRequest{
							Ledger: createLedgerRequest.Ledger,
							ID:     tx.ID,
							RequestBody: map[string]string{
								"foo": "bar",
							},
						})).To(BeNil())
					})
					It("should be ok", func() {
						transaction, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
							Ledger: createLedgerRequest.Ledger,
							ID:     tx.ID,
						})
						Expect(err).To(Succeed())
						Expect(transaction.Metadata).To(HaveKeyWithValue("foo", "bar"))
					})
					When("deleting metadata", func() {
						JustBeforeEach(func() {
							Expect(DeleteTransactionMetadata(ctx, testServer.GetValue(), operations.V2DeleteTransactionMetadataRequest{
								Ledger: createLedgerRequest.Ledger,
								ID:     tx.ID,
								Key:    "foo",
							}))
						})
						It("should be ok", func() {
							transaction, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
								Ledger: createLedgerRequest.Ledger,
								ID:     tx.ID,
							})
							Expect(err).To(Succeed())
							Expect(transaction.Metadata).NotTo(HaveKey("foo"))
						})
					})
				})
				When("using dryRun parameter", func() {
					BeforeEach(func() {
						createTransactionRequest.DryRun = pointer.For(true)
					})
					It("should respond but not create the transaction on the database", func() {
						checkTx()
						_, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
							Ledger: createLedgerRequest.Ledger,
							ID:     tx.ID,
						})
						Expect(err).NotTo(BeNil())
						Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumNotFound)))
					})
				})
				When("reverting it ", func() {
					var (
						revertTransactionRequest operations.V2RevertTransactionRequest
						reversedTx               *components.V2Transaction
					)
					BeforeEach(func() {
						revertTransactionRequest = operations.V2RevertTransactionRequest{
							Ledger: createLedgerRequest.Ledger,
						}
					})
					JustBeforeEach(func() {
						revertTransactionRequest.ID = tx.ID
						reversedTx, err = RevertTransaction(ctx, testServer.GetValue(), revertTransactionRequest)
						Expect(err).To(BeNil())
					})
					It("should be ok", func() {
						By("the created transaction should have the postings reversed")
						Expect(reversedTx.Postings).To(Equal([]components.V2Posting{{
							Amount:      big.NewInt(100),
							Asset:       "USD/2",
							Destination: "world",
							Source:      "bank",
						}}))
						Expect(reversedTx.ID).To(Equal(big.NewInt(2)))
						Expect(reversedTx.Metadata).NotTo(BeNil())
						Expect(reversedTx.Reverted).To(BeFalse())
						Expect(reversedTx.Reference).To(BeNil())
						Expect(reversedTx.Timestamp.Compare(tx.Timestamp)).To(BeNumerically(">", 0))

						By("the original transaction should be marked as reverted")
						tx, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
							Ledger: createLedgerRequest.Ledger,
							ID:     tx.ID,
						})
						Expect(err).To(BeNil())
						Expect(tx.Reverted).To(BeTrue())
					})
					When("trying to revert again", func() {
						It("should fail", func() {
							reversedTx, err = RevertTransaction(ctx, testServer.GetValue(), revertTransactionRequest)
							Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumAlreadyRevert)))
						})
					})
					When("using atEffectiveDate param", func() {
						BeforeEach(func() {
							revertTransactionRequest.AtEffectiveDate = pointer.For(true)
						})
						It("Should revert the transaction at the same date as the original tx", func() {
							Expect(err).To(BeNil())
							Expect(reversedTx.Timestamp).To(Equal(tx.Timestamp))
						})
					})
					When("using dryRun param", func() {
						BeforeEach(func() {
							revertTransactionRequest.DryRun = pointer.For(true)
						})
						It("should respond but not create the database", func() {
							Expect(err).To(BeNil())
							Expect(reversedTx.ID).To(Equal(big.NewInt(2)))
							Expect(reversedTx.Metadata).NotTo(BeNil())
							Expect(reversedTx.Reverted).To(BeFalse())
							Expect(reversedTx.Reference).To(BeNil())
							Expect(reversedTx.Timestamp.Compare(tx.Timestamp)).To(BeNumerically(">", 0))

							By("the original transaction should not be marked as reverted")
							tx, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
								Ledger: createLedgerRequest.Ledger,
								ID:     tx.ID,
							})
							Expect(err).To(BeNil())
							Expect(tx.Reverted).To(BeFalse())
						})
					})
				})
				When("transferring funds to another account", func() {
					JustBeforeEach(func() {
						_, err := CreateTransaction(ctx, testServer.GetValue(), operations.V2CreateTransactionRequest{
							Ledger: createLedgerRequest.Ledger,
							V2PostTransaction: components.V2PostTransaction{
								Postings: []components.V2Posting{{
									Amount:      tx.Postings[0].Amount,
									Asset:       tx.Postings[0].Asset,
									Destination: "discard",
									Source:      tx.Postings[0].Destination,
								}},
							},
						})
						Expect(err).To(BeNil())
					})
					When("trying to revert the first tx", func() {
						var (
							revertTransactionRequest operations.V2RevertTransactionRequest
						)
						BeforeEach(func() {
							revertTransactionRequest = operations.V2RevertTransactionRequest{
								Ledger: createLedgerRequest.Ledger,
							}
						})
						JustBeforeEach(func() {
							revertTransactionRequest.ID = tx.ID
							_, err = RevertTransaction(ctx, testServer.GetValue(), revertTransactionRequest)
						})
						It("should fail with insufficient funds error", func() {
							Expect(err).NotTo(BeNil())
							Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumInsufficientFund)))
						})
						When("using force query param", func() {
							BeforeEach(func() {
								revertTransactionRequest.Force = pointer.For(true)
							})
							It("should revert the transaction even if the account does not have funds", func() {
								Expect(err).To(BeNil())
							})
						})
					})
				})
			})
			Context("from bank to user:1 with not enough funds", func() {
				BeforeEach(func() {
					createTransactionRequest.V2PostTransaction.Postings = []components.V2Posting{
						{
							Amount:      big.NewInt(100),
							Asset:       "USD/2",
							Destination: "user:1",
							Source:      "bank",
						},
					}
				})
				It("should fail", func() {
					Expect(err).NotTo(BeNil())
					Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumInsufficientFund)))
				})
			})
			Context("from world to bank with negative amount", func() {
				BeforeEach(func() {
					createTransactionRequest.V2PostTransaction.Postings = []components.V2Posting{{
						Amount:      big.NewInt(-100),
						Asset:       "USD/2",
						Destination: "user:1",
						Source:      "bank",
					}}
				})
				It("should fail", func() {
					Expect(err).NotTo(BeNil())
					Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumCompilationFailed)))
				})
			})
			Context("with invalid numscript script", func() {
				BeforeEach(func() {
					createTransactionRequest.V2PostTransaction.Script = &components.V2PostTransactionScript{
						Plain: `send [COIN XXX] (
							source = @world
							destination = @bob
						)`,
					}
				})
				It("should fail", func() {
					Expect(err).NotTo(BeNil())
					Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumCompilationFailed)))
				})
			})
			Context("with valid numscript script", func() {
				BeforeEach(func() {
					createTransactionRequest.V2PostTransaction.Script = &components.V2PostTransactionScript{
						Plain: `send [COIN 100] (
							source = @world
							destination = @bob
						)
						set_account_meta(@world, "foo", "bar")
						`,
					}
				})
				JustBeforeEach(func() {
					Expect(err).To(BeNil())
				})
				It("should be ok", func() {
					account, err := GetAccount(ctx, testServer.GetValue(), operations.V2GetAccountRequest{
						Ledger:  createLedgerRequest.Ledger,
						Address: "world",
					})
					Expect(err).To(BeNil())
					Expect(account.Metadata).To(HaveKeyWithValue("foo", "bar"))
				})
			})
		})
		When("adding some metadata on 'world' account", func() {
			JustBeforeEach(func() {
				Expect(err).To(BeNil())
				Expect(AddMetadataToAccount(ctx, testServer.GetValue(), operations.V2AddMetadataToAccountRequest{
					Ledger:  createLedgerRequest.Ledger,
					Address: "world",
					RequestBody: map[string]string{
						"foo": "bar",
					},
				})).To(BeNil())
			})
			It("should be ok", func() {
				account, err := GetAccount(ctx, testServer.GetValue(), operations.V2GetAccountRequest{
					Ledger:  createLedgerRequest.Ledger,
					Address: "world",
				})
				Expect(err).To(Succeed())
				Expect(account.Metadata).To(HaveKeyWithValue("foo", "bar"))
			})
			When("deleting metadata", func() {
				JustBeforeEach(func() {
					Expect(DeleteAccountMetadata(ctx, testServer.GetValue(), operations.V2DeleteAccountMetadataRequest{
						Ledger:  createLedgerRequest.Ledger,
						Address: "world",
						Key:     "foo",
					}))
				})
				It("should be ok", func() {
					account, err := GetAccount(ctx, testServer.GetValue(), operations.V2GetAccountRequest{
						Ledger:  createLedgerRequest.Ledger,
						Address: "world",
					})
					Expect(err).To(Succeed())
					Expect(account.Metadata).NotTo(HaveKey("foo"))
				})
			})
		})
		Context("with a set of all possible actions", func() {
			JustBeforeEach(func() {
				Expect(err).To(BeNil())
				tx, err := CreateTransaction(ctx, testServer.GetValue(), operations.V2CreateTransactionRequest{
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
					ID:     tx.ID,
					RequestBody: map[string]string{
						"foo": "bar",
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
					ID:     tx.ID,
					Key:    "foo",
				})).To(BeNil())

				Expect(DeleteAccountMetadata(ctx, testServer.GetValue(), operations.V2DeleteAccountMetadataRequest{
					Ledger:  createLedgerRequest.Ledger,
					Address: "world",
					Key:     "foo",
				})).To(BeNil())

				_, err = RevertTransaction(ctx, testServer.GetValue(), operations.V2RevertTransactionRequest{
					Ledger: createLedgerRequest.Ledger,
					ID:     tx.ID,
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
								Features: ledger.MinimalFeatureSet,
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
							db = testServer.GetValue().Database()
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
		When("trying to revert a not existing transaction", func() {
			It("should fail", func() {
				_, err := RevertTransaction(ctx, testServer.GetValue(), operations.V2RevertTransactionRequest{
					Ledger: createLedgerRequest.Ledger,
					ID:     big.NewInt(10),
				})
				Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumNotFound)))
			})
		})
	})
})
