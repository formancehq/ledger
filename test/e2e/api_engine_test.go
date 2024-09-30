//go:build it

package test_suite

import (
	"encoding/json"
	"github.com/formancehq/go-libs/time"
	"github.com/formancehq/ledger/internal/bus"
	ledgerevents "github.com/formancehq/ledger/pkg/events"
	"github.com/nats-io/nats.go"
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
)

var _ = Context("Ledger engine tests", func() {
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
	var events chan *nats.Msg
	BeforeEach(func() {
		events = testServer.GetValue().Subscribe()
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
			Expect(err).To(BeNil())
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

					By("it should also send an event", func() {
						Eventually(events).Should(Receive(Event(ledgerevents.EventTypeCommittedTransactions, WithPayload(bus.CommittedTransactions{
							Ledger:          "foo",
							Transactions:    []ledger.Transaction{ConvertSDKTxToCoreTX(tx)},
							AccountMetadata: ledger.AccountMetadata{},
						}))))
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
							Expand: pointer.For("volumes,effectiveVolumes"),
						})
						Expect(err).To(BeNil())
						Expect(&components.V2Transaction{
							InsertedAt:                 transactionFromAPI.InsertedAt,
							Timestamp:                  transactionFromAPI.Timestamp,
							Postings:                   transactionFromAPI.Postings,
							Reference:                  transactionFromAPI.Reference,
							Metadata:                   transactionFromAPI.Metadata,
							ID:                         transactionFromAPI.ID,
							Reverted:                   transactionFromAPI.Reverted,
							PreCommitEffectiveVolumes:  transactionFromAPI.PreCommitEffectiveVolumes,
							PostCommitEffectiveVolumes: transactionFromAPI.PostCommitEffectiveVolumes,
							PreCommitVolumes:           transactionFromAPI.PreCommitVolumes,
							PostCommitVolumes:          transactionFromAPI.PostCommitVolumes,
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
						It("should respond with the same tx as previously minus pre commit volumes", func() {
							tx.PreCommitVolumes = nil
							tx.PostCommitVolumes = nil
							tx.PreCommitEffectiveVolumes = nil
							tx.PostCommitEffectiveVolumes = nil
							Expect(newTx).To(Equal(tx))
						})
					})
				})
				When("adding a metadata on the transaction", func() {
					metadata := map[string]string{
						"foo": "bar",
					}
					JustBeforeEach(func() {
						Expect(AddMetadataToTransaction(ctx, testServer.GetValue(), operations.V2AddMetadataOnTransactionRequest{
							Ledger:      createLedgerRequest.Ledger,
							ID:          tx.ID,
							RequestBody: metadata,
						})).To(BeNil())
					})
					It("should be ok", func() {
						transaction, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
							Ledger: createLedgerRequest.Ledger,
							ID:     tx.ID,
						})
						Expect(err).To(Succeed())
						Expect(transaction.Metadata).To(HaveKeyWithValue("foo", "bar"))

						By("it should send an event", func() {
							Eventually(events).Should(Receive(Event(ledgerevents.EventTypeSavedMetadata, WithPayload(bus.SavedMetadata{
								Ledger:     createLedgerRequest.Ledger,
								TargetType: ledger.MetaTargetTypeTransaction,
								TargetID:   tx.ID.String(),
								Metadata:   metadata,
							}))))
						})
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

							By("it should send an event", func() {
								Eventually(events).Should(Receive(Event(ledgerevents.EventTypeDeletedMetadata, WithPayload(bus.DeletedMetadata{
									Ledger:     createLedgerRequest.Ledger,
									TargetType: ledger.MetaTargetTypeTransaction,
									TargetID:   tx.ID.String(),
									Key:        "foo",
								}))))
							})
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

						By("the original transaction should be marked as reverted", func() {
							tx, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
								Ledger: createLedgerRequest.Ledger,
								ID:     tx.ID,
							})
							Expect(err).To(BeNil())
							Expect(tx.Reverted).To(BeTrue())
						})

						By("it should send an event", func() {
							Eventually(events).Should(Receive(Event(ledgerevents.EventTypeRevertedTransaction, WithPayload(bus.RevertedTransaction{
								Ledger: createLedgerRequest.Ledger,
								RevertedTransaction: ConvertSDKTxToCoreTX(tx).
									WithRevertedAt(time.New(reversedTx.Timestamp)).
									WithPostCommitEffectiveVolumes(nil),
								RevertTransaction: ConvertSDKTxToCoreTX(reversedTx),
							}))))
						})
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
