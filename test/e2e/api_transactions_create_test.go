//go:build it

package test_suite

import (
	"math/big"
	"time"

	"github.com/formancehq/go-libs/v2/logging"
	. "github.com/formancehq/go-libs/v2/testing/api"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/bus"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"

	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/pointer"
	ledgerevents "github.com/formancehq/ledger/pkg/events"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger transactions create API tests", func() {
	for _, data := range []struct {
		description      string
		numscriptRewrite bool
	}{
		{"default", false},
		{"numscript rewrite", true},
	} {
		Context(data.description, func() {
			var (
				db  = UseTemplatedDatabase()
				ctx = logging.TestingContext()
			)
			testServer := NewTestServer(func() Configuration {
				return Configuration{
					CommonConfiguration: CommonConfiguration{
						PostgresConfiguration: db.GetValue().ConnectionOptions(),
						Output:                GinkgoWriter,
						Debug:                 debug,
					},
					NatsURL:                      natsServer.GetValue().ClientURL(),
					ExperimentalNumscriptRewrite: data.numscriptRewrite,
				}
			})

			BeforeEach(func() {
				err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
					Ledger: "default",
				})
				Expect(err).To(BeNil())

				err = CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
					Ledger: "test",
				})
				Expect(err).To(BeNil())
			})

			When("creating a transaction", func() {
				var (
					events    chan *nats.Msg
					timestamp = time.Now().Round(time.Second).UTC()
					rsp       *components.V2Transaction
					req       operations.V2CreateTransactionRequest
					err       error
				)
				BeforeEach(func() {
					events = Subscribe(GinkgoT(), testServer.GetValue())
					req = operations.V2CreateTransactionRequest{
						V2PostTransaction: components.V2PostTransaction{
							Timestamp: &timestamp,
						},
						Ledger: "default",
					}
				})
				JustBeforeEach(func() {
					// Create a transaction
					rsp, err = CreateTransaction(ctx, testServer.GetValue(), req)
				})
				Context("overriding an account metadata", func() {
					BeforeEach(func() {
						err := AddMetadataToAccount(ctx, testServer.GetValue(), operations.V2AddMetadataToAccountRequest{
							Address: "alice",
							Ledger:  "default",
							RequestBody: map[string]string{
								"clientType": "gold",
							},
						})
						Expect(err).ToNot(HaveOccurred())

						req.V2PostTransaction.Script = &components.V2PostTransactionScript{
							Plain: `
								send [USD 100] (
									source = @world
									destination = @alice
								)			
								set_account_meta(@alice, "clientType", "silver")
								set_account_meta(@foo, "status", "pending")
							`,
						}
					})
					It("should override account metadata", func() {
						Expect(err).To(BeNil())

						account, err := GetAccount(ctx, testServer.GetValue(), operations.V2GetAccountRequest{
							Address: "alice",
							Ledger:  "default",
						})
						Expect(err).ToNot(HaveOccurred())
						Expect(*account).Should(Equal(components.V2Account{
							Address: "alice",
							Metadata: map[string]string{
								"clientType": "silver",
							},
						}))

						account, err = GetAccount(ctx, testServer.GetValue(), operations.V2GetAccountRequest{
							Address: "foo",
							Ledger:  "default",
						})
						Expect(err).ToNot(HaveOccurred())
						Expect(*account).Should(Equal(components.V2Account{
							Address: "foo",
							Metadata: map[string]string{
								"status": "pending",
							},
						}))
					})
				})
				Context("with account metadata", func() {
					BeforeEach(func() {
						req = operations.V2CreateTransactionRequest{
							V2PostTransaction: components.V2PostTransaction{
								Postings: []components.V2Posting{
									{
										Amount:      big.NewInt(100),
										Asset:       "USD",
										Source:      "world",
										Destination: "alice",
									},
								},
								AccountMetadata: map[string]map[string]string{
									"world": {
										"foo": "bar",
									},
									"alice": {
										"foo": "baz",
									},
								},
							},
							Ledger: "default",
						}
					})
					It("should be ok", func() {
						alice, err := GetAccount(ctx, testServer.GetValue(), operations.V2GetAccountRequest{
							Address: "alice",
							Ledger:  "default",
						})
						Expect(err).To(BeNil())
						Expect(alice.Metadata).To(Equal(map[string]string{
							"foo": "baz",
						}))

						world, err := GetAccount(ctx, testServer.GetValue(), operations.V2GetAccountRequest{
							Address: "world",
							Ledger:  "default",
						})
						Expect(err).To(BeNil())
						Expect(world.Metadata).To(Equal(map[string]string{
							"foo": "bar",
						}))
					})
				})
				Context("with valid data", func() {
					BeforeEach(func() {
						req = operations.V2CreateTransactionRequest{
							V2PostTransaction: components.V2PostTransaction{
								Metadata: map[string]string{
									"add some quotes":           "\" quoted value\"",
									"add some utf-8 characters": "½",
								},
								Postings: []components.V2Posting{
									{
										Amount:      big.NewInt(100),
										Asset:       "USD",
										Source:      "world",
										Destination: "alice",
									},
								},
								Timestamp: &timestamp,
								Reference: pointer.For("foo"),
							},
							Ledger: "default",
						}
					})
					It("should be ok", func() {
						response, err := GetTransaction(
							ctx,
							testServer.GetValue(),
							operations.V2GetTransactionRequest{
								Ledger: "default",
								ID:     rsp.ID,
								Expand: pointer.For("volumes"),
							},
						)
						Expect(err).ToNot(HaveOccurred())

						Expect(*response).To(Equal(components.V2Transaction{
							Timestamp:  rsp.Timestamp,
							InsertedAt: rsp.InsertedAt,
							Postings:   rsp.Postings,
							Reference:  rsp.Reference,
							Metadata:   rsp.Metadata,
							ID:         rsp.ID,
							PreCommitVolumes: map[string]map[string]components.V2Volume{
								"world": {
									"USD": {
										Input:   big.NewInt(0),
										Output:  big.NewInt(0),
										Balance: big.NewInt(0),
									},
								},
								"alice": {
									"USD": {
										Input:   big.NewInt(0),
										Output:  big.NewInt(0),
										Balance: big.NewInt(0),
									},
								},
							},
							PostCommitVolumes: map[string]map[string]components.V2Volume{
								"world": {
									"USD": {
										Input:   big.NewInt(0),
										Output:  big.NewInt(100),
										Balance: big.NewInt(-100),
									},
								},
								"alice": {
									"USD": {
										Input:   big.NewInt(100),
										Output:  big.NewInt(0),
										Balance: big.NewInt(100),
									},
								},
							},
						}))

						account, err := GetAccount(
							ctx,
							testServer.GetValue(),
							operations.V2GetAccountRequest{
								Address: "alice",
								Ledger:  "default",
								Expand:  pointer.For("volumes"),
							},
						)
						Expect(err).ToNot(HaveOccurred())

						Expect(*account).Should(Equal(components.V2Account{
							Address:  "alice",
							Metadata: metadata.Metadata{},
							Volumes: map[string]components.V2Volume{
								"USD": {
									Input:   big.NewInt(100),
									Output:  big.NewInt(0),
									Balance: big.NewInt(100),
								},
							},
						}))
						By("should trigger a new event", func() {
							Eventually(events).Should(Receive(Event(ledgerevents.EventTypeCommittedTransactions, WithPayload(bus.CommittedTransactions{
								Ledger:          "default",
								Transactions:    []ledger.Transaction{ConvertSDKTxToCoreTX(rsp)},
								AccountMetadata: ledger.AccountMetadata{},
							}))))
						})
					})
					When("using a reference", func() {
						BeforeEach(func() {
							req.V2PostTransaction.Reference = pointer.For("foo")
						})
						It("should be ok", func() {
							Expect(err).To(BeNil())
						})
						When("trying to commit a new transaction with the same reference", func() {
							JustBeforeEach(func() {
								_, err = CreateTransaction(ctx, testServer.GetValue(), req)
								Expect(err).To(HaveOccurred())
								Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumConflict)))
							})
							It("Should fail with "+string(components.V2ErrorsEnumConflict)+" error code", func() {})
						})
					})
				})
				When("with insufficient funds", func() {
					BeforeEach(func() {
						req.V2PostTransaction.Postings = []components.V2Posting{{
							Amount:      big.NewInt(100),
							Asset:       "USD",
							Source:      "bob",
							Destination: "alice",
						}}
					})
					It("should fail", func() {
						var expectedErr string
						if data.numscriptRewrite {
							expectedErr = string(components.V2ErrorsEnumInterpreterRuntime)
						} else {
							expectedErr = string(components.V2ErrorsEnumInsufficientFund)
						}

						Expect(err).To(HaveOccurred())
						Expect(err).To(HaveErrorCode(expectedErr))
					})
				})
				When("with nil amount", func() {
					BeforeEach(func() {
						req.V2PostTransaction.Postings = []components.V2Posting{{
							Asset:       "USD",
							Source:      "bob",
							Destination: "alice",
						}}
					})
					It("should fail", func() {
						Expect(err).To(HaveOccurred())
						Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
					})
				})
				When("with negative amount", func() {
					BeforeEach(func() {
						req.V2PostTransaction.Postings = []components.V2Posting{{
							Amount:      big.NewInt(-100),
							Asset:       "USD",
							Source:      "bob",
							Destination: "alice",
						}}
					})
					It("should fail", func() {
						Expect(err).To(HaveOccurred())
						Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
					})
				})
				When("with invalid source address", func() {
					BeforeEach(func() {
						req.V2PostTransaction.Postings = []components.V2Posting{{
							Amount:      big.NewInt(-100),
							Asset:       "USD",
							Source:      "bob;test",
							Destination: "alice",
						}}
					})
					It("should fail", func() {
						Expect(err).To(HaveOccurred())
						Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
					})
				})
				When("with invalid destination address", func() {
					BeforeEach(func() {
						req.V2PostTransaction.Postings = []components.V2Posting{{
							Amount:      big.NewInt(-100),
							Asset:       "USD",
							Source:      "bob",
							Destination: "alice;test",
						}}
					})
					It("should fail", func() {
						Expect(err).To(HaveOccurred())
						Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
					})
				})
				When("with invalid asset", func() {
					BeforeEach(func() {
						req.V2PostTransaction.Postings = []components.V2Posting{{
							Amount:      big.NewInt(-100),
							Asset:       "USD//2",
							Source:      "bob",
							Destination: "alice",
						}}
					})
					It("should fail", func() {
						Expect(err).To(HaveOccurred())
						Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
					})
				})
				When("using an idempotency key and a specific ledger", func() {
					BeforeEach(func() {
						req = operations.V2CreateTransactionRequest{
							IdempotencyKey: pointer.For("foo"),
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
								Timestamp: &timestamp,
								Reference: pointer.For("foo"),
							},
							Ledger: "default",
						}
					})
					It("should be ok", func() {
						Expect(err).To(Succeed())
						Expect(rsp.ID).To(Equal(big.NewInt(1)))
					})
					When("creating a ledger transaction with same ik and different ledger", func() {
						JustBeforeEach(func() {
							rsp, err = CreateTransaction(ctx, testServer.GetValue(), req)
						})
						It("should not have an error", func() {
							Expect(err).To(Succeed())
							Expect(rsp.ID).To(Equal(big.NewInt(1)))
						})
					})
				})
				When("using a negative amount in a script", func() {
					BeforeEach(func() {
						req = operations.V2CreateTransactionRequest{
							IdempotencyKey: pointer.For("testing"),
							V2PostTransaction: components.V2PostTransaction{
								Metadata: map[string]string{},
								Script: &components.V2PostTransactionScript{
									Plain: `send [COIN -100] (
								source = @world
								destination = @bob
							)`,
									Vars: map[string]string{},
								},
							},
							Ledger: "default",
						}
					})

					var expectedErr string
					if data.numscriptRewrite {
						expectedErr = string(components.V2ErrorsEnumInterpreterRuntime)
					} else {
						expectedErr = string(components.V2ErrorsEnumCompilationFailed)
					}

					It("should fail with "+expectedErr+" code", func() {
						Expect(err).NotTo(Succeed())
						Expect(err).To(HaveErrorCode(expectedErr))
					})
				})
				When("using a negative amount in the script with a variable", func() {
					BeforeEach(func() {
						req = operations.V2CreateTransactionRequest{
							IdempotencyKey: pointer.For("testing"),
							V2PostTransaction: components.V2PostTransaction{
								Metadata: map[string]string{},
								Script: &components.V2PostTransactionScript{
									Plain: `vars {
								monetary $amount
							}
							send $amount (
								source = @world
								destination = @bob
							)`,
									Vars: map[string]string{
										"amount": "USD -100",
									},
								},
							},
							Ledger: "default",
						}
					})

					var expectedErr string
					if data.numscriptRewrite {
						expectedErr = string(components.V2ErrorsEnumInterpreterRuntime)
					} else {
						expectedErr = string(components.V2ErrorsEnumCompilationFailed)
					}
					It("should fail with "+expectedErr+" code", func() {
						Expect(err).NotTo(Succeed())
						Expect(err).To(HaveErrorCode(expectedErr))
					})
				})
				Context("with error on script", func() {
					BeforeEach(func() {
						req = operations.V2CreateTransactionRequest{
							IdempotencyKey: pointer.For("testing"),
							V2PostTransaction: components.V2PostTransaction{
								Metadata: map[string]string{},
								Script: &components.V2PostTransactionScript{
									Plain: `XXX`,
									Vars:  map[string]string{},
								},
							},
							Ledger: "default",
						}
					})
					var expectedErr string
					if data.numscriptRewrite {
						expectedErr = string(components.V2ErrorsEnumInterpreterParse)
					} else {
						expectedErr = string(components.V2ErrorsEnumCompilationFailed)
					}
					It("should fail with "+expectedErr+" code", func() {
						Expect(err).NotTo(Succeed())
						Expect(err).To(HaveErrorCode(expectedErr))
					})
				})
				Context("with no postings", func() {
					BeforeEach(func() {
						req = operations.V2CreateTransactionRequest{
							IdempotencyKey: pointer.For("testing"),
							V2PostTransaction: components.V2PostTransaction{
								Metadata: map[string]string{},
								Script: &components.V2PostTransactionScript{
									Plain: `vars {
								monetary $amount
							}
							set_tx_meta("foo", "bar")
							`,
									Vars: map[string]string{
										"amount": "USD 100",
									},
								},
							},
							Ledger: "default",
						}
					})
					It("should fail with "+string(components.V2ErrorsEnumNoPostings)+" code", func() {
						Expect(err).NotTo(Succeed())
						Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumNoPostings)))
					})
				})
				When("with metadata override", func() {
					BeforeEach(func() {
						req = operations.V2CreateTransactionRequest{
							IdempotencyKey: pointer.For("testing"),
							V2PostTransaction: components.V2PostTransaction{
								Metadata: map[string]string{
									"foo": "baz",
								},
								Script: &components.V2PostTransactionScript{
									Plain: `send [COIN 100] (
								source = @world
								destination = @bob
							)
							set_tx_meta("foo", "bar")`,
									Vars: map[string]string{},
								},
							},
							Ledger: "default",
						}
					})
					It("should fail with "+string(components.V2ErrorsEnumMetadataOverride)+" code", func() {
						Expect(err).NotTo(Succeed())
						Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumMetadataOverride)))
					})
				})
				When("with dry run mode", func() {
					BeforeEach(func() {
						req = operations.V2CreateTransactionRequest{
							IdempotencyKey: pointer.For("testing"),
							V2PostTransaction: components.V2PostTransaction{
								Metadata: map[string]string{},
								Script: &components.V2PostTransactionScript{
									Plain: `send [COIN 100] (
								source = @world
								destination = @bob
							)`,
									Vars: map[string]string{},
								},
							},
							DryRun: pointer.For(true),
							Ledger: "default",
						}
					})
					It("should be ok", func() {
						Expect(err).To(BeNil())
					})
				})
			})

			When("creating a transaction on the ledger v1 with old variable format", func() {
				var (
					err      error
					response *operations.CreateTransactionResponse
				)
				BeforeEach(func() {
					v, _ := big.NewInt(0).SetString("1320000000000000000000000000000000000000000000000001", 10)
					response, err = testServer.GetValue().Client().Ledger.V1.CreateTransaction(
						ctx,
						"default",
						components.PostTransaction{
							Metadata: map[string]any{},
							Script: &components.PostTransactionScript{
								Plain: `vars {
								monetary $amount
							}
							send $amount (
								source = @world
								destination = @bob
							)`,
								Vars: map[string]interface{}{
									"amount": map[string]any{
										"asset":  "EUR/12",
										"amount": v,
									},
								},
							},
						},
						nil,
					)
				})
				It("should be ok", func() {
					Expect(err).To(Succeed())
					Expect(response.TransactionsResponse.Data[0].Txid).To(Equal(big.NewInt(1)))
				})
			})
		})
	}
})
