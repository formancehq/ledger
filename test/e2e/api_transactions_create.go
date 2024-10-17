//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/v2/logging"
	. "github.com/formancehq/go-libs/v2/testing/api"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/bus"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"
	"math/big"
	"time"

	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/go-libs/v2/pointer"
	ledgerevents "github.com/formancehq/ledger/pkg/events"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger accounts list API tests", func() {
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
			events = testServer.GetValue().Subscribe()
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
		Context("with valid data", func() {
			BeforeEach(func() {
				req = operations.V2CreateTransactionRequest{
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
				Expect(err).To(HaveOccurred())
				Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumInsufficientFund)))
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
							Vars: map[string]interface{}{},
						},
					},
					Ledger: "default",
				}
			})
			It("should fail with "+string(components.V2ErrorsEnumCompilationFailed)+" code", func() {
				Expect(err).NotTo(Succeed())
				Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumCompilationFailed)))
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
							Vars: map[string]interface{}{
								"amount": "USD -100",
							},
						},
					},
					Ledger: "default",
				}
			})
			It("should fail with "+string(components.V2ErrorsEnumCompilationFailed)+" code", func() {
				Expect(err).NotTo(Succeed())
				Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumCompilationFailed)))
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
							Vars:  map[string]interface{}{},
						},
					},
					Ledger: "default",
				}
			})
			It("should fail with "+string(components.V2ErrorsEnumCompilationFailed)+" code", func() {
				Expect(err).NotTo(Succeed())
				Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumCompilationFailed)))
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
							Vars: map[string]interface{}{
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
							Vars: map[string]interface{}{},
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
							Vars: map[string]interface{}{},
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
				operations.CreateTransactionRequest{
					PostTransaction: components.PostTransaction{
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
					Ledger: "default",
				},
			)
		})
		It("should be ok", func() {
			Expect(err).To(Succeed())
			Expect(response.TransactionsResponse.Data[0].Txid).To(Equal(big.NewInt(1)))
		})
	})
})
