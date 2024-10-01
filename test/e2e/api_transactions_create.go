//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/logging"
	. "github.com/formancehq/go-libs/testing/api"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/bus"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"
	"math/big"
	"time"

	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/pointer"
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

	When("creating a transaction on a ledger", func() {
		var (
			events    chan *nats.Msg
			timestamp = time.Now().Round(time.Second).UTC()
			rsp       *components.V2Transaction
			err       error
		)
		BeforeEach(func() {

			events = testServer.GetValue().Subscribe()

			// Create a transaction
			rsp, err = CreateTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2CreateTransactionRequest{
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
				},
			)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should be available on api", func() {
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
		})
		When("trying to commit a new transaction with the same reference", func() {
			var (
				err error
			)
			BeforeEach(func() {
				_, err = CreateTransaction(
					ctx,
					testServer.GetValue(),
					operations.V2CreateTransactionRequest{
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
					},
				)
				Expect(err).To(HaveOccurred())
				Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumConflict)))
			})
			It("Should fail with "+string(components.V2ErrorsEnumConflict)+" error code", func() {})
		})
		It("should trigger a new event", func() {
			// Wait for created transaction event
			Eventually(events).Should(Receive(Event(ledgerevents.EventTypeCommittedTransactions, WithPayload(bus.CommittedTransactions{
				Ledger:          "default",
				Transactions:    []ledger.Transaction{ConvertSDKTxToCoreTX(rsp)},
				AccountMetadata: ledger.AccountMetadata{},
			}))))
		})
	})

	When("creating a transaction on a ledger with insufficient funds", func() {
		It("should fail", func() {
			_, err := CreateTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Metadata: map[string]string{},
						Postings: []components.V2Posting{
							{
								Amount:      big.NewInt(100),
								Asset:       "USD",
								Source:      "bob",
								Destination: "alice",
							},
						},
					},
					Ledger: "default",
				},
			)
			Expect(err).To(HaveOccurred())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumInsufficientFund)))
		})
	})

	When("creating a transaction on a ledger with an idempotency key and a specific ledger", func() {
		var (
			err        error
			response   *components.V2Transaction
			timestamp  = time.Now().Add(-1 * time.Minute).Round(time.Second).UTC()
			timestamp2 = time.Now().Round(time.Second).UTC()
		)
		BeforeEach(func() {
			response, err = CreateTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2CreateTransactionRequest{
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
				},
			)
		})
		It("should be ok", func() {
			Expect(err).To(Succeed())
			Expect(response.ID).To(Equal(big.NewInt(1)))
		})
		When("creating a ledger transaction with same ik and different ledger", func() {
			BeforeEach(func() {
				response, err = CreateTransaction(
					ctx,
					testServer.GetValue(),
					operations.V2CreateTransactionRequest{
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
							Timestamp: &timestamp2,
							Reference: pointer.For("foo2"),
						},
						Ledger: "test",
					},
				)
			})
			It("should not have an error", func() {
				Expect(err).To(Succeed())
				Expect(response.ID).To(Equal(big.NewInt(1)))
			})
		})
	})

	When("creating a transaction on a ledger with an idempotency key", func() {
		var (
			err      error
			response *components.V2Transaction
			req      operations.V2CreateTransactionRequest
		)
		createTransaction := func() {
			response, err = CreateTransaction(ctx, testServer.GetValue(), req)
		}
		BeforeEach(func() {
			req = operations.V2CreateTransactionRequest{
				IdempotencyKey: pointer.For("testing"),
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
				Ledger: "default",
			}
		})
		JustBeforeEach(createTransaction)
		It("should be ok", func() {
			Expect(err).To(Succeed())
			Expect(response.ID).To(Equal(big.NewInt(1)))
		})
		When("replaying with the same IK", func() {
			BeforeEach(createTransaction)
			It("should respond with the same tx id", func() {
				Expect(err).To(Succeed())
				Expect(response.ID).To(Equal(big.NewInt(1)))
			})
		})
		When("creating another tx with the same IK but different input", func() {
			JustBeforeEach(func() {
				req.V2PostTransaction.Metadata = metadata.Metadata{
					"foo": "bar",
				}
				createTransaction()
			})
			It("should fail", func() {
				Expect(err).NotTo(Succeed())
				Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
			})
		})
	})
	// TODO(gfyrag): test negative amount with a variable
	When("creating a transaction on a ledger with a negative amount in the script", func() {
		var (
			err error
		)
		BeforeEach(func() {
			_, err = CreateTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2CreateTransactionRequest{
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
				},
			)
		})
		It("should fail with "+string(components.V2ErrorsEnumCompilationFailed)+" code", func() {
			Expect(err).NotTo(Succeed())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumCompilationFailed)))
		})
	})
	When("creating a transaction on a ledger with a negative amount in the script", func() {
		var (
			err error
		)
		BeforeEach(func() {
			_, err = CreateTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2CreateTransactionRequest{
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
				},
			)
		})
		It("should fail with "+string(components.V2ErrorsEnumCompilationFailed)+" code", func() {
			Expect(err).NotTo(Succeed())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumCompilationFailed)))
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
	When("creating a transaction on a ledger with error on script", func() {
		var (
			err error
		)
		BeforeEach(func() {
			_, err = CreateTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2CreateTransactionRequest{
					IdempotencyKey: pointer.For("testing"),
					V2PostTransaction: components.V2PostTransaction{
						Metadata: map[string]string{},
						Script: &components.V2PostTransactionScript{
							Plain: `XXX`,
							Vars:  map[string]interface{}{},
						},
					},
					Ledger: "default",
				},
			)
		})
		It("should fail with "+string(components.V2ErrorsEnumCompilationFailed)+" code", func() {
			Expect(err).NotTo(Succeed())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumCompilationFailed)))
		})
	})
	When("creating a transaction with no postings", func() {
		var (
			err error
		)
		BeforeEach(func() {
			_, err = CreateTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2CreateTransactionRequest{
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
				},
			)
		})
		It("should fail with "+string(components.V2ErrorsEnumNoPostings)+" code", func() {
			Expect(err).NotTo(Succeed())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumNoPostings)))
		})
	})
	When("creating a transaction with metadata override", func() {
		var (
			err error
		)
		BeforeEach(func() {
			_, err = CreateTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2CreateTransactionRequest{
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
				},
			)
		})
		It("should fail with "+string(components.V2ErrorsEnumMetadataOverride)+" code", func() {
			Expect(err).NotTo(Succeed())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumMetadataOverride)))
		})
	})
	When("creating a tx with dry run mode", func() {
		var (
			err error
			ret *components.V2Transaction
		)
		BeforeEach(func() {
			ret, err = CreateTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2CreateTransactionRequest{
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
				},
			)
		})
		It("should be ok", func() {
			Expect(err).To(BeNil())
		})
		When("creating a tx without dry run", func() {
			var (
				tx *components.V2Transaction
			)
			BeforeEach(func() {
				tx, err = CreateTransaction(
					ctx,
					testServer.GetValue(),
					operations.V2CreateTransactionRequest{
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
						Ledger: "default",
					},
				)
			})
			It("Should return the same tx id as with dry run", func() {
				Expect(tx.ID.Uint64()).To(Equal(ret.ID.Uint64() + 1))
			})
		})
	})
})
