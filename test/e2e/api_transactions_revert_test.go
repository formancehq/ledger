//go:build it

package test_suite

import (
	libtime "github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/bus"
	"math/big"
	"time"

	"github.com/formancehq/go-libs/v2/logging"
	. "github.com/formancehq/go-libs/v2/testing/api"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"

	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	ledgerevents "github.com/formancehq/ledger/pkg/events"
)

var _ = Context("Ledger revert transactions API tests", func() {
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
		}
	})
	BeforeEach(func() {
		err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())
	})
	When("creating a transaction on a ledger", func() {
		var (
			timestamp = time.Now().Round(time.Second).UTC()
			tx        *components.V2Transaction
			events    chan *nats.Msg
			err       error
		)
		BeforeEach(func() {
			events = Subscribe(GinkgoT(), testServer.GetValue())
			tx, err = CreateTransaction(
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
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
		})
		When("transferring funds from destination to another account", func() {
			BeforeEach(func() {
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
									Source:      "alice",
									Destination: "foo",
								},
							},
							Timestamp: &timestamp,
						},
						Ledger: "default",
					},
				)
				Expect(err).ToNot(HaveOccurred())
			})
			When("trying to revert the original transaction", func() {
				var (
					force bool
					err   error
				)
				revertTx := func() {
					_, err = RevertTransaction(
						ctx,
						testServer.GetValue(),
						operations.V2RevertTransactionRequest{
							Force:  pointer.For(force),
							ID:     tx.ID,
							Ledger: "default",
						},
					)
				}
				JustBeforeEach(revertTx)
				It("Should fail", func() {
					Expect(err).To(HaveOccurred())
					Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumInsufficientFund)))
				})
				Context("With forcing", func() {
					BeforeEach(func() {
						force = true
					})
					It("Should be ok", func() {
						Expect(err).ToNot(HaveOccurred())
					})
				})
			})
		})
		When("reverting it", func() {
			var newTransaction *components.V2Transaction
			BeforeEach(func() {
				newTransaction, err = RevertTransaction(
					ctx,
					testServer.GetValue(),
					operations.V2RevertTransactionRequest{
						Ledger: "default",
						ID:     tx.ID,
					},
				)
				Expect(err).To(Succeed())
			})
			It("should trigger a new event", func() {
				Eventually(events).Should(Receive(Event(ledgerevents.EventTypeRevertedTransaction, WithPayload(bus.RevertedTransaction{
					Ledger: "default",
					RevertedTransaction: ledger.Transaction{
						ID: pointer.For(int(newTransaction.ID.Int64())),
						TransactionData: ledger.TransactionData{
							Metadata: map[string]string{
								"com.formance.spec/state/reverts": tx.ID.String(),
							},
							Postings: []ledger.Posting{
								ledger.NewPosting("alice", "world", "USD", big.NewInt(100)),
							},
							InsertedAt: libtime.New(*newTransaction.InsertedAt),
							Timestamp:  libtime.New(newTransaction.Timestamp),
						},
						PostCommitVolumes: map[string]ledger.VolumesByAssets{
							"world": {
								"USD": {
									Input:  big.NewInt(100),
									Output: big.NewInt(100),
								},
							},
							"alice": {
								"USD": {
									Input:  big.NewInt(100),
									Output: big.NewInt(100),
								},
							},
						},
						PostCommitEffectiveVolumes: map[string]ledger.VolumesByAssets{
							"world": {
								"USD": {
									Input:  big.NewInt(100),
									Output: big.NewInt(100),
								},
							},
							"alice": {
								"USD": {
									Input:  big.NewInt(100),
									Output: big.NewInt(100),
								},
							},
						},
					},
					RevertTransaction: ledger.Transaction{
						ID: pointer.For(int(tx.ID.Int64())),
						TransactionData: ledger.TransactionData{
							Metadata: map[string]string{},
							Postings: []ledger.Posting{
								ledger.NewPosting("world", "alice", "USD", big.NewInt(100)),
							},
							InsertedAt: libtime.New(*tx.InsertedAt),
							Timestamp:  libtime.New(tx.Timestamp),
						},
						RevertedAt: pointer.For(libtime.New(newTransaction.Timestamp)),
						PostCommitVolumes: map[string]ledger.VolumesByAssets{
							"world": {
								"USD": {
									Input:  new(big.Int),
									Output: big.NewInt(100),
								},
							},
							"alice": {
								"USD": {
									Input:  big.NewInt(100),
									Output: new(big.Int),
								},
							},
						},
					},
				}))))
			})
			It("should revert the original transaction", func() {
				response, err := GetTransaction(
					ctx,
					testServer.GetValue(),
					operations.V2GetTransactionRequest{
						Ledger: "default",
						ID:     tx.ID,
					},
				)
				Expect(err).NotTo(HaveOccurred())

				Expect(response.Reverted).To(BeTrue())

				By("The newly created transaction should have correct metadata applied", func() {
					response, err := GetTransaction(
						ctx,
						testServer.GetValue(),
						operations.V2GetTransactionRequest{
							Ledger: "default",
							ID:     newTransaction.ID,
						},
					)
					Expect(err).NotTo(HaveOccurred())
					Expect(response.Metadata).To(Equal(map[string]string{
						"com.formance.spec/state/reverts": tx.ID.String(),
					}))
				})
			})
			When("trying to revert again", func() {
				It("should be rejected", func() {
					_, err := RevertTransaction(
						ctx,
						testServer.GetValue(),
						operations.V2RevertTransactionRequest{
							Ledger: "default",
							ID:     tx.ID,
						},
					)
					Expect(err).NotTo(BeNil())
					Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumAlreadyRevert)))
				})
			})
		})
		When("reverting it at effective date", func() {
			BeforeEach(func() {
				_, err := RevertTransaction(
					ctx,
					testServer.GetValue(),
					operations.V2RevertTransactionRequest{
						Ledger:          "default",
						ID:              tx.ID,
						AtEffectiveDate: pointer.For(true),
					},
				)
				Expect(err).To(Succeed())
			})
			It("should revert the original transaction at date of the original tx", func() {
				response, err := GetTransaction(
					ctx,
					testServer.GetValue(),
					operations.V2GetTransactionRequest{
						Ledger: "default",
						ID:     tx.ID,
					},
				)
				Expect(err).NotTo(HaveOccurred())

				Expect(response.Reverted).To(BeTrue())
				Expect(response.Timestamp).To(Equal(tx.Timestamp))
			})
		})
		When("reverting with dryRun", func() {
			BeforeEach(func() {
				_, err := RevertTransaction(
					ctx,
					testServer.GetValue(),
					operations.V2RevertTransactionRequest{
						Ledger: "default",
						ID:     tx.ID,
						DryRun: pointer.For(true),
					},
				)
				Expect(err).To(Succeed())
			})
			It("should not revert the transaction", func() {
				tx, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
					Ledger: "default",
					ID:     tx.ID,
				})
				Expect(err).To(BeNil())
				Expect(tx.Reverted).To(BeFalse())
			})
		})
	})
	When("creating a transaction through an empty passthrough account", func() {
		var (
			timestamp = time.Now().Round(time.Second).UTC()
			tx        *components.V2Transaction
			err       error
		)
		BeforeEach(func() {
			tx, err = CreateTransaction(
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
								Destination: "walter",
							},
						},
						Timestamp: &timestamp,
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
		})
		When("creating the pass-through transaction", func() {
			BeforeEach(func() {
				tx, err = CreateTransaction(
					ctx,
					testServer.GetValue(),
					operations.V2CreateTransactionRequest{
						V2PostTransaction: components.V2PostTransaction{
							Metadata: map[string]string{},
							Postings: []components.V2Posting{
								{
									Amount:      big.NewInt(10),
									Asset:       "USD",
									Source:      "walter",
									Destination: "wendy",
								},
								{
									Amount:      big.NewInt(10),
									Asset:       "USD",
									Source:      "wendy",
									Destination: "world",
								},
							},
							Timestamp: &timestamp,
						},
						Ledger: "default",
					},
				)
				Expect(err).ToNot(HaveOccurred())
			})
			When("reverting the pass-through transaction", func() {
				BeforeEach(func() {
					_, err := RevertTransaction(
						ctx,
						testServer.GetValue(),
						operations.V2RevertTransactionRequest{
							Ledger:          "default",
							ID:              tx.ID,
							AtEffectiveDate: pointer.For(true),
						},
					)
					Expect(err).To(Succeed())
				})
				It("should revert the passthrough transaction at date of the original tx", func() {
					response, err := GetTransaction(
						ctx,
						testServer.GetValue(),
						operations.V2GetTransactionRequest{
							Ledger: "default",
							ID:     tx.ID,
						},
					)
					Expect(err).NotTo(HaveOccurred())
					Expect(response.Reverted).To(BeTrue())
					Expect(response.Timestamp).To(Equal(tx.Timestamp))
				})
			})
		})
	})
})
