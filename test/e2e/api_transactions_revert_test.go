//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
	"math/big"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	. "github.com/formancehq/go-libs/v3/testing/api"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"

	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/nats-io/nats.go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	ledgerevents "github.com/formancehq/ledger/pkg/events"
)

var _ = Context("Ledger revert transactions API tests", func() {
	var (
		db      = UseTemplatedDatabase()
		ctx     = logging.TestingContext()
		natsURL = ginkgo.DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)
	)

	testServer := DeferTestServer(
		ginkgo.DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(
			testservice.NatsInstrumentation(natsURL),
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
		),
		testservice.WithLogger(GinkgoT()),
	)

	BeforeEach(func(specContext SpecContext) {
		_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())
	})
	When("creating a transaction on a ledger", func() {
		var (
			timestamp = time.Now().Round(time.Second).UTC()
			tx        *operations.V2CreateTransactionResponse
			events    chan *nats.Msg
			err       error
		)
		BeforeEach(func(specContext SpecContext) {
			_, events = Subscribe(specContext, testServer, natsURL)
			tx, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(
				ctx,
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
			BeforeEach(func(specContext SpecContext) {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(
					ctx,
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
				revertTx := func(specContext SpecContext) {
					_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.RevertTransaction(
						ctx,
						operations.V2RevertTransactionRequest{
							Force:  pointer.For(force),
							ID:     tx.V2CreateTransactionResponse.Data.ID,
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
			BeforeEach(func(specContext SpecContext) {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.RevertTransaction(
					ctx,
					operations.V2RevertTransactionRequest{
						Ledger: "default",
						ID:     tx.V2CreateTransactionResponse.Data.ID,
					},
				)
				Expect(err).To(Succeed())
			})
			It("should trigger a new event", func() {
				Eventually(events).Should(Receive(Event(ledgerevents.EventTypeRevertedTransaction)))
			})
			It("should revert the original transaction", func(specContext SpecContext) {
				response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetTransaction(
					ctx,
					operations.V2GetTransactionRequest{
						Ledger: "default",
						ID:     tx.V2CreateTransactionResponse.Data.ID,
					},
				)
				Expect(err).NotTo(HaveOccurred())

				Expect(response.V2GetTransactionResponse.Data.Reverted).To(BeTrue())
			})
			When("trying to revert again", func() {
				It("should be rejected", func(specContext SpecContext) {
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.RevertTransaction(
						ctx,
						operations.V2RevertTransactionRequest{
							Ledger: "default",
							ID:     tx.V2CreateTransactionResponse.Data.ID,
						},
					)
					Expect(err).NotTo(BeNil())
					Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumAlreadyRevert)))
				})
			})
		})
		When("reverting it at effective date", func() {
			BeforeEach(func(specContext SpecContext) {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.RevertTransaction(
					ctx,
					operations.V2RevertTransactionRequest{
						Ledger:          "default",
						ID:              tx.V2CreateTransactionResponse.Data.ID,
						AtEffectiveDate: pointer.For(true),
					},
				)
				Expect(err).To(Succeed())
			})
			It("should revert the original transaction at date of the original tx", func(specContext SpecContext) {
				response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetTransaction(
					ctx,
					operations.V2GetTransactionRequest{
						Ledger: "default",
						ID:     tx.V2CreateTransactionResponse.Data.ID,
					},
				)
				Expect(err).NotTo(HaveOccurred())

				Expect(response.V2GetTransactionResponse.Data.Reverted).To(BeTrue())
				Expect(response.V2GetTransactionResponse.Data.Timestamp).To(Equal(tx.V2CreateTransactionResponse.Data.Timestamp))
			})
		})
		When("reverting with dryRun", func() {
			BeforeEach(func(specContext SpecContext) {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.RevertTransaction(
					ctx,
					operations.V2RevertTransactionRequest{
						Ledger: "default",
						ID:     tx.V2CreateTransactionResponse.Data.ID,
						DryRun: pointer.For(true),
					},
				)
				Expect(err).To(Succeed())
			})
			It("should not revert the transaction", func(specContext SpecContext) {
				tx, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetTransaction(ctx, operations.V2GetTransactionRequest{
					Ledger: "default",
					ID:     tx.V2CreateTransactionResponse.Data.ID,
				})
				Expect(err).To(BeNil())
				Expect(tx.V2GetTransactionResponse.Data.Reverted).To(BeFalse())
			})
		})
	})
	When("creating a transaction through an empty passthrough account", func() {
		var (
			timestamp = time.Now().Round(time.Second).UTC()
			tx        *operations.V2CreateTransactionResponse
			err       error
		)
		BeforeEach(func(specContext SpecContext) {
			tx, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(
				ctx,
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
			BeforeEach(func(specContext SpecContext) {
				tx, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(
					ctx,
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
				BeforeEach(func(specContext SpecContext) {
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.RevertTransaction(
						ctx,
						operations.V2RevertTransactionRequest{
							Ledger:          "default",
							ID:              tx.V2CreateTransactionResponse.Data.ID,
							AtEffectiveDate: pointer.For(true),
						},
					)
					Expect(err).To(Succeed())
				})
				It("should revert the passthrough transaction at date of the original tx", func(specContext SpecContext) {
					response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetTransaction(
						ctx,
						operations.V2GetTransactionRequest{
							Ledger: "default",
							ID:     tx.V2CreateTransactionResponse.Data.ID,
						},
					)
					Expect(err).NotTo(HaveOccurred())
					Expect(response.V2GetTransactionResponse.Data.Reverted).To(BeTrue())
					Expect(response.V2GetTransactionResponse.Data.Timestamp).To(Equal(tx.V2CreateTransactionResponse.Data.Timestamp))
				})
			})
		})
	})
})
