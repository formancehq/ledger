//go:build it

package test_suite

import (
	"bytes"
	"encoding/json"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/bus"
	ledgerevents "github.com/formancehq/ledger/pkg/events"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
	"github.com/nats-io/nats.go"
	"math/big"
	"net/http"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	. "github.com/formancehq/go-libs/v3/testing/api"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"

	"github.com/formancehq/go-libs/v3/metadata"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger engine tests", func() {

	var (
		db           = UseTemplatedDatabase()
		ctx          = logging.TestingContext()
		events       chan *nats.Msg
		bulkResponse *operations.V2CreateBulkResponse
		bulkMaxSize  = 100
		natsURL      = ginkgo.DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)
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
		_, events = Subscribe(specContext, testServer, natsURL)
	})
	When("creating a bulk on a ledger", func() {
		var (
			now              = time.Now().Round(time.Microsecond).UTC()
			items            []components.V2BulkElement
			err              error
			atomic, parallel bool
		)
		BeforeEach(func() {
			atomic = false
			parallel = false
			items = []components.V2BulkElement{
				components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
					Data: &components.V2PostTransaction{
						Metadata: map[string]string{},
						Postings: []components.V2Posting{{
							Amount:      big.NewInt(100),
							Asset:       "USD/2",
							Destination: "bank",
							Source:      "world",
						}},
						Timestamp: &now,
					},
				}),
				components.CreateV2BulkElementAddMetadata(components.V2BulkElementAddMetadata{
					Data: &components.Data{
						Metadata: metadata.Metadata{
							"foo":  "bar",
							"role": "admin",
						},
						TargetID:   components.CreateV2TargetIDBigint(big.NewInt(1)),
						TargetType: components.V2TargetTypeTransaction,
					},
				}),
				components.CreateV2BulkElementDeleteMetadata(components.V2BulkElementDeleteMetadata{
					Data: &components.V2BulkElementDeleteMetadataData{
						Key:        "foo",
						TargetID:   components.CreateV2TargetIDBigint(big.NewInt(1)),
						TargetType: components.V2TargetTypeTransaction,
					},
				}),
				components.CreateV2BulkElementRevertTransaction(components.V2BulkElementRevertTransaction{
					Data: &components.V2BulkElementRevertTransactionData{
						ID: big.NewInt(1),
					},
				}),
			}
		})
		JustBeforeEach(func(specContext SpecContext) {
			bulkResponse, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateBulk(ctx, operations.V2CreateBulkRequest{
				Atomic:      pointer.For(atomic),
				Parallel:    pointer.For(parallel),
				RequestBody: items,
				Ledger:      "default",
			})
		})
		shouldBeOk := func(specContext SpecContext) {
			Expect(err).To(Succeed())

			tx, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetTransaction(ctx, operations.V2GetTransactionRequest{
				ID:     big.NewInt(1),
				Ledger: "default",
			})
			Expect(err).To(Succeed())
			reversedTx, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetTransaction(ctx, operations.V2GetTransactionRequest{
				ID:     big.NewInt(2),
				Ledger: "default",
			})
			Expect(err).To(Succeed())

			Expect(tx.V2GetTransactionResponse.Data).To(Equal(components.V2Transaction{
				ID: big.NewInt(1),
				Metadata: metadata.Metadata{
					"role": "admin",
				},
				Postings: []components.V2Posting{{
					Amount:      big.NewInt(100),
					Asset:       "USD/2",
					Destination: "bank",
					Source:      "world",
				}},
				Reverted:   true,
				RevertedAt: &reversedTx.V2GetTransactionResponse.Data.Timestamp,
				Timestamp:  now,
				InsertedAt: tx.V2GetTransactionResponse.Data.InsertedAt,
			}))
			By("It should send events", func() {
				Eventually(events).Should(Receive(Event(ledgerevents.EventTypeCommittedTransactions)))
				Eventually(events).Should(Receive(Event(ledgerevents.EventTypeSavedMetadata)))
				Eventually(events).Should(Receive(Event(ledgerevents.EventTypeDeletedMetadata)))
				Eventually(events).Should(Receive(Event(ledgerevents.EventTypeRevertedTransaction)))
			})
		}
		It("should be ok", shouldBeOk)
		Context("with atomic", func() {
			BeforeEach(func() {
				atomic = true
			})
			It("should be ok", shouldBeOk)
		})
		Context("with exceeded batch size", func() {
			BeforeEach(func() {
				items = make([]components.V2BulkElement, 0)
				for i := 0; i < bulkMaxSize+1; i++ {
					items = append(items, components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
						Data: &components.V2PostTransaction{
							Metadata: map[string]string{},
							Postings: []components.V2Posting{{
								Amount:      big.NewInt(100),
								Asset:       "USD/2",
								Destination: "bank",
								Source:      "world",
							}},
							Timestamp: &now,
						},
					}))
				}
			})
			It("should respond with an error", func() {
				Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumBulkSizeExceeded)))
			})
		})
		Context("with parallel", func() {
			BeforeEach(func() {
				parallel = true
				items = make([]components.V2BulkElement, 0)
				for i := 0; i < bulkMaxSize; i++ {
					items = append(items, components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
						Data: &components.V2PostTransaction{
							Metadata: map[string]string{},
							Postings: []components.V2Posting{{
								Amount:      big.NewInt(100),
								Asset:       "USD/2",
								Destination: "bank",
								Source:      "world",
							}},
							Timestamp: &now,
						},
					}))
				}
			})
			It("should be ok", func() {
				Expect(err).To(BeNil())
			})
		})
	})
	When("creating a bulk on a ledger using json stream", func() {
		var (
			now   = time.Now().Round(time.Microsecond).UTC()
			items []components.V2BulkElement
			err   error
		)
		BeforeEach(func() {
			items = []components.V2BulkElement{
				components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
					Data: &components.V2PostTransaction{
						Metadata: map[string]string{},
						Postings: []components.V2Posting{{
							Amount:      big.NewInt(100),
							Asset:       "USD/2",
							Destination: "bank",
							Source:      "world",
						}},
						Timestamp: &now,
					},
				}),
				components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
					Data: &components.V2PostTransaction{
						Metadata: map[string]string{},
						Postings: []components.V2Posting{{
							Amount:      big.NewInt(100),
							Asset:       "USD/2",
							Destination: "bank",
							Source:      "world",
						}},
						Timestamp: &now,
					},
				}),
			}
		})
		JustBeforeEach(func(specContext SpecContext) {
			stream := bytes.NewBuffer(nil)
			for _, item := range items {
				data, err := json.Marshal(item)
				Expect(err).To(Succeed())
				stream.Write(data)
			}
			stream.Write([]byte("\n"))

			testServer, err := testServer.Wait(specContext)
			Expect(err).To(BeNil())

			req, err := http.NewRequest(http.MethodPost, testservice.GetServerURL(testServer).String()+"/v2/default/_bulk", stream)
			req.Header.Set("Content-Type", "application/vnd.formance.ledger.api.v2.bulk+json-stream")
			Expect(err).To(Succeed())

			rsp, err := http.DefaultClient.Do(req)
			Expect(err).To(Succeed())
			Expect(rsp.StatusCode).To(Equal(http.StatusOK))
		})
		It("should be ok", func(specContext SpecContext) {
			Expect(err).To(Succeed())

			txs, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
				Ledger: "default",
			})
			Expect(err).To(Succeed())
			Expect(txs.V2TransactionsCursorResponse.Cursor.Data).To(HaveLen(2))
		})
	})
	When("creating a bulk with an error on a ledger", func() {
		var (
			now    = time.Now().Round(time.Microsecond).UTC()
			err    error
			atomic bool
		)
		JustBeforeEach(func(specContext SpecContext) {
			bulkResponse, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateBulk(ctx, operations.V2CreateBulkRequest{
				Atomic: pointer.For(atomic),
				RequestBody: []components.V2BulkElement{
					components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
						Data: &components.V2PostTransaction{
							Metadata: map[string]string{},
							Postings: []components.V2Posting{{
								Amount:      big.NewInt(100),
								Asset:       "USD/2",
								Destination: "bank",
								Source:      "world",
							}},
							Timestamp: &now,
						},
					}),
					components.CreateV2BulkElementCreateTransaction(components.V2BulkElementCreateTransaction{
						Data: &components.V2PostTransaction{
							Metadata: map[string]string{},
							Postings: []components.V2Posting{{
								Amount:      big.NewInt(200), // Insufficient fund
								Asset:       "USD/2",
								Destination: "user",
								Source:      "bank",
							}},
							Timestamp: &now,
						},
					}),
				},
				Ledger: "default",
			})
			Expect(err).To(Succeed())
		})
		shouldRespondWithAnError := func() {
			GinkgoHelper()

			Expect(bulkResponse.V2BulkResponse.Data[1].Type).To(Equal(components.V2BulkElementResultType("ERROR")))
			Expect(bulkResponse.V2BulkResponse.Data[1].V2BulkElementResultError.ErrorCode).To(Equal("INSUFFICIENT_FUND"))
		}
		It("should respond with an error", func(specContext SpecContext) {
			shouldRespondWithAnError()

			By("should have created the first item", func() {
				txs, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
					Ledger: "default",
				})
				Expect(err).To(Succeed())
				Expect(txs.V2TransactionsCursorResponse.Cursor.Data).To(HaveLen(1))
			})

			By("Should have sent one event", func() {
				Eventually(events).Should(Receive(Event(ledgerevents.EventTypeCommittedTransactions, WithPayload(bus.CommittedTransactions{
					Ledger:          "default",
					Transactions:    []ledger.Transaction{ConvertSDKTxToCoreTX(&bulkResponse.V2BulkResponse.Data[0].V2BulkElementResultCreateTransaction.Data)},
					AccountMetadata: ledger.AccountMetadata{},
				}))))
				Eventually(events).ShouldNot(Receive())
			})
		})
		Context("with atomic", func() {
			BeforeEach(func() {
				atomic = true
			})
			It("should not commit anything", func(specContext SpecContext) {
				shouldRespondWithAnError()

				txs, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
					Ledger: "default",
				})
				Expect(err).To(Succeed())
				Expect(txs.V2TransactionsCursorResponse.Cursor.Data).To(HaveLen(0))

				By("Should not have sent any event", func() {
					Eventually(events).ShouldNot(Receive())
				})
			})
		})
	})
})
