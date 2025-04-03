//go:build it

package test_suite

import (
	"bytes"
	"encoding/json"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/testing/deferred"
	"github.com/formancehq/go-libs/v2/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v2/testing/testservice"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/bus"
	ledgerevents "github.com/formancehq/ledger/pkg/events"
	"github.com/nats-io/nats.go"
	"math/big"
	"net/http"
	"time"

	"github.com/formancehq/go-libs/v2/logging"
	. "github.com/formancehq/go-libs/v2/testing/api"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"

	"github.com/formancehq/go-libs/v2/metadata"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger engine tests", func() {

	var (
		db           = UseTemplatedDatabase()
		ctx          = logging.TestingContext()
		events       chan *nats.Msg
		bulkResponse []components.V2BulkElementResult
		bulkMaxSize  = 100
		natsURL = deferred.DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)
	)

	testServer := DeferTestServer(
		deferred.DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(
			testservice.NatsInstrumentation(natsURL),
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
		),
		testservice.WithLogger(GinkgoT()),
	)

	BeforeEach(func() {
		err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())
		_, events = Subscribe(GinkgoT(), testServer.GetValue(), natsURL)
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
		JustBeforeEach(func() {
			bulkResponse, err = CreateBulk(ctx, testServer.GetValue(), operations.V2CreateBulkRequest{
				Atomic:      pointer.For(atomic),
				Parallel:    pointer.For(parallel),
				RequestBody: items,
				Ledger:      "default",
			})
		})
		shouldBeOk := func() {
			Expect(err).To(Succeed())

			tx, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
				ID:     big.NewInt(1),
				Ledger: "default",
			})
			Expect(err).To(Succeed())
			reversedTx, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
				ID:     big.NewInt(2),
				Ledger: "default",
			})
			Expect(err).To(Succeed())

			Expect(*tx).To(Equal(components.V2Transaction{
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
				RevertedAt: &reversedTx.Timestamp,
				Timestamp:  now,
				InsertedAt: tx.InsertedAt,
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
		JustBeforeEach(func() {
			stream := bytes.NewBuffer(nil)
			for _, item := range items {
				data, err := json.Marshal(item)
				Expect(err).To(Succeed())
				stream.Write(data)
			}
			stream.Write([]byte("\n"))

			req, err := http.NewRequest(http.MethodPost, testservice.GetServerURL(testServer.GetValue()).String()+"/v2/default/_bulk", stream)
			req.Header.Set("Content-Type", "application/vnd.formance.ledger.api.v2.bulk+json-stream")
			Expect(err).To(Succeed())

			rsp, err := http.DefaultClient.Do(req)
			Expect(err).To(Succeed())
			Expect(rsp.StatusCode).To(Equal(http.StatusOK))
		})
		It("should be ok", func() {
			Expect(err).To(Succeed())

			txs, err := ListTransactions(ctx, testServer.GetValue(), operations.V2ListTransactionsRequest{
				Ledger: "default",
			})
			Expect(err).To(Succeed())
			Expect(txs.Data).To(HaveLen(2))
		})
	})
	When("creating a bulk with an error on a ledger", func() {
		var (
			now    = time.Now().Round(time.Microsecond).UTC()
			err    error
			atomic bool
		)
		JustBeforeEach(func() {
			bulkResponse, err = CreateBulk(ctx, testServer.GetValue(), operations.V2CreateBulkRequest{
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

			Expect(bulkResponse[1].Type).To(Equal(components.V2BulkElementResultType("ERROR")))
			Expect(bulkResponse[1].V2BulkElementResultError.ErrorCode).To(Equal("INSUFFICIENT_FUND"))
		}
		It("should respond with an error", func() {
			shouldRespondWithAnError()

			By("should have created the first item", func() {
				txs, err := ListTransactions(ctx, testServer.GetValue(), operations.V2ListTransactionsRequest{
					Ledger: "default",
				})
				Expect(err).To(Succeed())
				Expect(txs.Data).To(HaveLen(1))
			})

			By("Should have sent one event", func() {
				Eventually(events).Should(Receive(Event(ledgerevents.EventTypeCommittedTransactions, WithPayload(bus.CommittedTransactions{
					Ledger:          "default",
					Transactions:    []ledger.Transaction{ConvertSDKTxToCoreTX(&bulkResponse[0].V2BulkElementResultCreateTransaction.Data)},
					AccountMetadata: ledger.AccountMetadata{},
				}))))
				Eventually(events).ShouldNot(Receive())
			})
		})
		Context("with atomic", func() {
			BeforeEach(func() {
				atomic = true
			})
			It("should not commit anything", func() {
				shouldRespondWithAnError()

				txs, err := ListTransactions(ctx, testServer.GetValue(), operations.V2ListTransactionsRequest{
					Ledger: "default",
				})
				Expect(err).To(Succeed())
				Expect(txs.Data).To(HaveLen(0))

				By("Should not have sent any event", func() {
					Eventually(events).ShouldNot(Receive())
				})
			})
		})
	})
})
