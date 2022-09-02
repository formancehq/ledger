package suite_test

import (
	"fmt"
	"net/http"

	"github.com/google/uuid"
	. "github.com/numary/ledger/it/internal"
	ledgerclient "github.com/numary/ledger/it/internal/client"
	. "github.com/numary/ledger/it/internal/httplistener"
	. "github.com/numary/ledger/it/internal/otlpinterceptor"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/api/idempotency"
	"github.com/numary/ledger/pkg/bus"
	"github.com/numary/ledger/pkg/core"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = DescribeServerExecute("Transactions create api", func() {
	WithNewLedger(func() {
		When("creating a new transactions from world to a 'bank' account", func() {
			var (
				request      ledgerclient.ApiCreateTransactionRequest
				response     ledgerclient.TransactionsResponse
				httpResponse *http.Response
				postings     = []ledgerclient.Posting{
					*ledgerclient.NewPosting(100, "USD", "bank", "world"),
				}
			)
			BeforeEach(func() {
				request = CreateTransaction().TransactionData(ledgerclient.TransactionData{
					Postings: postings,
				})
			})
			JustBeforeEach(func() {
				response, httpResponse = MustExecute[ledgerclient.TransactionsResponse](request)
			})
			It("should return a complete transaction", func() {
				Expect(response.Data).To(HaveLen(1))
				Expect(response.Data[0]).To(Equal(ledgerclient.Transaction{
					Postings:  postings,
					Timestamp: response.Data[0].Timestamp,
					Reference: ledgerclient.PtrString(""), // TODO: Fix that in ledger
					PreCommitVolumes: ledgerclient.NewAggregatedVolumes().
						SetVolumes("world", "USD", ledgerclient.NewVolume(0, 0, 0)).
						SetVolumes("bank", "USD", ledgerclient.NewVolume(0, 0, 0)),
					PostCommitVolumes: ledgerclient.NewAggregatedVolumes().
						SetVolumes("world", "USD", ledgerclient.NewVolume(0, 100, -100)).
						SetVolumes("bank", "USD", ledgerclient.NewVolume(100, 0, 100)),
				}))
			})
			It("Should trigger an event", func() {
				Expect(CurrentLedger()).To(HaveTriggeredEvent(bus.CommittedTransactions{
					Ledger: CurrentLedger(),
					Transactions: []core.ExpandedTransaction{
						{
							Transaction: core.Transaction{
								TransactionData: core.NewTransactionData(core.NewPosting("world", "bank", "USD", core.NewMonetaryInt(100))).
									SetReference("").
									SetTimestamp(response.Data[0].Timestamp),
							},
							PreCommitVolumes: core.NewAccountsAssetsVolumes().
								SetVolumes("world", "USD", core.NewVolumes(core.NewMonetaryInt(0), core.NewMonetaryInt(0))).
								SetVolumes("bank", "USD", core.NewVolumes(core.NewMonetaryInt(0), core.NewMonetaryInt(0))),
							PostCommitVolumes: core.NewAccountsAssetsVolumes().
								SetVolumes("world", "USD", core.NewVolumes(core.NewMonetaryInt(0), core.NewMonetaryInt(100))).
								SetVolumes("bank", "USD", core.NewVolumes(core.NewMonetaryInt(100), core.NewMonetaryInt(0))),
						},
					},
					Volumes: core.NewAccountsAssetsVolumes().
						SetVolumes("bank", "USD", core.NewVolumes(core.NewMonetaryInt(100), core.NewMonetaryInt(0))).
						SetVolumes("world", "USD", core.NewVolumes(core.NewMonetaryInt(0), core.NewMonetaryInt(100))),
					PostCommitVolumes: core.NewAccountsAssetsVolumes().
						SetVolumes("bank", "USD", core.NewVolumes(core.NewMonetaryInt(100), core.NewMonetaryInt(0))).
						SetVolumes("world", "USD", core.NewVolumes(core.NewMonetaryInt(0), core.NewMonetaryInt(100))),
					PreCommitVolumes: core.NewAccountsAssetsVolumes().
						SetVolumes("bank", "USD", core.NewVolumes(core.NewMonetaryInt(0), core.NewMonetaryInt(0))).
						SetVolumes("world", "USD", core.NewVolumes(core.NewMonetaryInt(0), core.NewMonetaryInt(0))),
				}))
			})
			It("should create a trace", func() {
				Expect(httpResponse).To(HaveTrace(
					NewTrace("/:ledger/transactions").
						WithAttributes(HTTPStandardAttributes(
							http.MethodPost,
							fmt.Sprintf("/%s/transactions", CurrentLedger()),
							"/:ledger/transactions",
						)).
						AddSubSpans(
							NewSpan("Initialize"),
							NewSpan("LoadMapping"),
							NewSpan("GetLastTransaction"),
							NewSpan("GetVolumes"),
							NewSpan("GetVolumes"),
							NewSpan("GetAccount"),
							NewSpan("Commit"),
							NewSpan("Close"),
						),
				))
			})
			Context("with an idempotency key", func() {
				var (
					ik           string
					httpResponse *http.Response
				)
				BeforeEach(func() {
					ik = uuid.NewString()
					request = request.IdempotencyKey(ik)
				})
				Context("Then replay request", func() {
					JustBeforeEach(func() {
						response, httpResponse = MustExecute[ledgerclient.TransactionsResponse](request)
					})
					It("Should return response using idempotency key hit", func() {
						Expect(httpResponse.Header.Get(idempotency.HeaderIdempotencyHit)).To(Equal("true"))
					})
				})
				Context("Then reusing IK with another request", func() {
					var (
						httpResponse *http.Response
						err          error
					)
					JustBeforeEach(func() {
						_, httpResponse, err = CreateTransaction().
							TransactionData(ledgerclient.TransactionData{
								Postings: []ledgerclient.Posting{{
									Amount:      666,
									Asset:       "USD",
									Destination: "bank",
									Source:      "world",
								}},
							}).
							IdempotencyKey(ik).
							Execute()
					})
					It("Should return a 400 status code", func() {
						Expect(httpResponse.StatusCode).To(Equal(http.StatusBadRequest))
						Expect(err).To(HaveLedgerErrorCode(apierrors.ErrInternal))
					})
				})
			})
		})
	})
})
