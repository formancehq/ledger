package suite_test

import (
	"context"
	"net/http"

	"github.com/google/uuid"
	. "github.com/numary/ledger/it/internal"
	ledgerclient "github.com/numary/ledger/it/internal/client"
	"github.com/numary/ledger/pkg/api/idempotency"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Scenario("Transactions api", func() {
	WithNewLedger("with an empty ledger", func(ledger *string) {
		When("listing transactions", func() {
			var (
				err            error
				cursorResponse ledgerclient.ListTransactions200Response
			)
			BeforeEach(func() {
				cursorResponse, _, err = Client().TransactionsApi.
					ListTransactions(context.Background(), *ledger).
					Execute()
				Expect(err).To(BeNil())
			})
			It("should return no transactions", func() {
				Expect(cursorResponse.Cursor.Data).To(HaveLen(0))
			})
		})
		When("creating a new transactions from world to a 'bank' account", func() {
			var (
				err      error
				request  ledgerclient.ApiCreateTransactionRequest
				response ledgerclient.TransactionsResponse
				postings = []ledgerclient.Posting{
					*ledgerclient.NewPosting(100, "USD", "bank", "world"),
				}
			)
			BeforeEach(func() {
				request = Client().TransactionsApi.
					CreateTransaction(context.Background(), *ledger).
					TransactionData(ledgerclient.TransactionData{
						Postings: postings,
					})
			})
			JustBeforeEach(func() {
				response, _, err = request.Execute()
				Expect(err).To(BeNil())
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
			Then("listing transactions", func() {
				var (
					err            error
					cursorResponse ledgerclient.ListTransactions200Response
					httpResponse   *http.Response
				)
				JustBeforeEach(func() {
					cursorResponse, httpResponse, err = Client().TransactionsApi.
						ListTransactions(context.Background(), *ledger).
						Execute()
					Expect(err).To(BeNil())
				})
				It("should return 1 transaction", func() {
					Expect(cursorResponse.Cursor.Data).To(HaveLen(1))
					_ = httpResponse
				})
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
						response, httpResponse, err = request.Execute()
					})
					It("Should return response using idempotency key hit", func() {
						Expect(err).To(BeNil())
						Expect(httpResponse.Header.Get(idempotency.HeaderIdempotencyHit)).To(Equal("true"))
					})
				})
			})
		})
	})
})
