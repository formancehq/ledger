package suite_test

import (
	"context"
	"net/http"

	. "github.com/numary/ledger/it/internal"
	ledgerclient "github.com/numary/numary-sdk-go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Scenario("Transactions api", func(env *Environment) {
	WithNewLedger("with an empty ledger", func(ledger *string) {
		When("listing transactions", func() {
			var (
				err            error
				cursorResponse ledgerclient.ListTransactions200Response
			)
			BeforeEach(func() {
				cursorResponse, _, err = env.TransactionsApi.
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
				response ledgerclient.TransactionsResponse
				postings = []ledgerclient.Posting{
					*ledgerclient.NewPosting(100, "USD", "bank", "world"),
				}
			)
			BeforeEach(func() {
				response, _, err = env.TransactionsApi.
					CreateTransaction(context.Background(), *ledger).
					TransactionData(ledgerclient.TransactionData{
						Postings: postings,
					}).
					Execute()
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
				BeforeEach(func() {
					cursorResponse, httpResponse, err = env.TransactionsApi.
						ListTransactions(context.Background(), *ledger).
						Execute()
					Expect(err).To(BeNil())
				})
				It("should return 1 transaction", func() {
					Expect(cursorResponse.Cursor.Data).To(HaveLen(1))
					_ = httpResponse
				})
			})
		})
	})
})
