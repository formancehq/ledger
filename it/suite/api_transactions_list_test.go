package suite_test

import (
	"fmt"
	"net/http"

	. "github.com/numary/ledger/it/internal"
	ledgerclient "github.com/numary/ledger/it/internal/client"
	. "github.com/numary/ledger/it/internal/otlpinterceptor"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = DescribeServerExecute("Transactions list", func() {
	WithNewLedger(func() {
		When("listing transactions", func() {
			var (
				cursorResponse ledgerclient.ListTransactions200Response
				httpResponse   *http.Response
			)
			BeforeEach(func() {
				cursorResponse, httpResponse = MustExecute[ledgerclient.ListTransactions200Response](ListTransactions())
			})
			It("should return no transactions", func() {
				Expect(cursorResponse.Cursor.Data).To(HaveLen(0))
			})
			It("should create a trace", func() {
				Expect(httpResponse).To(HaveTrace(Trace{
					Name: "/:ledger/transactions",
					Attributes: HTTPStandardAttributes(
						http.MethodGet,
						fmt.Sprintf("/%s/transactions", CurrentLedger()),
						"/:ledger/transactions",
					),
					SubSpans: []*Span{
						{Name: "Initialize"},
						{Name: "GetTransactions"},
						{Name: "Close"},
					},
				}))
			})
		})
		When("creating a new transactions from world to a 'bank' account", func() {
			BeforeEach(func() {
				_, _ = MustExecute[ledgerclient.TransactionsResponse](CreateTransaction().TransactionData(ledgerclient.TransactionData{
					Postings: []ledgerclient.Posting{
						*ledgerclient.NewPosting(100, "USD", "bank", "world"),
					},
				}))
			})
			Then("listing transactions", func() {
				var (
					cursorResponse ledgerclient.ListTransactions200Response
				)
				JustBeforeEach(func() {
					cursorResponse, _ = MustExecute[ledgerclient.ListTransactions200Response](ListTransactions())
				})
				It("should return 1 transaction", func() {
					Expect(cursorResponse.Cursor.Data).To(HaveLen(1))
				})
			})
		})
	})
})
