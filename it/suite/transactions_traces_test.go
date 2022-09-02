package suite_test

import (
	"context"
	"fmt"
	"net/http"

	. "github.com/numary/ledger/it/internal"
	ledgerclient "github.com/numary/ledger/it/internal/client"
	. "github.com/numary/ledger/it/internal/otlpinterceptor"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Scenario("Transactions api", func() {
	WithNewLedger("with an empty ledger", func(ledger *string) {
		When("listing transactions", func() {
			var (
				err          error
				httpResponse *http.Response
			)
			BeforeEach(func() {
				_, httpResponse, err = Client().TransactionsApi.
					ListTransactions(context.Background(), *ledger).
					Execute()
				Expect(err).To(BeNil())
			})
			It("should create a trace", func() {
				Expect(httpResponse).To(HaveTrace(Trace{
					Name: "/:ledger/transactions",
					Attributes: HTTPStandardAttributes(
						http.MethodGet,
						fmt.Sprintf("/%s/transactions", *ledger),
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
			var (
				err          error
				httpResponse *http.Response
			)
			BeforeEach(func() {
				_, httpResponse, err = Client().TransactionsApi.
					CreateTransaction(context.Background(), *ledger).
					TransactionData(ledgerclient.TransactionData{
						Postings: []ledgerclient.Posting{
							*ledgerclient.NewPosting(100, "USD", "bank", "world"),
						},
					}).
					Execute()
				Expect(err).To(BeNil())
			})
			It("should create a trace", func() {
				Expect(httpResponse).To(HaveTrace(
					NewTrace("/:ledger/transactions").
						WithAttributes(HTTPStandardAttributes(
							http.MethodPost,
							fmt.Sprintf("/%s/transactions", *ledger),
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
		})
	})
})
