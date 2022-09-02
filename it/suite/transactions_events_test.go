package suite_test

import (
	"context"

	. "github.com/numary/ledger/it/internal"
	ledgerclient "github.com/numary/ledger/it/internal/client"
	. "github.com/numary/ledger/it/internal/httplistener"
	"github.com/numary/ledger/pkg/bus"
	"github.com/numary/ledger/pkg/core"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Scenario("Transactions api", func() {
	WithNewLedger("with an empty ledger", func(ledger *string) {
		When("creating a new transactions from world to a 'bank' account", func() {
			var (
				err      error
				response ledgerclient.TransactionsResponse
				postings = []ledgerclient.Posting{
					*ledgerclient.NewPosting(100, "USD", "bank", "world"),
				}
			)
			BeforeEach(func() {
				response, _, err = Client().TransactionsApi.
					CreateTransaction(context.Background(), *ledger).
					TransactionData(ledgerclient.TransactionData{
						Postings: postings,
					}).
					Execute()
				Expect(err).To(BeNil())
			})
			It("Should trigger an event", func() {
				Expect(ledger).To(HaveTriggeredEvent(bus.CommittedTransactions{
					Ledger: *ledger,
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
		})
	})
})
