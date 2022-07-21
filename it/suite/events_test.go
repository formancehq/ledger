package suite_test

import (
	"context"

	. "github.com/numary/ledger/it/internal"
	. "github.com/numary/ledger/it/internal/httplistener"
	"github.com/numary/ledger/pkg/bus"
	"github.com/numary/ledger/pkg/core"
	ledgerclient "github.com/numary/numary-sdk-go"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Scenario("Transactions api", func(env *Environment) {
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
				response, _, err = env.TransactionsApi.
					CreateTransaction(context.Background(), *ledger).
					TransactionData(ledgerclient.TransactionData{
						Postings: postings,
					}).
					Execute()
				Expect(err).To(BeNil())
			})
			It("Should trigger an event", func() {
				Expect(ledger).To(HaveTriggeredEvent(bus.CommittedTransactions{
					Transactions: []core.Transaction{
						{
							TransactionData: core.NewTransactionData(core.NewPosting("world", "bank", "USD", 100)).
								SetReference("").
								SetTimestamp(response.Data[0].Timestamp),
							PreCommitVolumes: core.NewAccountsAssetsVolumes().
								SetVolumes("world", "USD", core.NewVolumes(0, 0)).
								SetVolumes("bank", "USD", core.NewVolumes(0, 0)),
							PostCommitVolumes: core.NewAccountsAssetsVolumes().
								SetVolumes("world", "USD", core.NewVolumes(0, 100)).
								SetVolumes("bank", "USD", core.NewVolumes(100, 0)),
						},
					},
					Volumes: core.NewAccountsAssetsVolumes().
						SetVolumes("bank", "USD", core.NewVolumes(100, 0)).
						SetVolumes("world", "USD", core.NewVolumes(0, 100)),
					PostCommitVolumes: core.NewAccountsAssetsVolumes().
						SetVolumes("bank", "USD", core.NewVolumes(100, 0)).
						SetVolumes("world", "USD", core.NewVolumes(0, 100)),
					PreCommitVolumes: core.NewAccountsAssetsVolumes().
						SetVolumes("bank", "USD", core.NewVolumes(0, 0)).
						SetVolumes("world", "USD", core.NewVolumes(0, 0)),
				}))
			})
		})
	})
})
