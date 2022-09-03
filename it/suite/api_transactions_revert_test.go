package suite_test

import (
	"context"

	ledgerclient "github.com/numary/ledger/it/internal/client"
	. "github.com/numary/ledger/it/internal/httplistener"
	. "github.com/numary/ledger/it/internal/server"
	"github.com/numary/ledger/pkg/bus"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = DescribeServerExecute("Transactions create api", func() {
	WithNewLedger(func() {
		Describe("Given a ledger with a transaction registered", func() {
			var (
				tx core.ExpandedTransaction
			)
			BeforeEach(func() {
				tx = core.ExpandedTransaction{
					Transaction: core.Transaction{
						TransactionData: core.NewTransactionData(
							core.NewPosting("world", "bank", "USD", core.NewMonetaryInt(100)),
						),
					},
					PreCommitVolumes: core.NewAccountsAssetsVolumes().
						SetVolumes("world", "USD", core.NewVolumes(core.NewMonetaryInt(0), core.NewMonetaryInt(0))).
						SetVolumes("bank", "USD", core.NewVolumes(core.NewMonetaryInt(0), core.NewMonetaryInt(0))),
					PostCommitVolumes: core.NewAccountsAssetsVolumes().
						SetVolumes("world", "USD", core.NewVolumes(core.NewMonetaryInt(0), core.NewMonetaryInt(100))).
						SetVolumes("bank", "USD", core.NewVolumes(core.NewMonetaryInt(100), core.NewMonetaryInt(0))),
				}
				Expect(GetLedgerStore().Commit(context.Background(), tx)).To(BeNil())
			})
			When("Reverting it", func() {
				var (
					response ledgerclient.TransactionResponse
				)
				BeforeEach(func() {
					response, _ = MustExecute[ledgerclient.TransactionResponse](RevertTransaction(int32(tx.ID)))
				})
				It("Should be ok", func() {
					Expect(response.Data.Txid).To(BeEquivalentTo(1))

					count, err := GetLedgerStore().CountTransactions(context.Background(), *ledger.NewTransactionsQuery())
					Expect(err).To(BeNil())
					Expect(count).To(BeEquivalentTo(2))

					volumes, err := GetLedgerStore().GetVolumes(context.Background(), "bank", "USD")
					Expect(err).To(BeNil())
					Expect(volumes).To(Equal(core.Volumes{
						Input:  core.NewMonetaryInt(100),
						Output: core.NewMonetaryInt(100),
					}))
				})
				It("Should trigger an event", func() {
					tx.Metadata = core.RevertedMetadata(uint64(response.Data.Txid))
					Expect(CurrentLedger()).To(HaveTriggeredEvent(bus.RevertedTransaction{
						Ledger:              CurrentLedger(),
						RevertedTransaction: tx,
						RevertTransaction: core.ExpandedTransaction{
							Transaction: core.Transaction{
								TransactionData: core.NewTransactionData(core.NewPosting("bank", "world", "USD", core.NewMonetaryInt(100))).
									SetReference("").
									SetTimestamp(response.Data.Timestamp).
									SetMetadata(core.RevertMetadata(tx.ID)),
								ID: 1,
							},
							PreCommitVolumes: core.NewAccountsAssetsVolumes().
								SetVolumes("world", "USD", core.NewVolumes(core.NewMonetaryInt(0), core.NewMonetaryInt(100))).
								SetVolumes("bank", "USD", core.NewVolumes(core.NewMonetaryInt(100), core.NewMonetaryInt(0))),
							PostCommitVolumes: core.NewAccountsAssetsVolumes().
								SetVolumes("world", "USD", core.NewVolumes(core.NewMonetaryInt(100), core.NewMonetaryInt(100))).
								SetVolumes("bank", "USD", core.NewVolumes(core.NewMonetaryInt(100), core.NewMonetaryInt(100))),
						},
					}))
				})
			})
		})
	})
})
