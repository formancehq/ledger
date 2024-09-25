//go:build it

package test_suite

import (
	"context"
	"fmt"
	"github.com/alitto/pond"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	ledger "github.com/formancehq/ledger/internal"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/types"
	"math/big"
	"math/rand"
	"sync"
	"sync/atomic"
)

var _ = Context("Ledger stress tests", func() {
	var (
		db  = pgtesting.UsePostgresDatabase(pgServer)
		ctx = logging.TestingContext()
	)

	testServer := NewTestServer(func() Configuration {
		return Configuration{
			PostgresConfiguration: db.GetValue().ConnectionOptions(),
			Output:                GinkgoWriter,
			Debug:                 debug,
		}
	})

	const (
		countLedgers      = 30
		countBuckets      = 10
		countTransactions = 500
		countAccounts     = 20
	)

	When(fmt.Sprintf("creating %d ledgers dispatched on %d buckets", countLedgers, countLedgers/10), func() {
		BeforeEach(func() {
			for i := range countLedgers {
				bucketName := fmt.Sprintf("bucket%d", i/countBuckets)
				ledgerName := fmt.Sprintf("ledger%d", i)
				err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
					Ledger: ledgerName,
					V2CreateLedgerRequest: &components.V2CreateLedgerRequest{
						Bucket: &bucketName,
						Features: ledger.MinimalFeatureSet.
							// todo: as we are interested only by aggregated volumes at current date, these features should not be required
							With(ledger.FeatureMovesHistory, "ON").
							With(ledger.FeatureMovesHistoryPostCommitVolumes, "SYNC"),
					},
				})
				Expect(err).ShouldNot(HaveOccurred())
			}
		})
		When(fmt.Sprintf("creating %d transactions across the same account pool", countTransactions), func() {
			var (
				createdTransactions map[string][]*big.Int
				mu                  sync.Mutex
			)
			BeforeEach(func() {
				createdTransactions = map[string][]*big.Int{}
				wp := pond.New(20, 20)
				for range countTransactions {
					wp.Submit(func() {
						defer GinkgoRecover()

						ledger := fmt.Sprintf("ledger%d", rand.Intn(countLedgers))
						createdTx, err := CreateTransaction(ctx, testServer.GetValue(), operations.V2CreateTransactionRequest{
							Ledger: ledger,
							V2PostTransaction: components.V2PostTransaction{
								// todo: add another postings
								Postings: []components.V2Posting{{
									Source:      fmt.Sprintf("accounts:%d", rand.Intn(countAccounts)),
									Destination: fmt.Sprintf("accounts:%d", rand.Intn(countAccounts)),
									Asset:       "USD",
									Amount:      big.NewInt(100),
								}},
							},
							Force: pointer.For(true),
						})
						Expect(err).ShouldNot(HaveOccurred())
						mu.Lock()
						if createdTransactions[ledger] == nil {
							createdTransactions[ledger] = []*big.Int{}
						}
						createdTransactions[ledger] = append(createdTransactions[ledger], createdTx.ID)
						mu.Unlock()
					})
					go func() {

					}()
				}
				wp.StopAndWait()
			})
			When("getting aggregated volumes with no parameters", func() {
				It("should be zero", func() {
					Expect(testServer.GetValue()).To(HaveCoherentState())
				})
			})
			When("trying to revert concurrently all transactions", func() {
				It("should be handled correctly", func() {
					const (
						duplicates = 1
					)
					var (
						success  atomic.Int64
						failures atomic.Int64
					)
					wp := pond.New(20, 20)
					for ledger, ids := range createdTransactions {
						for _, id := range ids {
							for range duplicates + 1 {
								wp.Submit(func() {
									defer GinkgoRecover()

									_, err := RevertTransaction(ctx, testServer.GetValue(), operations.V2RevertTransactionRequest{
										Ledger: ledger,
										ID:     id,
										Force:  pointer.For(true),
									})
									if err == nil {
										success.Add(1)
									} else {
										failures.Add(1)
									}
								})
							}
						}
					}
					wp.StopAndWait()
					By("we should have the correct amount of success/failures", func() {
						Expect(success.Load()).To(Equal(int64(countTransactions)))
						Expect(failures.Load()).To(Equal(int64(duplicates * countTransactions)))
					})
					By("we should still have the aggregated balances to 0", func() {
						Expect(testServer.GetValue()).To(HaveCoherentState())
					})
				})
			})
		})
	})
})

type HaveCoherentStateMatcher struct{}

func (h HaveCoherentStateMatcher) Match(actual interface{}) (success bool, err error) {
	srv, ok := actual.(*Server)
	if !ok {
		return false, fmt.Errorf("expect type %T", new(Server))
	}
	ctx := context.Background()

	ledgers, err := ListLedgers(ctx, srv, operations.V2ListLedgersRequest{
		PageSize: pointer.For(int64(100)),
	})
	if err != nil {
		return false, err
	}

	for _, ledger := range ledgers.Data {
		aggregatedBalances, err := GetAggregatedBalances(ctx, srv, operations.V2GetBalancesAggregatedRequest{
			Ledger:           ledger.Name,
			UseInsertionDate: pointer.For(true),
		})
		Expect(err).To(BeNil())
		if len(aggregatedBalances) == 0 { // it's random, a ledger could not have been targeted
			// just in case, check if the ledger has transactions
			txs, err := ListTransactions(ctx, srv, operations.V2ListTransactionsRequest{
				Ledger: ledger.Name,
			})
			Expect(err).To(BeNil())
			Expect(txs.Data).To(HaveLen(0))
		} else {
			Expect(aggregatedBalances).To(HaveLen(1))
			Expect(aggregatedBalances["USD"]).To(Equal(big.NewInt(0)))
		}
	}

	return true, nil
}

func (h HaveCoherentStateMatcher) FailureMessage(_ interface{}) (message string) {
	return fmt.Sprintf("server should has coherent state")
}

func (h HaveCoherentStateMatcher) NegatedFailureMessage(_ interface{}) (message string) {
	return fmt.Sprintf("server should not has coherent state but has")
}

var _ types.GomegaMatcher = (*HaveCoherentStateMatcher)(nil)

func HaveCoherentState() *HaveCoherentStateMatcher {
	return &HaveCoherentStateMatcher{}
}
