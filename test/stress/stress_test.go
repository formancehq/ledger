//go:build it

package test_suite

import (
	"fmt"
	"github.com/formancehq/ledger/pkg/features"
	"math/big"
	"math/rand"
	"sync"
	"sync/atomic"

	"github.com/alitto/pond"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger stress tests", func() {
	var (
		db  = pgtesting.UsePostgresDatabase(pgServer)
		ctx = logging.TestingContext()
	)

	testServer := NewTestServer(func() Configuration {
		return Configuration{
			CommonConfiguration: CommonConfiguration{
				PostgresConfiguration: db.GetValue().ConnectionOptions(),
				Output:                GinkgoWriter,
				Debug:                 debug,
			},
			ExperimentalFeatures: true,
		}
	})

	const (
		countLedgers      = 6
		countBuckets      = 3
		countTransactions = 500
		countAccounts     = 80
	)

	When(fmt.Sprintf("creating %d ledgers dispatched on %d buckets", countLedgers, countLedgers/10), func() {
		BeforeEach(func() {
			for i := range countLedgers {
				bucketName := fmt.Sprintf("bucket%d", i/countBuckets)
				ledgerName := fmt.Sprintf("ledger%d", i)
				err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
					Ledger: ledgerName,
					V2CreateLedgerRequest: components.V2CreateLedgerRequest{
						Bucket:   &bucketName,
						Features: features.MinimalFeatureSet.With(features.FeatureMovesHistory, "ON"),
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
								Postings: []components.V2Posting{
									{
										Source:      fmt.Sprintf("accounts:%d", rand.Intn(countAccounts)),
										Destination: fmt.Sprintf("accounts:%d", rand.Intn(countAccounts)),
										Asset:       "USD",
										Amount:      big.NewInt(100),
									},
									{
										Source:      fmt.Sprintf("accounts:%d", rand.Intn(countAccounts)),
										Destination: fmt.Sprintf("accounts:%d", rand.Intn(countAccounts)),
										Asset:       "USD",
										Amount:      big.NewInt(100),
									},
								},
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
						// We will introduce attempts to duplicate transactions twice.
						// At the end we will check than the correct number of revert has
						// succeeded and the correct number has failed.
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
