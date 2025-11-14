//go:build it

package test_suite

import (
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"

	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

var _ = Context("Bucket cleanup worker", func() {
	var (
		db                = UseTemplatedDatabase()
		connectionOptions = DeferMap(db, (*pgtesting.Database).ConnectionOptions)
		ctx               = logging.TestingContext()
	)

	const (
		retentionPeriod = 2 * time.Second // Short retention for testing
	)

	testServer := ginkgo.DeferTestServer(
		connectionOptions,
		testservice.WithInstruments(
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
			ExperimentalFeaturesInstrumentation(),
		),
		testservice.WithLogger(GinkgoT()),
	)

	ginkgo.DeferTestWorker(connectionOptions, testservice.WithInstruments(
		BucketCleanupCRONSpecInstrumentation("* * * * * *"), // Run every second
		BucketCleanupRetentionPeriodInstrumentation(retentionPeriod),
	))

	When("deleting a bucket", func() {
		const (
			bucketName = "test-bucket-cleanup"
			ledgerName = "test-ledger"
		)

		JustBeforeEach(func(specContext SpecContext) {
			// Create a ledger in a bucket
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: ledgerName,
				V2CreateLedgerRequest: components.V2CreateLedgerRequest{
					Bucket: pointer.For(bucketName),
				},
			})
			Expect(err).To(BeNil())

			// Create a transaction to ensure the bucket schema is initialized
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
				Ledger: ledgerName,
				V2PostTransaction: components.V2PostTransaction{
					Postings: []components.V2Posting{
						{
							Amount:      big.NewInt(100),
							Asset:       "USD",
							Source:      "world",
							Destination: "bank",
						},
					},
				},
			})
			Expect(err).To(BeNil())

			// Delete the bucket (soft delete)
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteBucket(ctx, operations.V2DeleteBucketRequest{
				Bucket: bucketName,
			})
			Expect(err).To(BeNil())
		})

		It("should not delete the bucket immediately", func(specContext SpecContext) {
			// Verify the ledger still exists (with deleted_at set) via API
			ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
				PageSize:       pointer.For(int64(100)),
				IncludeDeleted: pointer.For(true),
				RequestBody: map[string]any{
					"$match": map[string]any{
						"name": ledgerName,
					},
				},
			})
			Expect(err).To(BeNil())
			Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(1))
			Expect(ledgers.V2LedgerListResponse.Cursor.Data[0].Name).To(Equal(ledgerName))
			Expect(ledgers.V2LedgerListResponse.Cursor.Data[0].DeletedAt).NotTo(BeNil(), "ledger should have deletedAt set")
		})

		It("should hard delete the bucket after retention period", func(specContext SpecContext) {
			// Wait for the worker to run (it runs every second)
			// Verify the ledger is deleted via API (even with includeDeleted=true)
			Eventually(func(g Gomega) bool {
				ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
					PageSize:       pointer.For(int64(100)),
					IncludeDeleted: pointer.For(true),
					RequestBody: map[string]any{
						"$match": map[string]any{
							"name": ledgerName,
						},
					},
				})
				g.Expect(err).To(BeNil())
				return len(ledgers.V2LedgerListResponse.Cursor.Data) == 0
			}).
				WithTimeout(5*time.Second).
				Should(BeTrue(), "ledger should be deleted from _system.ledgers after retention period")
		})
	})

	When("deleting multiple buckets", func() {
		const (
			bucket1Name = "test-bucket-cleanup-1"
			bucket2Name = "test-bucket-cleanup-2"
			bucket3Name = "test-bucket-cleanup-3"
			ledger1Name = "test-ledger-1"
			ledger2Name = "test-ledger-2"
			ledger3Name = "test-ledger-3"
		)

		JustBeforeEach(func(specContext SpecContext) {
			// Create ledgers in different buckets
			for _, tc := range []struct {
				ledgerName string
				bucketName string
			}{
				{ledger1Name, bucket1Name},
				{ledger2Name, bucket2Name},
				{ledger3Name, bucket3Name},
			} {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
					Ledger: tc.ledgerName,
					V2CreateLedgerRequest: components.V2CreateLedgerRequest{
						Bucket: pointer.For(tc.bucketName),
					},
				})
				Expect(err).To(BeNil())

				// Create a transaction to ensure the bucket schema is initialized
				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
					Ledger: tc.ledgerName,
					V2PostTransaction: components.V2PostTransaction{
						Postings: []components.V2Posting{
							{
								Amount:      big.NewInt(100),
								Asset:       "USD",
								Source:      "world",
								Destination: "bank",
							},
						},
					},
				})
				Expect(err).To(BeNil())
			}

			// Delete all buckets
			for _, bucketName := range []string{bucket1Name, bucket2Name} {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteBucket(ctx, operations.V2DeleteBucketRequest{
					Bucket: bucketName,
				})
				Expect(err).To(BeNil())
			}
		})

		It("should handle multiple buckets correctly", func(specContext SpecContext) {

			Eventually(func(g Gomega) bool {
				// Verify ledger1 is deleted (even with includeDeleted=true)
				list, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
					PageSize:       pointer.For(int64(100)),
					IncludeDeleted: pointer.For(true),
					RequestBody: map[string]any{
						"$match": map[string]any{
							"name": ledger1Name,
						},
					},
				})
				g.Expect(err).To(BeNil())

				return len(list.V2LedgerListResponse.Cursor.Data) == 0
			}).
				WithTimeout(5*time.Second).
				Should(BeTrue(), "buckets should be handled correctly")

			Eventually(func(g Gomega) bool {

				// Verify ledger2 is deleted (even with includeDeleted=true)
				list, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
					PageSize:       pointer.For(int64(100)),
					IncludeDeleted: pointer.For(true),
					RequestBody: map[string]any{
						"$match": map[string]any{
							"name": ledger2Name,
						},
					},
				})
				g.Expect(err).To(BeNil())

				return len(list.V2LedgerListResponse.Cursor.Data) == 0
			}).
				WithTimeout(5*time.Second).
				Should(BeTrue(), "buckets should be handled correctly")

			// Verify ledger3 details
			ledgers3, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
				PageSize:       pointer.For(int64(100)),
				IncludeDeleted: pointer.For(true),
				RequestBody: map[string]any{
					"$match": map[string]any{
						"name": ledger3Name,
					},
				},
			})
			Expect(err).To(BeNil())
			Expect(ledgers3.V2LedgerListResponse.Cursor.Data).To(HaveLen(1))
			Expect(ledgers3.V2LedgerListResponse.Cursor.Data[0].Name).To(Equal(ledger3Name))
			Expect(ledgers3.V2LedgerListResponse.Cursor.Data[0].DeletedAt).To(BeNil())
			Expect(ledgers3.V2LedgerListResponse.Cursor.Data[0].Bucket).To(Equal(bucket3Name))
		})
	})
})
