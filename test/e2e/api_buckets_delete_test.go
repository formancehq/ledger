//go:build it

package test_suite

import (
	"fmt"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Buckets deletion API tests", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	testServer := ginkgo.DeferTestServer(
		DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
		),
		testservice.WithLogger(GinkgoT()),
	)

	When("creating ledgers in different buckets", func() {
		const (
			bucket1 = "bucket1"
			bucket2 = "bucket2"
		)

		BeforeEach(func(specContext SpecContext) {
			// Create 3 ledgers in bucket1
			for i := range 3 {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
					Ledger: fmt.Sprintf("ledger-bucket1-%d", i),
					V2CreateLedgerRequest: components.V2CreateLedgerRequest{
						Bucket: pointer.For(bucket1),
					},
				})
				Expect(err).To(BeNil())
			}

			// Create 2 ledgers in bucket2
			for i := range 2 {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
					Ledger: fmt.Sprintf("ledger-bucket2-%d", i),
					V2CreateLedgerRequest: components.V2CreateLedgerRequest{
						Bucket: pointer.For(bucket2),
					},
				})
				Expect(err).To(BeNil())
			}
		})

		It("should list all ledgers before deletion", func(specContext SpecContext) {
			ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
				PageSize: pointer.For(int64(100)),
			})
			Expect(err).To(BeNil())
			Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(5))
		})

		It("should list ledgers from bucket1 before deletion", func(specContext SpecContext) {
			ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
				PageSize: pointer.For(int64(100)),
				RequestBody: map[string]any{
					"$match": map[string]any{
						"bucket": bucket1,
					},
				},
			})
			Expect(err).To(BeNil())
			Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(3))
		})

		When("deleting bucket1", func() {
			var err error

			JustBeforeEach(func(specContext SpecContext) {
				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteBucket(ctx, operations.V2DeleteBucketRequest{
					Bucket: bucket1,
				})
			})

			It("should succeed", func() {
				Expect(err).To(BeNil())
			})

			It("should not list ledgers from bucket1 after deletion", func(specContext SpecContext) {
				ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
					PageSize: pointer.For(int64(100)),
					RequestBody: map[string]any{
						"$match": map[string]any{
							"bucket": bucket1,
						},
					},
				})
				Expect(err).To(BeNil())
				Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(0))
			})

			It("should still list ledgers from bucket2", func(specContext SpecContext) {
				ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
					PageSize: pointer.For(int64(100)),
					RequestBody: map[string]any{
						"$match": map[string]any{
							"bucket": bucket2,
						},
					},
				})
				Expect(err).To(BeNil())
				Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(2))
			})

			It("should list only bucket2 ledgers in the full list", func(specContext SpecContext) {
				ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
					PageSize: pointer.For(int64(100)),
				})
				Expect(err).To(BeNil())
				Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(2))
				// Verify all remaining ledgers are from bucket2
				for _, ledger := range ledgers.V2LedgerListResponse.Cursor.Data {
					Expect(ledger.Bucket).To(Equal(bucket2))
				}
			})

			It("should list deleted ledgers from bucket1 when includeDeleted is true", func(specContext SpecContext) {
				ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
					PageSize:       pointer.For(int64(100)),
					IncludeDeleted: pointer.For(true),
					RequestBody: map[string]any{
						"$match": map[string]any{
							"bucket": bucket1,
						},
					},
				})
				Expect(err).To(BeNil())
				Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(3))
				// Verify all ledgers are from bucket1
				for _, ledger := range ledgers.V2LedgerListResponse.Cursor.Data {
					Expect(ledger.Bucket).To(Equal(bucket1))
				}
			})

			It("should list all ledgers including deleted when includeDeleted is true", func(specContext SpecContext) {
				ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
					PageSize:       pointer.For(int64(100)),
					IncludeDeleted: pointer.For(true),
				})
				Expect(err).To(BeNil())
				Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(5))
				// Verify we have ledgers from both buckets
				bucket1Count := 0
				bucket2Count := 0
				for _, ledger := range ledgers.V2LedgerListResponse.Cursor.Data {
					if ledger.Bucket == bucket1 {
						bucket1Count++
					} else if ledger.Bucket == bucket2 {
						bucket2Count++
					}
				}
				Expect(bucket1Count).To(Equal(3))
				Expect(bucket2Count).To(Equal(2))
			})
		})
	})

	When("deleting an empty bucket", func() {
		const emptyBucket = "empty-bucket"

		BeforeEach(func(specContext SpecContext) {
			// Create a bucket by creating a ledger, then delete the ledger
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: "temp-ledger",
				V2CreateLedgerRequest: components.V2CreateLedgerRequest{
					Bucket: pointer.For(emptyBucket),
				},
			})
			Expect(err).To(BeNil())

			// Note: We can't actually delete a ledger, but we can delete the bucket
			// which will mark all ledgers as deleted
		})

		When("deleting the bucket", func() {
			var err error

			JustBeforeEach(func(specContext SpecContext) {
				_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteBucket(ctx, operations.V2DeleteBucketRequest{
					Bucket: emptyBucket,
				})
			})

			It("should succeed", func() {
				Expect(err).To(BeNil())
			})

			It("should not list the ledger after bucket deletion", func(specContext SpecContext) {
				ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
					PageSize: pointer.For(int64(100)),
					RequestBody: map[string]any{
						"$match": map[string]any{
							"bucket": emptyBucket,
						},
					},
				})
				Expect(err).To(BeNil())
				Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(0))
			})

			It("should list the deleted ledger when includeDeleted is true", func(specContext SpecContext) {
				ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
					PageSize:       pointer.For(int64(100)),
					IncludeDeleted: pointer.For(true),
					RequestBody: map[string]any{
						"$match": map[string]any{
							"bucket": emptyBucket,
						},
					},
				})
				Expect(err).To(BeNil())
				Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(1))
				Expect(ledgers.V2LedgerListResponse.Cursor.Data[0].Name).To(Equal("temp-ledger"))
				Expect(ledgers.V2LedgerListResponse.Cursor.Data[0].Bucket).To(Equal(emptyBucket))
			})

			When("restoring the bucket", func() {
				var restoreErr error

				JustBeforeEach(func(specContext SpecContext) {
					_, restoreErr = Wait(specContext, DeferClient(testServer)).Ledger.V2.RestoreBucket(ctx, operations.V2RestoreBucketRequest{
						Bucket: emptyBucket,
					})
				})

				It("should succeed", func() {
					Expect(restoreErr).To(BeNil())
				})

				It("should list the ledger after restoration", func(specContext SpecContext) {
					ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
						PageSize: pointer.For(int64(100)),
						RequestBody: map[string]any{
							"$match": map[string]any{
								"bucket": emptyBucket,
							},
						},
					})
					Expect(err).To(BeNil())
					Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(1))
					Expect(ledgers.V2LedgerListResponse.Cursor.Data[0].Name).To(Equal("temp-ledger"))
					Expect(ledgers.V2LedgerListResponse.Cursor.Data[0].Bucket).To(Equal(emptyBucket))
				})
			})
		})
	})

	When("restoring a deleted bucket", func() {
		const (
			bucket1 = "restore-bucket1"
			bucket2 = "restore-bucket2"
		)

		BeforeEach(func(specContext SpecContext) {
			// Create 2 ledgers in bucket1
			for i := range 2 {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
					Ledger: fmt.Sprintf("restore-ledger1-%d", i),
					V2CreateLedgerRequest: components.V2CreateLedgerRequest{
						Bucket: pointer.For(bucket1),
					},
				})
				Expect(err).To(BeNil())
			}

			// Create 1 ledger in bucket2
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: "restore-ledger2-0",
				V2CreateLedgerRequest: components.V2CreateLedgerRequest{
					Bucket: pointer.For(bucket2),
				},
			})
			Expect(err).To(BeNil())

			// Delete bucket1
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteBucket(ctx, operations.V2DeleteBucketRequest{
				Bucket: bucket1,
			})
			Expect(err).To(BeNil())
		})

		It("should not list ledgers from bucket1 before restoration", func(specContext SpecContext) {
			ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
				PageSize: pointer.For(int64(100)),
				RequestBody: map[string]any{
					"$match": map[string]any{
						"bucket": bucket1,
					},
				},
			})
			Expect(err).To(BeNil())
			Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(0))
		})

		When("restoring bucket1", func() {
			var restoreErr error

			JustBeforeEach(func(specContext SpecContext) {
				_, restoreErr = Wait(specContext, DeferClient(testServer)).Ledger.V2.RestoreBucket(ctx, operations.V2RestoreBucketRequest{
					Bucket: bucket1,
				})
			})

			It("should succeed", func() {
				Expect(restoreErr).To(BeNil())
			})

			It("should list ledgers from bucket1 after restoration", func(specContext SpecContext) {
				ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
					PageSize: pointer.For(int64(100)),
					RequestBody: map[string]any{
						"$match": map[string]any{
							"bucket": bucket1,
						},
					},
				})
				Expect(err).To(BeNil())
				Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(2))
			})

			It("should still list ledgers from bucket2", func(specContext SpecContext) {
				ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
					PageSize: pointer.For(int64(100)),
					RequestBody: map[string]any{
						"$match": map[string]any{
							"bucket": bucket2,
						},
					},
				})
				Expect(err).To(BeNil())
				Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(1))
			})

			It("should list all ledgers including restored ones", func(specContext SpecContext) {
				ledgers, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListLedgers(ctx, operations.V2ListLedgersRequest{
					PageSize: pointer.For(int64(100)),
				})
				Expect(err).To(BeNil())
				Expect(ledgers.V2LedgerListResponse.Cursor.Data).To(HaveLen(3))
			})
		})
	})
})
