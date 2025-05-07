package test_suite

import (
	stdtime "time"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/internal/worker"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/robfig/cron/v3"
)

var _ = Context("Bucket deletion lifecycle tests", func() {
	var (
		ctx = logging.TestingContext()
	)

	Context("Bucket deletion lifecycle", func() {
		db := UseTemplatedDatabase()
		connectionOptions := DeferMap(db, func(from *pgtesting.Database) bunconnect.ConnectionOptions {
			return bunconnect.ConnectionOptions{
				DatabaseSourceName: from.ConnectionOptions().DatabaseSourceName,
				MaxOpenConns:       100,
			}
		})
		natsURL := DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)
		testServer := DeferTestServer(
			connectionOptions,
			testservice.WithInstruments(
				testservice.NatsInstrumentation(natsURL),
				testservice.DebugInstrumentation(debug),
				testservice.OutputInstrumentation(GinkgoWriter),
			),
			testservice.WithLogger(GinkgoT()),
		)

		When("creating a bucket and marking it for deletion", func() {
			var (
				bucketName string
				ledgerName string
			)

			BeforeEach(func(specContext SpecContext) {
				bucketName = "test-bucket-" + uuid.NewString()[:8]
				ledgerName = bucketName + "-ledger"

				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
					Ledger: ledgerName,
					V2CreateLedgerRequest: components.V2CreateLedgerRequest{
						Bucket: pointer.For(bucketName),
					},
				})
				Expect(err).ToNot(HaveOccurred())

				info, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetLedgerInfo(ctx, operations.V2GetLedgerInfoRequest{
					Ledger: ledgerName,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(info.V2LedgerInfoResponse.Data.Name).To(PointTo(Equal(ledgerName)))
			})

			It("should mark the bucket as deleted and make it inaccessible", func(specContext SpecContext) {
				client := Wait(specContext, DeferClient(testServer))
				resp, err := client.Ledger.V2.DeleteBucket(ctx, operations.V2DeleteBucketRequest{
					Bucket: bucketName,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(resp.HTTPMeta.Response.StatusCode).To(Equal(204))

				_, err = client.Ledger.V2.GetLedgerInfo(ctx, operations.V2GetLedgerInfoRequest{
					Ledger: ledgerName,
				})
				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("404"))

				buckets, err := client.Ledger.V2.ListBuckets(ctx)
				Expect(err).ToNot(HaveOccurred())
				
				var foundBucket bool
				var isDeleted bool
				for _, bucket := range buckets.V2BucketWithStatuses {
					if bucket.Name == bucketName {
						foundBucket = true
						isDeleted = bucket.DeletedAt != nil
						break
					}
				}
				Expect(foundBucket).To(BeTrue(), "Bucket should be found in the list")
				Expect(isDeleted).To(BeTrue(), "Bucket should be marked as deleted")
			})

			It("should physically delete the bucket after grace period", func(specContext SpecContext) {
				client := Wait(specContext, DeferClient(testServer))
				_, err := client.Ledger.V2.DeleteBucket(ctx, operations.V2DeleteBucketRequest{
					Bucket: bucketName,
				})
				Expect(err).ToNot(HaveOccurred())

				db := ConnectToDatabase(ctx, connectionOptions)

				bucketFactory := bucket.NewDefaultFactory()
				ledgerStoreFactory := ledgerstore.NewFactory(db)
				systemStoreFactory := system.NewStoreFactory()

				drv := driver.New(db, ledgerStoreFactory, bucketFactory, systemStoreFactory)

				schedule, err := cron.ParseStandard("* * * * * *") // Every second
				Expect(err).ToNot(HaveOccurred())

				_, err = db.NewUpdate().
					Model(&ledger.Bucket{}).
					Set("deleted_at = ?", time.Now().Add(-2*stdtime.Minute)).
					Where("name = ?", bucketName).
					Exec(ctx)
				Expect(err).ToNot(HaveOccurred())

				logger := logging.FromContext(ctx)
				bucketDeletionRunner := worker.NewBucketDeletionRunner(logger, drv, worker.BucketDeletionRunnerConfig{
					Schedule:    schedule,
					GracePeriod: 1 * time.Second, // Very short grace period for testing
				})

				err = bucketDeletionRunner.Run(ctx)
				Expect(err).ToNot(HaveOccurred())

				count, err := db.NewSelect().
					Model(&ledger.Bucket{}).
					Where("name = ?", bucketName).
					Count(ctx)
				Expect(err).ToNot(HaveOccurred())
				Expect(count).To(Equal(0), "Bucket should be physically deleted from the database")

				buckets, err := client.Ledger.V2.ListBuckets(ctx)
				Expect(err).ToNot(HaveOccurred())
				
				var foundBucket bool
				for _, bucket := range buckets.V2BucketWithStatuses {
					if bucket.Name == bucketName {
						foundBucket = true
						break
					}
				}
				Expect(foundBucket).To(BeFalse(), "Bucket should not be found in the list after physical deletion")
			})

			It("should allow restoring a bucket marked for deletion", func(specContext SpecContext) {
				client := Wait(specContext, DeferClient(testServer))
				_, err := client.Ledger.V2.DeleteBucket(ctx, operations.V2DeleteBucketRequest{
					Bucket: bucketName,
				})
				Expect(err).ToNot(HaveOccurred())

				_, err = client.Ledger.V2.GetLedgerInfo(ctx, operations.V2GetLedgerInfoRequest{
					Ledger: ledgerName,
				})
				Expect(err).To(HaveOccurred())

				resp, err := client.Ledger.V2.RestoreBucket(ctx, operations.V2RestoreBucketRequest{
					Bucket: bucketName,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(resp.HTTPMeta.Response.StatusCode).To(Equal(204))

				info, err := client.Ledger.V2.GetLedgerInfo(ctx, operations.V2GetLedgerInfoRequest{
					Ledger: ledgerName,
				})
				Expect(err).ToNot(HaveOccurred())
				Expect(info.V2LedgerInfoResponse.Data.Name).To(PointTo(Equal(ledgerName)))

				buckets, err := client.Ledger.V2.ListBuckets(ctx)
				Expect(err).ToNot(HaveOccurred())
				
				var foundBucket bool
				var isDeleted bool
				for _, bucket := range buckets.V2BucketWithStatuses {
					if bucket.Name == bucketName {
						foundBucket = true
						isDeleted = bucket.DeletedAt != nil
						break
					}
				}
				Expect(foundBucket).To(BeTrue(), "Bucket should be found in the list")
				Expect(isDeleted).To(BeFalse(), "Bucket should not be marked as deleted")
			})
		})
	})
})
