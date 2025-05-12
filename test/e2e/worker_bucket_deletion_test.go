//go:build it

package test_suite

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/api"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	. "github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/formancehq/ledger/internal/storage/ledger"
	"github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/internal/worker"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/robfig/cron/v3"
)

var _ = Context("Bucket deletion worker", func() {
	var (
		db         = UseTemplatedDatabase()
		ctx        = logging.TestingContext()
		testServer = ginkgo.DeferTestServer(
			DeferMap(db, (*Database).ConnectionOptions),
			testservice.WithInstruments(
				testservice.OutputInstrumentation(GinkgoWriter),
			),
			testservice.WithLogger(GinkgoT()),
		)
	)

	When("creating a bucket and marking it for deletion", Ordered, func() {
		var (
			bucketName string
			ledgerName string
		)

		BeforeAll(func() {
			bucketName = "test-bucket-" + uuid.NewString()[:8]
			ledgerName = "test-ledger-" + uuid.NewString()[:8]
		})

		It("should create a ledger in a new bucket", func(specContext SpecContext) {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: ledgerName,
				V2CreateLedgerRequest: components.V2CreateLedgerRequest{
					Bucket: pointer.For(bucketName),
				},
			})
			Expect(err).ToNot(HaveOccurred())

			ledgerInfo, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetLedgerInfo(ctx, operations.V2GetLedgerInfoRequest{
				Ledger: ledgerName,
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(*ledgerInfo.V2LedgerInfoResponse.Data.Name).To(Equal(ledgerName))
		})

		It("should mark the bucket for deletion", func(specContext SpecContext) {
			// Wait for the server to be ready
			serverURL := testservice.GetServerURL(Wait(specContext, testServer))
			
			req, err := http.NewRequest("DELETE", serverURL.String()+"/v2/_/buckets/"+bucketName, nil)
			Expect(err).ToNot(HaveOccurred())
			resp, err := http.DefaultClient.Do(req)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusNoContent))
			resp.Body.Close()

			time.Sleep(100 * time.Millisecond)

			req, err = http.NewRequest("GET", serverURL.String()+"/v2/_/buckets", nil)
			Expect(err).ToNot(HaveOccurred())
			resp, err = http.DefaultClient.Do(req)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			var buckets []map[string]interface{}
			Expect(json.NewDecoder(resp.Body).Decode(&buckets)).To(Succeed())
			resp.Body.Close()

			var foundBucket bool
			for _, bucket := range buckets {
				if bucket["name"] == bucketName {
					foundBucket = true
					deletedAt, ok := bucket["deletedAt"]
					Expect(ok).To(BeTrue(), "deletedAt field should exist")
					Expect(deletedAt).NotTo(BeNil(), "deletedAt should not be nil")
					Expect(deletedAt).NotTo(Equal(""), "deletedAt should not be empty")
				}
			}
			Expect(foundBucket).To(BeTrue(), "Bucket should be found in buckets list with deleted status")

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.GetLedgerInfo(ctx, operations.V2GetLedgerInfoRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(HaveOccurred())
			Expect(err).To(HaveErrorCode("LEDGER_NOT_FOUND"))
		})

		It("should physically delete the bucket using the worker", func(specContext SpecContext) {
			dbOptions := DeferMap(db, (*Database).ConnectionOptions)
			
			bunDB, err := bunconnect.OpenSQLDB(ctx, dbOptions.GetValue())
			Expect(err).ToNot(HaveOccurred())
			defer bunDB.Close()
			
			testDriver := driver.New(
				bunDB,
				ledger.NewFactory(bunDB),
				bucket.NewDefaultFactory(),
				system.NewStoreFactory(),
			)
			Expect(testDriver).NotTo(BeNil())
			
			schedule, err := cron.ParseStandard("* * * * * *") // Every second
			Expect(err).ToNot(HaveOccurred())

			gracePeriod := time.Nanosecond
			bucketDeletionRunner := worker.NewBucketDeletionRunner(
				logging.NewDefaultLogger(GinkgoWriter, true, false, false),
				testDriver,
				worker.BucketDeletionRunnerConfig{
					Schedule:    schedule,
					GracePeriod: gracePeriod,
				},
			)

			err = bucketDeletionRunner.RunOnce(ctx)
			Expect(err).ToNot(HaveOccurred())

			// Wait for the server to be ready
			serverURL := testservice.GetServerURL(Wait(specContext, testServer))
			
			req, err := http.NewRequest("GET", serverURL.String()+"/v2/_/buckets", nil)
			Expect(err).ToNot(HaveOccurred())
			resp, err := http.DefaultClient.Do(req)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			var buckets []map[string]interface{}
			Expect(json.NewDecoder(resp.Body).Decode(&buckets)).To(Succeed())
			resp.Body.Close()

			foundBucket := false
			for _, bucket := range buckets {
				if bucket["name"] == bucketName {
					foundBucket = true
				}
			}
			Expect(foundBucket).To(BeFalse(), "Bucket should be completely deleted and not found in buckets list")
		})

		It("should allow creating a new ledger with the same bucket name after deletion", func(specContext SpecContext) {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: ledgerName + "-new",
				V2CreateLedgerRequest: components.V2CreateLedgerRequest{
					Bucket: pointer.For(bucketName),
				},
			})
			Expect(err).ToNot(HaveOccurred(), "Should be able to create a new ledger in the same bucket name after physical deletion")
		})
	})
})
