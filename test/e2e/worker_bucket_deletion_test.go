
package test_suite

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/api"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
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

var _ = Context("Bucket deletion worker tests", func() {
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

	When("creating a bucket, marking it for deletion, and running the worker", func() {
		var (
			bucketName = "test-bucket-" + uuid.NewString()[:8]
			ledgerName = "test-ledger-" + uuid.NewString()[:8]
		)

		BeforeEach(func(specContext SpecContext) {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: ledgerName,
				V2CreateLedgerRequest: components.V2CreateLedgerRequest{
					Bucket: pointer.For(bucketName),
				},
			})
			Expect(err).ToNot(HaveOccurred())
		})

		It("should physically delete the bucket after it's marked for deletion", func(specContext SpecContext) {
			ledgerInfo, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetLedgerInfo(ctx, operations.V2GetLedgerInfoRequest{
				Ledger: ledgerName,
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(*ledgerInfo.V2LedgerInfoResponse.Data.Name).To(Equal(ledgerName))

			req, err := http.NewRequest("DELETE", testservice.GetServerURL(testServer.GetValue()).String()+"/v2/_/buckets/"+bucketName, nil)
			Expect(err).ToNot(HaveOccurred())
			resp, err := http.DefaultClient.Do(req)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusNoContent))
			resp.Body.Close()

			req, err = http.NewRequest("GET", testservice.GetServerURL(testServer.GetValue()).String()+"/v2/_/buckets", nil)
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
					Expect(bucket["deletedAt"]).NotTo(BeNil())
				}
			}
			Expect(foundBucket).To(BeTrue(), "Bucket should be found in buckets list with deleted status")

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.GetLedgerInfo(ctx, operations.V2GetLedgerInfoRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(HaveOccurred())
			Expect(err).To(HaveErrorCode("LEDGER_NOT_FOUND"))

			driver := testServer.GetValue().GetDriver()
			Expect(driver).NotTo(BeNil())

			logger := logging.GetLogger(ctx)
			schedule, err := cron.ParseStandard("* * * * * *") // Every second
			Expect(err).ToNot(HaveOccurred())

			gracePeriod := time.Nanosecond
			bucketDeletionRunner := worker.NewBucketDeletionRunner(
				logger,
				driver,
				worker.BucketDeletionRunnerConfig{
					Schedule:    schedule,
					GracePeriod: gracePeriod,
				},
			)

			err = bucketDeletionRunner.RunOnce(ctx)
			Expect(err).ToNot(HaveOccurred())

			req, err = http.NewRequest("GET", testservice.GetServerURL(testServer.GetValue()).String()+"/v2/_/buckets", nil)
			Expect(err).ToNot(HaveOccurred())
			resp, err = http.DefaultClient.Do(req)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			buckets = nil
			Expect(json.NewDecoder(resp.Body).Decode(&buckets)).To(Succeed())
			resp.Body.Close()

			foundBucket = false
			for _, bucket := range buckets {
				if bucket["name"] == bucketName {
					foundBucket = true
				}
			}
			Expect(foundBucket).To(BeFalse(), "Bucket should be completely deleted and not found in buckets list")

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: ledgerName + "-new",
				V2CreateLedgerRequest: components.V2CreateLedgerRequest{
					Bucket: pointer.For(bucketName),
				},
			})
			Expect(err).ToNot(HaveOccurred(), "Should be able to create a new ledger in the same bucket name after physical deletion")
		})
	})
})
