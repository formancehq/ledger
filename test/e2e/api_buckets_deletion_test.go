//go:build it

package test_suite

import (
	"encoding/json"
	"net/http"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/api"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Bucket deletion API tests", func() {
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

	When("creating a bucket and marking it for deletion", func() {
		var (
			bucketName = "test-bucket-" + uuid.NewString()[:8]
			ledgerName = "test-ledger-" + uuid.NewString()[:8]
		)

		BeforeEach(func(specContext SpecContext) {
			// Create a ledger in the test bucket
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
				Ledger: ledgerName,
				V2CreateLedgerRequest: components.V2CreateLedgerRequest{
					Bucket: pointer.For(bucketName),
				},
			})
			Expect(err).ToNot(HaveOccurred())
		})

		It("should mark the bucket as deleted and make ledgers inaccessible", func(specContext SpecContext) {
			// First check that the ledger is accessible
			ledgerInfo, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetLedgerInfo(ctx, operations.V2GetLedgerInfoRequest{
				Ledger: ledgerName,
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(*ledgerInfo.V2LedgerInfoResponse.Data.Name).To(Equal(ledgerName))

			// List buckets and verify our bucket is there and not deleted
			req, err := http.NewRequest("GET", testservice.GetServerURL(testServer.GetValue()).String()+"/v2/_/buckets", nil)
			Expect(err).ToNot(HaveOccurred())
			resp, err := http.DefaultClient.Do(req)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusOK))

			var buckets []map[string]interface{}
			Expect(json.NewDecoder(resp.Body).Decode(&buckets)).To(Succeed())
			resp.Body.Close()

			var foundBucket bool
			for _, bucket := range buckets {
				if bucket["name"] == bucketName {
					foundBucket = true
					Expect(bucket["deletedAt"]).To(BeNil())
				}
			}
			Expect(foundBucket).To(BeTrue(), "Bucket should be found in buckets list")

			// Mark the bucket as deleted
			req, err = http.NewRequest("DELETE", testservice.GetServerURL(testServer.GetValue()).String()+"/v2/_/buckets/"+bucketName, nil)
			Expect(err).ToNot(HaveOccurred())
			resp, err = http.DefaultClient.Do(req)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusNoContent))
			resp.Body.Close()

			// List buckets again and verify our bucket is marked as deleted
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
					Expect(bucket["deletedAt"]).NotTo(BeNil())
				}
			}
			Expect(foundBucket).To(BeTrue(), "Bucket should be found in buckets list with deleted status")

			// Try to access the ledger, should fail with 404
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.GetLedgerInfo(ctx, operations.V2GetLedgerInfoRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(HaveOccurred())
			Expect(err).To(HaveErrorCode("LEDGER_NOT_FOUND"))

			// Restore the bucket
			req, err = http.NewRequest("POST", testservice.GetServerURL(testServer.GetValue()).String()+"/v2/_/buckets/"+bucketName+"/restore", nil)
			Expect(err).ToNot(HaveOccurred())
			resp, err = http.DefaultClient.Do(req)
			Expect(err).ToNot(HaveOccurred())
			Expect(resp.StatusCode).To(Equal(http.StatusNoContent))
			resp.Body.Close()

			// List buckets again and verify our bucket is no longer marked as deleted
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
					Expect(bucket["deletedAt"]).To(BeNil())
				}
			}
			Expect(foundBucket).To(BeTrue(), "Bucket should be found in buckets list with restored status")

			// Try to access the ledger again, should succeed
			ledgerInfo, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.GetLedgerInfo(ctx, operations.V2GetLedgerInfoRequest{
				Ledger: ledgerName,
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(*ledgerInfo.V2LedgerInfoResponse.Data.Name).To(Equal(ledgerName))
		})
	})
})
