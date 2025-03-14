//go:build it

package test_suite

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Buckets List API", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = context.Background()
	)

	testServer := testserver.NewTestServer(func() testserver.Configuration {
		return testserver.Configuration{
			CommonConfiguration: testserver.CommonConfiguration{
				PostgresConfiguration: db.GetValue().ConnectionOptions(),
				Output:                GinkgoWriter,
				Debug:                 debug,
			},
		}
	})

	BeforeEach(func() {
	})

	It("should list buckets with their associated ledgers", func() {
		// Generate unique names to avoid conflicts
		timestamp := time.Now().UnixNano()
		bucketName := fmt.Sprintf("test-bucket-%d", timestamp)
		ledgerName1 := fmt.Sprintf("test-ledger-1-%d", timestamp)
		ledgerName2 := fmt.Sprintf("test-ledger-2-%d", timestamp)

		// Create ledgers in the bucket
		err := testserver.CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
			Ledger: ledgerName1,
			V2CreateLedgerRequest: &components.V2CreateLedgerRequest{
				Bucket: pointer.For(bucketName),
			},
		})
		Expect(err).To(BeNil())

		err = testserver.CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
			Ledger: ledgerName2,
			V2CreateLedgerRequest: &components.V2CreateLedgerRequest{
				Bucket: pointer.For(bucketName),
			},
		})
		Expect(err).To(BeNil())

		// Create a ledger in a different bucket
		otherBucketName := fmt.Sprintf("test-bucket-other-%d", timestamp)
		otherLedgerName := fmt.Sprintf("test-ledger-other-%d", timestamp)
		err = testserver.CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
			Ledger: otherLedgerName,
			V2CreateLedgerRequest: &components.V2CreateLedgerRequest{
				Bucket: pointer.For(otherBucketName),
			},
		})
		Expect(err).To(BeNil())

		// Call the list buckets endpoint
		buckets, err := testserver.ListBuckets(ctx, testServer.GetValue())
		Expect(err).To(BeNil())

		// Verify that our test buckets are in the response
		var foundTestBucket, foundOtherBucket bool
		var testBucketInfo, otherBucketInfo operations.Data

		for _, bucket := range buckets.Object.Data.Data {
			if bucket.Name != nil && *bucket.Name == bucketName {
				foundTestBucket = true
				testBucketInfo = bucket
			}
			if bucket.Name != nil && *bucket.Name == otherBucketName {
				foundOtherBucket = true
				otherBucketInfo = bucket
			}
		}

		Expect(foundTestBucket).To(BeTrue(), "Test bucket should be in the response")
		Expect(foundOtherBucket).To(BeTrue(), "Other bucket should be in the response")

		// Verify the ledgers in the test bucket
		Expect(testBucketInfo.Ledgers).To(ContainElement(ledgerName1))
		Expect(testBucketInfo.Ledgers).To(ContainElement(ledgerName2))
		Expect(testBucketInfo.MarkForDeletion).NotTo(BeNil())
		Expect(*testBucketInfo.MarkForDeletion).To(BeFalse())

		// Verify the ledger in the other bucket
		Expect(otherBucketInfo.Ledgers).To(ContainElement(otherLedgerName))
		Expect(otherBucketInfo.MarkForDeletion).NotTo(BeNil())
		Expect(*otherBucketInfo.MarkForDeletion).To(BeFalse())

		// Now mark the test bucket as deleted
		err = testserver.DeleteBucket(ctx, testServer.GetValue(), operations.V2DeleteBucketRequest{
			Name: bucketName,
		})
		Expect(err).To(BeNil())

		// Call the list buckets endpoint again
		buckets, err = testserver.ListBuckets(ctx, testServer.GetValue())
		Expect(err).To(BeNil())

		// Find the test bucket again
		foundTestBucket = false
		for _, bucket := range buckets.Object.Data.Data {
			if bucket.Name != nil && *bucket.Name == bucketName {
				foundTestBucket = true
				testBucketInfo = bucket
				break
			}
		}

		Expect(foundTestBucket).To(BeTrue(), "Test bucket should still be in the response after deletion")
		Expect(testBucketInfo.MarkForDeletion).NotTo(BeNil(), "Test bucket should have MarkForDeletion field")
		Expect(*testBucketInfo.MarkForDeletion).To(BeTrue(), "Test bucket should be marked for deletion")
	})
})
