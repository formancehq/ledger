//go:build it

package test_suite

import (
	"fmt"
	"net/http"
	"time"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Bucket management", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	testServer := NewTestServer(func() Configuration {
		return Configuration{
			CommonConfiguration: CommonConfiguration{
				PostgresConfiguration: db.GetValue().ConnectionOptions(),
				Output:                GinkgoWriter,
				Debug:                 debug,
			},
			NatsURL:              natsServer.GetValue().ClientURL(),
			ExperimentalFeatures: true,
		}
	})

	It("should delete a bucket and mark all its ledgers as deleted", func() {
		// Generate unique names to avoid conflicts
		timestamp := time.Now().UnixNano()
		bucketName := fmt.Sprintf("test-bucket-%d", timestamp)
		ledgerName := fmt.Sprintf("test-ledger-%d", timestamp)

		// Create a ledger in the bucket
		err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
			Ledger: ledgerName,
			V2CreateLedgerRequest: &components.V2CreateLedgerRequest{
				Bucket: pointer.For(bucketName),
			},
		})
		Expect(err).To(BeNil())

		// Verify the ledger was created
		ledger, err := GetLedger(ctx, testServer.GetValue(), operations.V2GetLedgerRequest{
			Ledger: ledgerName,
		})
		Expect(err).To(BeNil())
		Expect(ledger.Bucket).To(Equal(bucketName))

		// Create a direct HTTP request to delete the bucket
		url := fmt.Sprintf("%s/v2/_system/bucket?name=%s", testServer.GetValue().URL(), bucketName)
		req, err := http.NewRequest(http.MethodDelete, url, nil)
		Expect(err).To(BeNil())

		// Execute the request
		client := &http.Client{}
		resp, err := client.Do(req)
		Expect(err).To(BeNil())
		defer resp.Body.Close()

		// Check the response status code
		Expect(resp.StatusCode).To(Equal(http.StatusNoContent))
	})
})
