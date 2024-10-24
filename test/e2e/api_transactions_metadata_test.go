//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/v2/logging"
	. "github.com/formancehq/go-libs/v2/testing/api"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger accounts list API tests", func() {
	var (
		db  = UseTemplatedDatabase()
		ctx = logging.TestingContext()
	)

	testServer := NewTestServer(func() Configuration {
		return Configuration{
			PostgresConfiguration: db.GetValue().ConnectionOptions(),
			Output:                GinkgoWriter,
			Debug:                 debug,
			NatsURL:               natsServer.GetValue().ClientURL(),
		}
	})
	BeforeEach(func() {
		err := CreateLedger(ctx, testServer.GetValue(), operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())
	})
	When("creating a transaction on a ledger", func() {
		var (
			timestamp = time.Now().Round(time.Second).UTC()
			rsp       *components.V2Transaction
			err       error
		)
		BeforeEach(func() {
			// Create a transaction
			rsp, err = CreateTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Metadata: map[string]string{},
						Postings: []components.V2Posting{
							{
								Amount:      big.NewInt(100),
								Asset:       "USD",
								Source:      "world",
								Destination: "alice",
							},
						},
						Timestamp: &timestamp,
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())

			// Check existence on api
			_, err := GetTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2GetTransactionRequest{
					Ledger: "default",
					ID:     rsp.ID,
				},
			)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should fail if the transaction does not exist", func() {
			metadata := map[string]string{
				"foo": "bar",
			}

			err := AddMetadataToTransaction(
				ctx,
				testServer.GetValue(),
				operations.V2AddMetadataOnTransactionRequest{
					RequestBody: metadata,
					Ledger:      "default",
					ID:          big.NewInt(666),
				},
			)
			Expect(err).To(HaveOccurred())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumNotFound)))
		})
		When("adding a metadata", func() {
			metadata := map[string]string{
				"foo": "bar",
			}
			BeforeEach(func() {
				err := AddMetadataToTransaction(
					ctx,
					testServer.GetValue(),
					operations.V2AddMetadataOnTransactionRequest{
						RequestBody: metadata,
						Ledger:      "default",
						ID:          rsp.ID,
					},
				)
				Expect(err).To(Succeed())
			})
			It("should be available on api", func() {
				response, err := GetTransaction(
					ctx,
					testServer.GetValue(),
					operations.V2GetTransactionRequest{
						Ledger: "default",
						ID:     rsp.ID,
					},
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(response.Metadata).Should(Equal(metadata))
			})
		})
	})
})
