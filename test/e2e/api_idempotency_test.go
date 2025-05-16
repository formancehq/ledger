//go:build it

package test_suite

import (
	"math/big"
	"time"

	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"
	. "github.com/formancehq/go-libs/v2/testing/api"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Idempotency key tests", func() {
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

	When("creating transactions with idempotency key", func() {
		var (
			timestamp = time.Now().Round(time.Second).UTC()
			tx        *components.V2Transaction
			err       error
			txRequest operations.V2CreateTransactionRequest
		)

		BeforeEach(func() {
			txRequest = operations.V2CreateTransactionRequest{
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
				Ledger:         "default",
				IdempotencyKey: pointer.For("tx-idempotency-key-1"),
			}
		})

		It("should succeed on the first call and be idempotent on subsequent calls", func() {
			// First call
			tx, err = CreateTransaction(ctx, testServer.GetValue(), txRequest)
			Expect(err).ToNot(HaveOccurred())
			id := tx.ID

			// Second call with same idempotency key should return the same result
			tx, err = CreateTransaction(ctx, testServer.GetValue(), txRequest)
			Expect(err).ToNot(HaveOccurred())
			Expect(tx.ID).To(Equal(id))
		})

		It("should fail when using the same idempotency key with different data", func() {
			// First call
			_, err = CreateTransaction(ctx, testServer.GetValue(), txRequest)
			Expect(err).ToNot(HaveOccurred())

			// Second call with same idempotency key but different data
			txRequest.V2PostTransaction.Postings[0].Amount = big.NewInt(200)
			_, err = CreateTransaction(ctx, testServer.GetValue(), txRequest)
			Expect(err).To(HaveOccurred())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
		})
	})

	When("adding metadata with idempotency key", func() {
		var (
			timestamp = time.Now().Round(time.Second).UTC()
			tx        *components.V2Transaction
			metadata  map[string]string
			err       error
		)

		BeforeEach(func() {
			// Create a transaction first
			tx, err = CreateTransaction(ctx, testServer.GetValue(), operations.V2CreateTransactionRequest{
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
			})
			Expect(err).ToNot(HaveOccurred())

			metadata = map[string]string{
				"foo": "bar",
			}
		})

		It("should succeed on the first call and be idempotent on subsequent calls", func() {
			// First call to add metadata with idempotency key
			err = AddMetadataToTransaction(ctx, testServer.GetValue(), operations.V2AddMetadataOnTransactionRequest{
				RequestBody:    metadata,
				Ledger:         "default",
				ID:             tx.ID,
				IdempotencyKey: pointer.For("metadata-idempotency-key-1"),
			})
			Expect(err).To(Succeed())

			// Verify metadata was added
			response, err := GetTransaction(ctx, testServer.GetValue(), operations.V2GetTransactionRequest{
				Ledger: "default",
				ID:     tx.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(response.Metadata).Should(Equal(metadata))

			// Second call with same idempotency key should succeed
			err = AddMetadataToTransaction(ctx, testServer.GetValue(), operations.V2AddMetadataOnTransactionRequest{
				RequestBody:    metadata,
				Ledger:         "default",
				ID:             tx.ID,
				IdempotencyKey: pointer.For("metadata-idempotency-key-1"),
			})
			Expect(err).To(Succeed())
		})

		It("should fail when using the same idempotency key with different data", func() {
			// First call
			err = AddMetadataToTransaction(ctx, testServer.GetValue(), operations.V2AddMetadataOnTransactionRequest{
				RequestBody:    metadata,
				Ledger:         "default",
				ID:             tx.ID,
				IdempotencyKey: pointer.For("metadata-idempotency-key-2"),
			})
			Expect(err).To(Succeed())

			// Second call with same idempotency key but different metadata
			differentMetadata := map[string]string{
				"foo": "baz",
			}
			err = AddMetadataToTransaction(ctx, testServer.GetValue(), operations.V2AddMetadataOnTransactionRequest{
				RequestBody:    differentMetadata,
				Ledger:         "default",
				ID:             tx.ID,
				IdempotencyKey: pointer.For("metadata-idempotency-key-2"),
			})
			Expect(err).To(HaveOccurred())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
		})
	})
})
