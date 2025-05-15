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

var _ = Context("Ledger transactions metadata API tests", func() {
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

			When("deleting a metadata with idempotency key", func() {
				It("should succeed on first call and be idempotent on second call", func() {
					ikPtr := pointer.For("delete-key-1")

					// First call to delete metadata with idempotency key
					_, err := testServer.GetValue().Client().Ledger.V2.DeleteTransactionMetadata(
						ctx,
						operations.V2DeleteTransactionMetadataRequest{
							Ledger:         "default",
							ID:             rsp.ID,
							Key:            "foo",
							IdempotencyKey: ikPtr,
						},
					)
					Expect(err).ToNot(HaveOccurred())

					// Verify metadata was deleted
					response, err := GetTransaction(
						ctx,
						testServer.GetValue(),
						operations.V2GetTransactionRequest{
							Ledger: "default",
							ID:     rsp.ID,
						},
					)
					Expect(err).ToNot(HaveOccurred())
					Expect(response.Metadata).Should(BeEmpty())

					// Second call with same idempotency key should succeed
					_, err = testServer.GetValue().Client().Ledger.V2.DeleteTransactionMetadata(
						ctx,
						operations.V2DeleteTransactionMetadataRequest{
							Ledger:         "default",
							ID:             rsp.ID,
							Key:            "foo",
							IdempotencyKey: ikPtr,
						},
					)
					Expect(err).ToNot(HaveOccurred())
				})

				It("should fail when using the same idempotency key with different parameters", func() {
					// Add two metadata entries
					err := AddMetadataToTransaction(
						ctx,
						testServer.GetValue(),
						operations.V2AddMetadataOnTransactionRequest{
							RequestBody: map[string]string{
								"foo": "bar",
								"baz": "qux",
							},
							Ledger: "default",
							ID:     rsp.ID,
						},
					)
					Expect(err).To(Succeed())

					ikPtr := pointer.For("delete-key-2")

					// First call to delete "foo" with idempotency key
					_, err = testServer.GetValue().Client().Ledger.V2.DeleteTransactionMetadata(
						ctx,
						operations.V2DeleteTransactionMetadataRequest{
							Ledger:         "default",
							ID:             rsp.ID,
							Key:            "foo",
							IdempotencyKey: ikPtr,
						},
					)
					Expect(err).ToNot(HaveOccurred())

					// Second call with same idempotency key but different key to delete
					_, err = testServer.GetValue().Client().Ledger.V2.DeleteTransactionMetadata(
						ctx,
						operations.V2DeleteTransactionMetadataRequest{
							Ledger:         "default",
							ID:             rsp.ID,
							Key:            "baz", // Different key
							IdempotencyKey: ikPtr,
						},
					)
					Expect(err).To(HaveOccurred())
				})
			})

			When("using the same idempotency key with same data", func() {
				It("should succeed and return the same result", func() {
					err := AddMetadataToTransaction(
						ctx,
						testServer.GetValue(),
						operations.V2AddMetadataOnTransactionRequest{
							RequestBody:    metadata,
							Ledger:         "default",
							ID:             rsp.ID,
							IdempotencyKey: pointer.For("test-idempotency-key"),
						},
					)
					Expect(err).To(Succeed())

					// Second call with same idempotency key and same data
					err = AddMetadataToTransaction(
						ctx,
						testServer.GetValue(),
						operations.V2AddMetadataOnTransactionRequest{
							RequestBody:    metadata,
							Ledger:         "default",
							ID:             rsp.ID,
							IdempotencyKey: pointer.For("test-idempotency-key"),
						},
					)
					Expect(err).To(Succeed())
				})
			})

			When("using the same idempotency key with different data", func() {
				It("should fail with ValidationError", func() {
					err := AddMetadataToTransaction(
						ctx,
						testServer.GetValue(),
						operations.V2AddMetadataOnTransactionRequest{
							RequestBody:    metadata,
							Ledger:         "default",
							ID:             rsp.ID,
							IdempotencyKey: pointer.For("test-idempotency-key-2"),
						},
					)
					Expect(err).To(Succeed())

					// Second call with same idempotency key but different data
					err = AddMetadataToTransaction(
						ctx,
						testServer.GetValue(),
						operations.V2AddMetadataOnTransactionRequest{
							RequestBody: map[string]string{
								"foo": "different-value",
							},
							Ledger:         "default",
							ID:             rsp.ID,
							IdempotencyKey: pointer.For("test-idempotency-key-2"),
						},
					)
					Expect(err).To(HaveOccurred())
					Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
				})
			})
		})
	})
})
