//go:build it

package test_suite

import (
	"math/big"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/api"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Ledger accounts list API tests", func() {
	var (
		db      = UseTemplatedDatabase()
		ctx     = logging.TestingContext()
		natsURL = DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)
	)

	testServer := ginkgo.DeferTestServer(
		DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(
			testservice.NatsInstrumentation(natsURL),
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
		),
		testservice.WithLogger(GinkgoT()),
	)

	BeforeEach(func(specContext SpecContext) {
		_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())
	})
	When("creating a transaction on a ledger", func() {
		var (
			timestamp = time.Now().Round(time.Second).UTC()
			txID      *big.Int
		)
		BeforeEach(func(specContext SpecContext) {
			// Create a transaction
			createResponse, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(
				ctx,
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
			txID = createResponse.V2CreateTransactionResponse.Data.ID

			// Check existence on api
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.GetTransaction(
				ctx,
				operations.V2GetTransactionRequest{
					Ledger: "default",
					ID:     txID,
				},
			)
			Expect(err).ToNot(HaveOccurred())
		})
		It("should fail if the transaction does not exist", func(specContext SpecContext) {
			metadata := map[string]string{
				"foo": "bar",
			}

			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(
				ctx,
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
			BeforeEach(func(specContext SpecContext) {
				_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(
					ctx,
					operations.V2AddMetadataOnTransactionRequest{
						RequestBody: metadata,
						Ledger:      "default",
						ID:          txID,
					},
				)
				Expect(err).To(Succeed())
			})
			It("should be available on api", func(specContext SpecContext) {
				response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetTransaction(
					ctx,
					operations.V2GetTransactionRequest{
						Ledger: "default",
						ID:     txID,
					},
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(response.V2GetTransactionResponse.Data.Metadata).Should(Equal(metadata))
			})
			When("deleting a metadata with idempotency key", func() {
				It("should succeed on first call and be idempotent on second call", func(specContext SpecContext) {
					ikPtr := pointer.For("delete-key-1")

					// First call to delete metadata with idempotency key
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteTransactionMetadata(
						ctx,
						operations.V2DeleteTransactionMetadataRequest{
							Ledger:         "default",
							ID:             txID,
							Key:            "foo",
							IdempotencyKey: ikPtr,
						},
					)
					Expect(err).ToNot(HaveOccurred())

					// Verify metadata was deleted
					response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetTransaction(
						ctx,
						operations.V2GetTransactionRequest{
							Ledger: "default",
							ID:     txID,
						},
					)
					Expect(err).ToNot(HaveOccurred())
					Expect(response.V2GetTransactionResponse.Data.Metadata).Should(BeEmpty())

					// Second call with same idempotency key should succeed
					_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteTransactionMetadata(
						ctx,
						operations.V2DeleteTransactionMetadataRequest{
							Ledger:         "default",
							ID:             txID,
							Key:            "foo",
							IdempotencyKey: ikPtr,
						},
					)
					Expect(err).ToNot(HaveOccurred())
				})

				It("should fail when using the same idempotency key with different parameters", func(specContext SpecContext) {
					// Add two metadata entries
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(
						ctx,
						operations.V2AddMetadataOnTransactionRequest{
							RequestBody: map[string]string{
								"foo": "bar",
								"baz": "qux",
							},
							Ledger: "default",
							ID:     txID,
						},
					)
					Expect(err).To(Succeed())

					ikPtr := pointer.For("delete-key-2")

					// First call to delete "foo" with idempotency key
					_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteTransactionMetadata(
						ctx,
						operations.V2DeleteTransactionMetadataRequest{
							Ledger:         "default",
							ID:             txID,
							Key:            "foo",
							IdempotencyKey: ikPtr,
						},
					)
					Expect(err).ToNot(HaveOccurred())

					// Second call with same idempotency key but different key to delete
					_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteTransactionMetadata(
						ctx,
						operations.V2DeleteTransactionMetadataRequest{
							Ledger:         "default",
							ID:             txID,
							Key:            "baz", // Different key
							IdempotencyKey: ikPtr,
						},
					)
					Expect(err).To(HaveOccurred())
				})
			})

			When("using the same idempotency key with same data", func() {
				It("should succeed and return the same result", func(specContext SpecContext) {
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(
						ctx,
						operations.V2AddMetadataOnTransactionRequest{
							RequestBody:    metadata,
							Ledger:         "default",
							ID:             txID,
							IdempotencyKey: pointer.For("test-idempotency-key"),
						},
					)
					Expect(err).To(Succeed())

					// Second call with same idempotency key and same data
					_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(
						ctx,
						operations.V2AddMetadataOnTransactionRequest{
							RequestBody:    metadata,
							Ledger:         "default",
							ID:             txID,
							IdempotencyKey: pointer.For("test-idempotency-key"),
						},
					)
					Expect(err).To(Succeed())
				})
			})

			When("using the same idempotency key with different data", func() {
				It("should fail with ValidationError", func(specContext SpecContext) {
					_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(
						ctx,
						operations.V2AddMetadataOnTransactionRequest{
							RequestBody:    metadata,
							Ledger:         "default",
							ID:             txID,
							IdempotencyKey: pointer.For("test-idempotency-key-2"),
						},
					)
					Expect(err).To(Succeed())

					// Second call with same idempotency key but different data
					_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(
						ctx,
						operations.V2AddMetadataOnTransactionRequest{
							RequestBody: map[string]string{
								"foo": "different-value",
							},
							Ledger:         "default",
							ID:             txID,
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
