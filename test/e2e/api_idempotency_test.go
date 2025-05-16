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

var _ = Context("Idempotency key tests", func() {
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

	When("creating transactions with idempotency key", func() {
		var (
			timestamp = time.Now().Round(time.Second).UTC()
			tx        components.V2Transaction
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

		It("should succeed on the first call and be idempotent on subsequent calls", func(specContext SpecContext) {
			// First call
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, txRequest)
			Expect(err).ToNot(HaveOccurred())
			tx = response.V2CreateTransactionResponse.Data
			id := tx.ID

			// Second call with same idempotency key should return the same result
			response, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, txRequest)
			Expect(err).ToNot(HaveOccurred())
			tx = response.V2CreateTransactionResponse.Data
			Expect(tx.ID).To(Equal(id))
		})

		It("should fail when using the same idempotency key with different data", func(specContext SpecContext) {
			// First call
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, txRequest)
			Expect(err).ToNot(HaveOccurred())

			// Second call with same idempotency key but different data
			txRequest.V2PostTransaction.Postings[0].Amount = big.NewInt(200)
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, txRequest)
			Expect(err).To(HaveOccurred())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
		})
	})

	When("adding metadata with idempotency key", func() {
		var (
			timestamp = time.Now().Round(time.Second).UTC()
			tx        components.V2Transaction
			metadata  map[string]string
		)

		BeforeEach(func(specContext SpecContext) {
			// Create a transaction first
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
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
			tx = response.V2CreateTransactionResponse.Data

			metadata = map[string]string{
				"foo": "bar",
			}
		})

		It("should succeed on the first call and be idempotent on subsequent calls", func(specContext SpecContext) {
			// First call to add metadata with idempotency key
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(ctx, operations.V2AddMetadataOnTransactionRequest{
				RequestBody:    metadata,
				Ledger:         "default",
				ID:             tx.ID,
				IdempotencyKey: pointer.For("metadata-idempotency-key-1"),
			})
			Expect(err).To(Succeed())

			// Verify metadata was added
			getResponse, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetTransaction(ctx, operations.V2GetTransactionRequest{
				Ledger: "default",
				ID:     tx.ID,
			})
			Expect(err).ToNot(HaveOccurred())
			Expect(getResponse.V2GetTransactionResponse.Data.Metadata).Should(Equal(metadata))

			// Second call with same idempotency key should succeed
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(ctx, operations.V2AddMetadataOnTransactionRequest{
				RequestBody:    metadata,
				Ledger:         "default",
				ID:             tx.ID,
				IdempotencyKey: pointer.For("metadata-idempotency-key-1"),
			})
			Expect(err).To(Succeed())
		})

		It("should fail when using the same idempotency key with different data", func(specContext SpecContext) {
			// First call
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(ctx, operations.V2AddMetadataOnTransactionRequest{
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
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(ctx, operations.V2AddMetadataOnTransactionRequest{
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
