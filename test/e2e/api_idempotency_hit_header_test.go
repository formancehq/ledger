//go:build it

package test_suite

import (
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"

	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

var _ = Context("Idempotency-Hit header tests", func() {
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
				IdempotencyKey: pointer.For("tx-idempotency-key-header-1"),
			}
		})

		It("should not have Idempotency-Hit header on first call", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, txRequest)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).ToNot(BeNil())

			httpResponse := response.HTTPMeta.GetResponse()
			Expect(httpResponse).ToNot(BeNil())
			Expect(httpResponse.Header.Get("Idempotency-Hit")).To(BeEmpty())
		})

		It("should have Idempotency-Hit header on second call with same key", func(specContext SpecContext) {
			// First call
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, txRequest)
			Expect(err).ToNot(HaveOccurred())

			// Second call with same idempotency key
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, txRequest)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).ToNot(BeNil())

			httpResponse := response.HTTPMeta.GetResponse()
			Expect(httpResponse).ToNot(BeNil())
			Expect(httpResponse.Header.Get("Idempotency-Hit")).To(Equal("true"))
		})
	})

	When("adding transaction metadata with idempotency key", func() {
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

		It("should not have Idempotency-Hit header on first call", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(ctx, operations.V2AddMetadataOnTransactionRequest{
				RequestBody:    metadata,
				Ledger:         "default",
				ID:             tx.ID,
				IdempotencyKey: pointer.For("metadata-idempotency-key-header-1"),
			})
			Expect(err).To(Succeed())
			Expect(response).ToNot(BeNil())

			httpResponse := response.HTTPMeta.GetResponse()
			Expect(httpResponse).ToNot(BeNil())
			Expect(httpResponse.Header.Get("Idempotency-Hit")).To(BeEmpty())
		})

		It("should have Idempotency-Hit header on second call with same key", func(specContext SpecContext) {
			// First call
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(ctx, operations.V2AddMetadataOnTransactionRequest{
				RequestBody:    metadata,
				Ledger:         "default",
				ID:             tx.ID,
				IdempotencyKey: pointer.For("metadata-idempotency-key-header-2"),
			})
			Expect(err).To(Succeed())

			// Second call with same idempotency key
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(ctx, operations.V2AddMetadataOnTransactionRequest{
				RequestBody:    metadata,
				Ledger:         "default",
				ID:             tx.ID,
				IdempotencyKey: pointer.For("metadata-idempotency-key-header-2"),
			})
			Expect(err).To(Succeed())
			Expect(response).ToNot(BeNil())

			httpResponse := response.HTTPMeta.GetResponse()
			Expect(httpResponse).ToNot(BeNil())
			Expect(httpResponse.Header.Get("Idempotency-Hit")).To(Equal("true"))
		})
	})

	When("deleting transaction metadata with idempotency key", func() {
		var (
			timestamp = time.Now().Round(time.Second).UTC()
			tx        components.V2Transaction
		)

		BeforeEach(func(specContext SpecContext) {
			// Create a transaction first
			createResponse, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
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
			tx = createResponse.V2CreateTransactionResponse.Data

			// Add metadata first
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataOnTransaction(ctx, operations.V2AddMetadataOnTransactionRequest{
				RequestBody: map[string]string{
					"foo": "bar",
				},
				Ledger: "default",
				ID:     tx.ID,
			})
			Expect(err).To(Succeed())
		})

		It("should not have Idempotency-Hit header on first call", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteTransactionMetadata(
				ctx,
				operations.V2DeleteTransactionMetadataRequest{
					Ledger:         "default",
					ID:             tx.ID,
					Key:            "foo",
					IdempotencyKey: pointer.For("delete-metadata-idempotency-key-header-1"),
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).ToNot(BeNil())

			httpResponse := response.HTTPMeta.GetResponse()
			Expect(httpResponse).ToNot(BeNil())
			Expect(httpResponse.Header.Get("Idempotency-Hit")).To(BeEmpty())
		})

		It("should have Idempotency-Hit header on second call with same key", func(specContext SpecContext) {
			// First call
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteTransactionMetadata(
				ctx,
				operations.V2DeleteTransactionMetadataRequest{
					Ledger:         "default",
					ID:             tx.ID,
					Key:            "foo",
					IdempotencyKey: pointer.For("delete-metadata-idempotency-key-header-2"),
				},
			)
			Expect(err).ToNot(HaveOccurred())

			// Second call with same idempotency key
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteTransactionMetadata(
				ctx,
				operations.V2DeleteTransactionMetadataRequest{
					Ledger:         "default",
					ID:             tx.ID,
					Key:            "foo",
					IdempotencyKey: pointer.For("delete-metadata-idempotency-key-header-2"),
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).ToNot(BeNil())

			httpResponse := response.HTTPMeta.GetResponse()
			Expect(httpResponse).ToNot(BeNil())
			Expect(httpResponse.Header.Get("Idempotency-Hit")).To(Equal("true"))
		})
	})

	When("reverting transaction with idempotency key", func() {
		var (
			timestamp = time.Now().Round(time.Second).UTC()
		)

		It("should not have Idempotency-Hit header on first call", func(specContext SpecContext) {
			// Create a transaction first
			createResponse, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
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
			tx := createResponse.V2CreateTransactionResponse.Data

			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.RevertTransaction(
				ctx,
				operations.V2RevertTransactionRequest{
					Ledger:         "default",
					ID:             tx.ID,
					IdempotencyKey: pointer.For("revert-idempotency-key-header-1"),
				},
			)
			Expect(err).To(Succeed())
			Expect(response).ToNot(BeNil())

			httpResponse := response.HTTPMeta.GetResponse()
			Expect(httpResponse).ToNot(BeNil())
			Expect(httpResponse.Header.Get("Idempotency-Hit")).To(BeEmpty())
		})

		It("should have Idempotency-Hit header on second call with same key", func(specContext SpecContext) {
			// Create a transaction first
			createResponse, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
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
			tx := createResponse.V2CreateTransactionResponse.Data

			// First call
			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.RevertTransaction(
				ctx,
				operations.V2RevertTransactionRequest{
					Ledger:         "default",
					ID:             tx.ID,
					IdempotencyKey: pointer.For("revert-idempotency-key-header-2"),
				},
			)
			Expect(err).To(Succeed())

			// Second call with same idempotency key (should return the same revert transaction)
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.RevertTransaction(
				ctx,
				operations.V2RevertTransactionRequest{
					Ledger:         "default",
					ID:             tx.ID,
					IdempotencyKey: pointer.For("revert-idempotency-key-header-2"),
				},
			)
			Expect(err).To(Succeed())
			Expect(response).ToNot(BeNil())

			httpResponse := response.HTTPMeta.GetResponse()
			Expect(httpResponse).ToNot(BeNil())
			Expect(httpResponse.Header.Get("Idempotency-Hit")).To(Equal("true"))
		})
	})

	When("adding account metadata with idempotency key", func() {
		var (
			metadata = map[string]string{
				"type": "checking",
				"tier": "premium",
			}
		)

		It("should not have Idempotency-Hit header on first call", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
				ctx,
				operations.V2AddMetadataToAccountRequest{
					RequestBody:    metadata,
					Address:        "account-header-test-1",
					Ledger:         "default",
					IdempotencyKey: pointer.For("account-meta-key-header-1"),
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).ToNot(BeNil())

			httpResponse := response.HTTPMeta.GetResponse()
			Expect(httpResponse).ToNot(BeNil())
			Expect(httpResponse.Header.Get("Idempotency-Hit")).To(BeEmpty())
		})

		It("should have Idempotency-Hit header on second call with same key", func(specContext SpecContext) {
			// First call
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
				ctx,
				operations.V2AddMetadataToAccountRequest{
					RequestBody:    metadata,
					Address:        "account-header-test-2",
					Ledger:         "default",
					IdempotencyKey: pointer.For("account-meta-key-header-2"),
				},
			)
			Expect(err).ToNot(HaveOccurred())

			// Second call with same idempotency key
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
				ctx,
				operations.V2AddMetadataToAccountRequest{
					RequestBody:    metadata,
					Address:        "account-header-test-2",
					Ledger:         "default",
					IdempotencyKey: pointer.For("account-meta-key-header-2"),
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).ToNot(BeNil())

			httpResponse := response.HTTPMeta.GetResponse()
			Expect(httpResponse).ToNot(BeNil())
			Expect(httpResponse.Header.Get("Idempotency-Hit")).To(Equal("true"))
		})
	})

	When("deleting account metadata with idempotency key", func() {
		var (
			metadata = map[string]string{
				"type": "checking",
				"tier": "premium",
			}
		)

		BeforeEach(func(specContext SpecContext) {
			// Add metadata first
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToAccount(
				ctx,
				operations.V2AddMetadataToAccountRequest{
					RequestBody: metadata,
					Address:     "account-delete-header-test",
					Ledger:      "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
		})

		It("should not have Idempotency-Hit header on first call", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteAccountMetadata(
				ctx,
				operations.V2DeleteAccountMetadataRequest{
					Address:        "account-delete-header-test",
					Ledger:         "default",
					Key:            "type",
					IdempotencyKey: pointer.For("account-delete-key-header-1"),
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).ToNot(BeNil())

			httpResponse := response.HTTPMeta.GetResponse()
			Expect(httpResponse).ToNot(BeNil())
			Expect(httpResponse.Header.Get("Idempotency-Hit")).To(BeEmpty())
		})

		It("should have Idempotency-Hit header on second call with same key", func(specContext SpecContext) {
			// First call
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteAccountMetadata(
				ctx,
				operations.V2DeleteAccountMetadataRequest{
					Address:        "account-delete-header-test",
					Ledger:         "default",
					Key:            "type",
					IdempotencyKey: pointer.For("account-delete-key-header-2"),
				},
			)
			Expect(err).ToNot(HaveOccurred())

			// Second call with same idempotency key
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.DeleteAccountMetadata(
				ctx,
				operations.V2DeleteAccountMetadataRequest{
					Address:        "account-delete-header-test",
					Ledger:         "default",
					Key:            "type",
					IdempotencyKey: pointer.For("account-delete-key-header-2"),
				},
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).ToNot(BeNil())

			httpResponse := response.HTTPMeta.GetResponse()
			Expect(httpResponse).ToNot(BeNil())
			Expect(httpResponse.Header.Get("Idempotency-Hit")).To(Equal("true"))
		})
	})

	When("inserting schema with idempotency key", func() {
		var (
			insertSchemaRequest = operations.V2InsertSchemaRequest{
				V2SchemaData: components.V2SchemaData{
					Chart: map[string]components.V2ChartSegment{
						"bank": {},
					},
				},
				Version:        "v1.0.0",
				Ledger:         "default",
				IdempotencyKey: pointer.For("insert-schema-key-header-1"),
			}
		)
		It("should not have Idempotency-Hit header on first call", func(specContext SpecContext) {
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.InsertSchema(
				ctx,
				insertSchemaRequest,
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).ToNot(BeNil())

			httpResponse := response.HTTPMeta.GetResponse()
			Expect(httpResponse).ToNot(BeNil())
			Expect(httpResponse.Header.Get("Idempotency-Hit")).To(BeEmpty())
		})

		It("should have Idempotency-Hit header on second call with same key", func(specContext SpecContext) {
			// First call
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.InsertSchema(
				ctx,
				insertSchemaRequest,
			)
			Expect(err).ToNot(HaveOccurred())

			// Second call with same idempotency key
			response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.InsertSchema(
				ctx,
				insertSchemaRequest,
			)
			Expect(err).ToNot(HaveOccurred())
			Expect(response).ToNot(BeNil())

			httpResponse := response.HTTPMeta.GetResponse()
			Expect(httpResponse).ToNot(BeNil())
			Expect(httpResponse.Header.Get("Idempotency-Hit")).To(Equal("true"))
		})
	})
})
