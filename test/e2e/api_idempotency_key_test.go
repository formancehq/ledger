
package test_suite

import (
	"math/big"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	. "github.com/formancehq/go-libs/v3/testing/api"
	"github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/natstesting"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Context("Idempotency key error handling tests", func() {
	var (
		db      = UseTemplatedDatabase()
		ctx     = logging.TestingContext()
		natsURL = ginkgo.DeferMap(natsServer, (*natstesting.NatsServer).ClientURL)
	)
	instruments := []testservice.Instrumentation{
		testservice.NatsInstrumentation(natsURL),
		testservice.DebugInstrumentation(debug),
		testservice.OutputInstrumentation(GinkgoWriter),
	}
	testServer := DeferTestServer(
		ginkgo.DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(instruments...),
		testservice.WithLogger(GinkgoT()),
	)

	BeforeEach(func(specContext SpecContext) {
		_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())
	})

	Context("when reusing an idempotency key with a different request body", func() {
		var (
			err       error
			timestamp = time.Now().Round(time.Second).UTC()
		)

		It("should return a 400 Bad Request error", func(specContext SpecContext) {
			idempKey := "test-idempotency-key-tx"
			firstReq := operations.V2CreateTransactionRequest{
				IdempotencyKey: pointer.For(idempKey),
				V2PostTransaction: components.V2PostTransaction{
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
			}

			firstResp, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, firstReq)
			Expect(err).To(BeNil())
			Expect(firstResp.V2CreateTransactionResponse.Data.ID).To(Equal(big.NewInt(1)))

			secondReq := operations.V2CreateTransactionRequest{
				IdempotencyKey: pointer.For(idempKey),
				V2PostTransaction: components.V2PostTransaction{
					Postings: []components.V2Posting{
						{
							Amount:      big.NewInt(200), // Different amount
							Asset:       "USD",
							Source:      "world",
							Destination: "alice",
						},
					},
					Timestamp: &timestamp,
				},
				Ledger: "default",
			}

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, secondReq)
			Expect(err).To(HaveOccurred())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
		})

		It("should return a 400 Bad Request when adding metadata with reused idempotency key", func(specContext SpecContext) {
			idempKey := "test-idempotency-key-meta"
			createReq := operations.V2CreateTransactionRequest{
				V2PostTransaction: components.V2PostTransaction{
					Postings: []components.V2Posting{
						{
							Amount:      big.NewInt(100),
							Asset:       "USD",
							Source:      "world",
							Destination: "alice",
						},
					},
				},
				Ledger: "default",
			}

			createResp, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, createReq)
			Expect(err).To(BeNil())

			firstMetaReq := operations.V2AddMetadataToTransactionRequest{
				IdempotencyKey: pointer.For(idempKey),
				ID:             createResp.V2CreateTransactionResponse.Data.ID,
				RequestBody: map[string]string{
					"key1": "value1",
				},
				Ledger: "default",
			}

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToTransaction(ctx, firstMetaReq)
			Expect(err).To(BeNil())

			secondMetaReq := operations.V2AddMetadataToTransactionRequest{
				IdempotencyKey: pointer.For(idempKey),
				ID:             createResp.V2CreateTransactionResponse.Data.ID,
				RequestBody: map[string]string{
					"key1": "different-value", // Different value
				},
				Ledger: "default",
			}

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.AddMetadataToTransaction(ctx, secondMetaReq)
			Expect(err).To(HaveOccurred())
			Expect(err).To(HaveErrorCode(string(components.V2ErrorsEnumValidation)))
		})
	})
})
