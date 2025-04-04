//go:build it

package test_suite

import (
	"github.com/formancehq/go-libs/v2/logging"
	. "github.com/formancehq/go-libs/v2/testing/api"
	. "github.com/formancehq/go-libs/v2/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v2/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v2/testing/testservice"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
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

	testServer := ginkgo.DeferTestServer(
		DeferMap(db, (*pgtesting.Database).ConnectionOptions),
		testservice.WithInstruments(
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
			rsp       *operations.V2CreateTransactionResponse
			err       error
		)
		BeforeEach(func(specContext SpecContext) {
			// Create a transaction
			rsp, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(
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

			// Check existence on api
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetTransaction(
				ctx,
				operations.V2GetTransactionRequest{
					Ledger: "default",
					ID:     rsp.V2CreateTransactionResponse.Data.ID,
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
						ID:          rsp.V2CreateTransactionResponse.Data.ID,
					},
				)
				Expect(err).To(Succeed())
			})
			It("should be available on api", func(specContext SpecContext) {
				response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetTransaction(
					ctx,
					operations.V2GetTransactionRequest{
						Ledger: "default",
						ID:     rsp.V2CreateTransactionResponse.Data.ID,
					},
				)
				Expect(err).ToNot(HaveOccurred())
				Expect(response.V2GetTransactionResponse.Data.Metadata).Should(Equal(metadata))
			})
		})
	})
})
