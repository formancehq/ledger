//go:build it

package test_suite

import (
	"math/big"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

var _ = Context("Ledger transactions list timezone handling", func() {
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

	JustBeforeEach(func(specContext SpecContext) {
		_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())
	})

	When("a transaction exists at 2023-04-11T12:00:00Z", func() {
		// txTime is unambiguously UTC noon.
		txTime := time.Date(2023, 4, 11, 12, 0, 0, 0, time.UTC)

		JustBeforeEach(func(specContext SpecContext) {
			_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(
				ctx,
				operations.V2CreateTransactionRequest{
					V2PostTransaction: components.V2PostTransaction{
						Metadata: map[string]string{},
						Postings: []components.V2Posting{
							{
								Amount:      big.NewInt(100),
								Asset:       "USD",
								Source:      "world",
								Destination: "account:0",
							},
						},
						Timestamp: pointer.For(txTime),
					},
					Ledger: "default",
				},
			)
			Expect(err).ToNot(HaveOccurred())
		})

		// 2023-04-11T14:00:00+04:00 is the SAME instant as 2023-04-11T10:00:00Z,
		// which is BEFORE the transaction (12:00:00Z). A correct $lt filter must
		// therefore match 0 transactions, exactly like the equivalent Z form.
		It("interprets a +04:00 offset the same as the equivalent Z time", func(specContext SpecContext) {
			count := func(ts string) string {
				response, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CountTransactions(
					ctx,
					operations.V2CountTransactionsRequest{
						Ledger: "default",
						RequestBody: map[string]any{
							"$lt": map[string]any{
								"timestamp": ts,
							},
						},
					},
				)
				Expect(err).ToNot(HaveOccurred())
				return response.Headers["Count"][0]
			}

			withOffset := count("2023-04-11T14:00:00+04:00") // == 2023-04-11T10:00:00Z
			withUTC := count("2023-04-11T10:00:00Z")

			By("the two equivalent instants must yield the same count")
			Expect(withOffset).To(Equal(withUTC),
				"offset form returned %q but equivalent Z form returned %q", withOffset, withUTC)

			By("both are before the transaction, so the count must be 0")
			Expect(withUTC).To(Equal("0"))
			Expect(withOffset).To(Equal("0"))
		})
	})
})
