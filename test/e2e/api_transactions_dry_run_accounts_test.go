//go:build it

package test_suite

import (
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	. "github.com/formancehq/go-libs/v5/pkg/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v5/pkg/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"

	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	. "github.com/formancehq/ledger/pkg/testserver"
	. "github.com/formancehq/ledger/pkg/testserver/ginkgo"
)

// A transaction writes its accounts through the operation's store. When that
// store was not the transaction's, the accounts were autocommitted: they
// survived an operation that never committed, and because the upsert lowers
// first_usage to the transaction's effective date (LEAST over the involved
// accounts), an existing account's first_usage was pulled back permanently.
//
// A dry run is the observable form of "the operation never commits": the whole
// transaction is rolled back by design, so nothing it wrote may remain. The
// ledger must be warm — the first write to a ledger runs under a
// controller-level transaction that hides the defect.
var _ = Context("Ledger transactions dry run account persistence", func() {
	var (
		db         = UseTemplatedDatabase()
		ctx        = logging.TestingContext()
		testServer = DeferTestServer(
			DeferMap(db, (*pgtesting.Database).ConnectionOptions),
			testservice.WithInstruments(
				testservice.DebugInstrumentation(debug),
				testservice.OutputInstrumentation(GinkgoWriter),
			),
			testservice.WithLogger(GinkgoT()),
		)
		backdated = time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	)

	BeforeEach(func(specContext SpecContext) {
		_, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
			Ledger: "default",
		})
		Expect(err).To(BeNil())

		// Warm the ledger: the very first write is wrapped in a controller-level
		// transaction, which would mask the defect.
		_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
			Ledger: "default",
			V2PostTransaction: components.V2PostTransaction{
				Postings: []components.V2Posting{{
					Source: "world", Destination: "seed", Amount: big.NewInt(10), Asset: "USD",
				}},
			},
		})
		Expect(err).To(BeNil())
	})

	When("dry running a backdated transaction to a new account", func() {
		var worldBefore *time.Time

		BeforeEach(func(specContext SpecContext) {
			world, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
				Ledger: "default", Address: "world",
			})
			Expect(err).To(BeNil())
			worldBefore = world.V2AccountResponse.Data.FirstUsage

			_, err = Wait(specContext, DeferClient(testServer)).Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
				Ledger: "default",
				DryRun: pointer.For(true),
				V2PostTransaction: components.V2PostTransaction{
					Timestamp: &backdated,
					Postings: []components.V2Posting{{
						Source: "world", Destination: "ghost", Amount: big.NewInt(5), Asset: "USD",
					}},
				},
			})
			Expect(err).To(BeNil())
		})

		It("should not persist the transaction", func(specContext SpecContext) {
			txs, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListTransactions(ctx, operations.V2ListTransactionsRequest{
				Ledger: "default",
			})
			Expect(err).To(BeNil())
			Expect(txs.V2TransactionsCursorResponse.Cursor.Data).To(HaveLen(1))
		})

		It("should not create the account the dry run touched", func(specContext SpecContext) {
			accounts, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.ListAccounts(ctx, operations.V2ListAccountsRequest{
				Ledger: "default",
			})
			Expect(err).To(BeNil())

			addresses := make([]string, 0, len(accounts.V2AccountsCursorResponse.Cursor.Data))
			for _, account := range accounts.V2AccountsCursorResponse.Cursor.Data {
				addresses = append(addresses, account.Address)
			}
			Expect(addresses).ToNot(ContainElement("ghost"))
		})

		It("should not backdate first_usage of an existing account", func(specContext SpecContext) {
			world, err := Wait(specContext, DeferClient(testServer)).Ledger.V2.GetAccount(ctx, operations.V2GetAccountRequest{
				Ledger: "default", Address: "world",
			})
			Expect(err).To(BeNil())
			Expect(world.V2AccountResponse.Data.FirstUsage).To(Equal(worldBefore))
		})
	})
})
