//go:build it

package test_suite

import (
	"fmt"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"
	. "github.com/formancehq/go-libs/v3/testing/deferred/ginkgo"
	"github.com/formancehq/go-libs/v3/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/v3/testing/testservice"
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
	const (
		pageSize = int64(10)
		txCount  = 2 * pageSize
	)
	When(fmt.Sprintf("creating %d transactions", txCount), func() {
		var (
			timestamp = time.Now().Round(time.Second).UTC()
		)
		BeforeEach(func(specContext SpecContext) {
			for i := 0; i < int(txCount); i++ {
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
									Destination: fmt.Sprintf("account:%d", i),
								},
							},
							Timestamp: &timestamp,
						},
						Ledger: "default",
					},
				)
				Expect(err).ToNot(HaveOccurred())
			}
		})
		When("Listing balances using v1 endpoint", func() {
			var (
				rsp *operations.GetBalancesResponse
			)
			BeforeEach(func(specContext SpecContext) {
				testServer, err := testServer.Wait(specContext)
				Expect(err).ToNot(HaveOccurred())

				rsp, err = Client(testServer).Ledger.V1.GetBalances(
					ctx,
					operations.GetBalancesRequest{
						Ledger:  "default",
						Address: pointer.For("world"),
					},
				)
				Expect(err).ToNot(HaveOccurred())
			})
			It("Should be return non empty balances", func() {
				Expect(rsp.BalancesCursorResponse.Cursor.Data).To(HaveLen(1))
				balances := rsp.BalancesCursorResponse.Cursor.Data[0]
				Expect(balances).To(HaveKey("world"))
				Expect(balances["world"]).To(HaveKey("USD"))
				Expect(balances["world"]["USD"]).To(Equal(int64(-2000)))
			})
		})
	})
})
