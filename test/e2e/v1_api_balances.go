//go:build it

package test_suite

import (
	"fmt"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/pointer"
	. "github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/formancehq/stack/ledger/client/models/operations"
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
	const (
		pageSize = int64(10)
		txCount  = 2 * pageSize
	)
	When(fmt.Sprintf("creating %d transactions", txCount), func() {
		var (
			timestamp = time.Now().Round(time.Second).UTC()
		)
		BeforeEach(func() {
			for i := 0; i < int(txCount); i++ {
				_, err := CreateTransaction(
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
				err error
			)
			BeforeEach(func() {
				rsp, err = testServer.GetValue().Client().Ledger.V1.GetBalances(
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
