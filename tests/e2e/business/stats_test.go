//go:build e2e

package business

import (
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("GetLedgerStats", Ordered, func() {

	Context("When getting stats for an empty ledger", Ordered, func() {
		var ledgerName = "stats-empty"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should return zero counts", func() {
			resp, err := sharedClient.GetLedgerStats(sharedCtx, &servicepb.GetLedgerStatsRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp.AccountCount).To(BeZero())
			Expect(resp.TransactionCount).To(BeZero())
		})
	})

	Context("When getting stats for a ledger with transactions", Ordered, func() {
		var ledgerName = "stats-with-data"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create 3 transactions producing 4 accounts: world, bank:main, bank:fees, users:alice
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("world", "bank:main", big.NewInt(1000), "USD"),
					}, nil, nil),
					testutil.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("bank:main", "bank:fees", big.NewInt(10), "USD"),
					}, nil, nil),
					testutil.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						testutil.NewPosting("bank:main", "users:alice", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return correct account and transaction counts", func() {
			// Index builder processes logs asynchronously; poll until indexes are up to date.
			Eventually(func(g Gomega) {
				resp, err := sharedClient.GetLedgerStats(sharedCtx, &servicepb.GetLedgerStatsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				// world + bank:main + bank:fees + users:alice = 4
				g.Expect(resp.AccountCount).To(Equal(uint64(4)))
				// 3 transactions
				g.Expect(resp.TransactionCount).To(Equal(uint64(3)))
			}).Should(Succeed())
		})
	})

	Context("When getting stats for a non-existent ledger", func() {
		It("Should return a NotFound error", func() {
			_, err := sharedClient.GetLedgerStats(sharedCtx, &servicepb.GetLedgerStatsRequest{
				Ledger: "non-existent-ledger",
			})
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})
	})
})
