//go:build e2e

package business

import (
	"github.com/formancehq/ledger/v3/pkg/actions"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should return zero counts", func() {
			resp, err := sharedClient.GetLedgerStats(sharedCtx, &servicepb.GetLedgerStatsRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(resp.TransactionCount).To(BeZero())
			Expect(resp.PostingCount).To(BeZero())
			Expect(resp.VolumeCount).To(BeZero())
			Expect(resp.MetadataCount).To(BeZero())
			Expect(resp.ReferenceCount).To(BeZero())
		})
	})

	Context("When getting stats for a ledger with transactions", Ordered, func() {
		var ledgerName = "stats-with-data"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create 3 transactions producing 4 accounts: world, bank:main, bank:fees, users:alice
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bank:main", big.NewInt(1000), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("bank:main", "bank:fees", big.NewInt(10), "USD"),
					}, nil, nil),
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("bank:main", "users:alice", big.NewInt(100), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
		})

		It("Should return correct counts", func() {
			// Index builder processes logs asynchronously; poll until indexes are up to date.
			Eventually(func(g Gomega) {
				resp, err := sharedClient.GetLedgerStats(sharedCtx, &servicepb.GetLedgerStatsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				// 3 transactions, each with 1 posting = 3 postings
				g.Expect(resp.TransactionCount).To(Equal(uint64(3)))
				g.Expect(resp.PostingCount).To(Equal(uint64(3)))
				// world/USD + bank:main/USD + bank:fees/USD + users:alice/USD = 4
				g.Expect(resp.VolumeCount).To(Equal(uint64(4)))
				g.Expect(resp.MetadataCount).To(BeZero())
				g.Expect(resp.ReferenceCount).To(BeZero())
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
