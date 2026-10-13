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
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
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
			Expect(resp.ReferenceCount).To(BeZero())
		})
	})

	Context("When getting stats for a ledger with transactions", Ordered, func() {
		var ledgerName = "stats-with-data"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			// Create 3 transactions producing 4 accounts: world, bank:main, bank:fees, users:alice
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "bank:main", big.NewInt(1000), "USD"),
			}, nil, nil),
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("bank:main", "bank:fees", big.NewInt(10), "USD"),
				}, nil, nil),
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("bank:main", "users:alice", big.NewInt(100), "USD"),
				}, nil, nil)))
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
				g.Expect(resp.ReferenceCount).To(BeZero())
			}).Should(Succeed())
		})
	})

	Context("When a persisted volume starts at zero", Ordered, func() {
		const ledgerName = "stats-persisted-zero-volume"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "alice", big.NewInt(0), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "alice", big.NewInt(1), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
		})

		It("Should count each persisted volume only once", func() {
			Eventually(func(g Gomega) {
				resp, err := sharedClient.GetLedgerStats(sharedCtx, &servicepb.GetLedgerStatsRequest{
					Ledger: ledgerName,
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp.TransactionCount).To(Equal(uint64(2)))
				g.Expect(resp.PostingCount).To(Equal(uint64(2)))
				g.Expect(resp.VolumeCount).To(Equal(uint64(2)))
			}).Should(Succeed())

			stream, err := sharedClient.ListLogs(sharedCtx, &servicepb.ListLogsRequest{Ledger: ledgerName})
			Expect(err).To(Succeed())
			logs := collectLogs(stream)
			Expect(logs).To(HaveLen(2))
			latest := logs[0]
			for _, log := range logs[1:] {
				if log.GetPayload().GetApply().GetLog().GetId() > latest.GetPayload().GetApply().GetLog().GetId() {
					latest = log
				}
			}
			Expect(latest.GetPayload().GetApply().GetLog().GetNewKeptVolumes()).To(BeEmpty())
		})
	})

	Context("When a purged account becomes normal before cache rotation", Ordered, func() {
		const ledgerName = "stats-purged-then-normal"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateLedgerAction(ledgerName, nil),
				actions.AddEphemeralAccountTypeAction(ledgerName, "temporary", "temporary:{id}"),
			))
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "temporary:one", big.NewInt(1), "USD"),
				}, nil),
				actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("temporary:one", "world", big.NewInt(1), "USD"),
				}, nil),
			))
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.RemoveAccountTypeAction(ledgerName, "temporary"),
			))
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.AddAccountTypeAction(ledgerName, "temporary", "temporary:{id}"),
			))
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "temporary:one", big.NewInt(1), "USD"),
				}, nil, nil),
			))
			Expect(err).To(Succeed())
		})

		It("Should count the recreated persistent row as new", func() {
			Eventually(func(g Gomega) {
				resp, err := sharedClient.GetLedgerStats(sharedCtx, &servicepb.GetLedgerStatsRequest{Ledger: ledgerName})
				g.Expect(err).To(Succeed())
				g.Expect(resp.PostingCount).To(Equal(uint64(3)))
				g.Expect(resp.VolumeCount).To(Equal(uint64(2)))
			}).Should(Succeed())

			stream, err := sharedClient.ListLogs(sharedCtx, &servicepb.ListLogsRequest{Ledger: ledgerName})
			Expect(err).To(Succeed())
			logs := collectLogs(stream)
			Expect(logs).To(HaveLen(6))
			latest := logs[0]
			for _, log := range logs[1:] {
				if log.GetPayload().GetApply().GetLog().GetId() > latest.GetPayload().GetApply().GetLog().GetId() {
					latest = log
				}
			}
			Expect(latest.GetPayload().GetApply().GetLog().GetNewKeptVolumes()).To(ContainElement(
				HaveField("Account", Equal("temporary:one")),
			))
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
