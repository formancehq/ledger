//go:build e2e

package business

import (
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("Periods", Ordered, func() {

	Context("Auto-bootstrap", Ordered, func() {
		BeforeAll(func() {
			// Create a ledger and a transaction to trigger period auto-bootstrap
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction("period-test", nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should have at least one OPEN period after first proposal", func() {
			periods, err := actions.ListAllPeriods(sharedCtx, sharedClient)
			Expect(err).To(Succeed())
			Expect(periods).NotTo(BeEmpty())
			// The last period should always be OPEN
			lastPeriod := periods[len(periods)-1]
			Expect(lastPeriod.Status).To(Equal(commonpb.PeriodStatus_PERIOD_OPEN))
		})
	})

	Context("Close period", Ordered, func() {
		It("Should close the current period and open a new one", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					{
						Type: &servicepb.Request_ClosePeriod{
							ClosePeriod: &servicepb.ClosePeriodRequest{},
						},
					},
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			closePeriodLog := resp.Logs[0].Payload.GetClosePeriod()
			Expect(closePeriodLog).NotTo(BeNil())
			Expect(closePeriodLog.ClosedPeriod.Status).To(Equal(commonpb.PeriodStatus_PERIOD_CLOSING))
			Expect(closePeriodLog.NewPeriod.Id).To(Equal(closePeriodLog.ClosedPeriod.Id + 1))
			Expect(closePeriodLog.NewPeriod.Status).To(Equal(commonpb.PeriodStatus_PERIOD_OPEN))
		})

		It("Should have at least two periods after close (last OPEN)", func() {
			// The sealer runs in the background so the second-to-last period may be CLOSING or CLOSED
			Eventually(func(g Gomega) {
				periods, err := actions.ListAllPeriods(sharedCtx, sharedClient)
				g.Expect(err).To(Succeed())
				g.Expect(len(periods)).To(BeNumerically(">=", 2))
				// Last period should always be OPEN
				g.Expect(periods[len(periods)-1].Status).To(Equal(commonpb.PeriodStatus_PERIOD_OPEN))
			}).Within(5 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())
		})

		It("Should eventually seal the closed period", func() {
			Eventually(func(g Gomega) {
				periods, err := actions.ListAllPeriods(sharedCtx, sharedClient)
				g.Expect(err).To(Succeed())
				g.Expect(len(periods)).To(BeNumerically(">=", 2))
				// Second-to-last period should eventually be CLOSED (sealed)
				secondToLast := periods[len(periods)-2]
				g.Expect(secondToLast.Status).To(Equal(commonpb.PeriodStatus_PERIOD_CLOSED))
				g.Expect(secondToLast.SealingHash).NotTo(BeEmpty())
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("Multiple simultaneous closing periods", Ordered, func() {
		It("Should allow closing while another period is already in CLOSING state", func() {
			// Wait until there's no CLOSING period (previous seal completed)
			Eventually(func(g Gomega) {
				periods, err := actions.ListAllPeriods(sharedCtx, sharedClient)
				g.Expect(err).To(Succeed())
				for _, p := range periods {
					g.Expect(p.Status).NotTo(Equal(commonpb.PeriodStatus_PERIOD_CLOSING))
				}
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Close the period twice in quick succession — both should succeed
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					{Type: &servicepb.Request_ClosePeriod{ClosePeriod: &servicepb.ClosePeriodRequest{}}},
				},
			})
			Expect(err).To(Succeed())

			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					{Type: &servicepb.Request_ClosePeriod{ClosePeriod: &servicepb.ClosePeriodRequest{}}},
				},
			})
			Expect(err).To(Succeed())

			// Wait for all periods to be sealed
			Eventually(func(g Gomega) {
				periods, err := actions.ListAllPeriods(sharedCtx, sharedClient)
				g.Expect(err).To(Succeed())
				for _, p := range periods {
					g.Expect(p.Status).NotTo(Equal(commonpb.PeriodStatus_PERIOD_CLOSING))
				}
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})
})

var _ = Describe("Receipts", Ordered, func() {

	const ledger = "receipt-test"

	BeforeAll(func() {
		// Create a ledger
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.CreateLedgerAction(ledger, nil)},
		})
		Expect(err).To(Succeed())
	})

	Context("GetTransaction receipt", Ordered, func() {
		var txID uint64

		BeforeAll(func() {
			// Create a transaction
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
						actions.NewPosting("world", "users:alice", big.NewInt(1000), "USD"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			applyLog := resp.Logs[0].Payload.GetApply()
			Expect(applyLog).NotTo(BeNil())
			txID = applyLog.Log.Data.GetCreatedTransaction().Transaction.Id
		})

		It("Should return a non-empty receipt on GetTransaction", func() {
			resp, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledger,
				TransactionId: txID,
			})
			Expect(err).To(Succeed())
			Expect(resp.Transaction).NotTo(BeNil())
			Expect(resp.Transaction.Id).To(Equal(txID))
			Expect(resp.Receipt).NotTo(BeEmpty(), "receipt should be non-empty when signing key is configured")
		})
	})

	Context("Receipt-based revert", Ordered, func() {
		var (
			txID    uint64
			receipt string
		)

		BeforeAll(func() {
			// Create a transaction
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
						actions.NewPosting("world", "users:bob", big.NewInt(500), "EUR"),
					}, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			applyLog := resp.Logs[0].Payload.GetApply()
			Expect(applyLog).NotTo(BeNil())
			txID = applyLog.Log.Data.GetCreatedTransaction().Transaction.Id

			// Get the receipt via GetTransaction
			getTxResp, err := sharedClient.GetTransaction(sharedCtx, &servicepb.GetTransactionRequest{
				Ledger:        ledger,
				TransactionId: txID,
			})
			Expect(err).To(Succeed())
			Expect(getTxResp.Receipt).NotTo(BeEmpty())
			receipt = getTxResp.Receipt
		})

		It("Should revert using receipt", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					{
						Type: &servicepb.Request_Apply{
							Apply: &servicepb.LedgerApplyRequest{
								Ledger: ledger,
								Data: &servicepb.LedgerApplyRequest_RevertTransaction{
									RevertTransaction: &servicepb.RevertTransactionPayload{
										TransactionId: txID,
										Force:         true,
										Receipt:       receipt,
									},
								},
							},
						},
					},
				},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			applyLog := resp.Logs[0].Payload.GetApply()
			Expect(applyLog).NotTo(BeNil())
			revertedTx := applyLog.Log.Data.GetRevertedTransaction()
			Expect(revertedTx).NotTo(BeNil())
			Expect(revertedTx.RevertTransaction).NotTo(BeNil())
		})

		It("Should reject reverting the same transaction again", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					{
						Type: &servicepb.Request_Apply{
							Apply: &servicepb.LedgerApplyRequest{
								Ledger: ledger,
								Data: &servicepb.LedgerApplyRequest_RevertTransaction{
									RevertTransaction: &servicepb.RevertTransactionPayload{
										TransactionId: txID,
										Force:         true,
										Receipt:       receipt,
									},
								},
							},
						},
					},
				},
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonTransactionAlreadyReverted))
		})
	})
})
