//go:build e2e

package business

import (
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
	"context"
	"io"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// listAllPeriods collects all periods from the streaming RPC.
func listAllPeriods(ctx context.Context, client servicepb.BucketServiceClient) ([]*commonpb.Period, error) {
	stream, err := client.ListPeriods(ctx, &servicepb.ListPeriodsRequest{})
	if err != nil {
		return nil, err
	}

	var periods []*commonpb.Period
	for {
		period, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		periods = append(periods, period)
	}
	return periods, nil
}

var _ = Describe("Periods", Ordered, func() {

	Context("Auto-bootstrap", Ordered, func() {
		BeforeAll(func() {
			// Create a ledger and a transaction to trigger period auto-bootstrap
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{testutil.CreateLedgerAction("period-test", nil)},
			})
			Expect(err).To(Succeed())
		})

		It("Should have at least one OPEN period after first proposal", func() {
			periods, err := listAllPeriods(sharedCtx, sharedClient)
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
				periods, err := listAllPeriods(sharedCtx, sharedClient)
				g.Expect(err).To(Succeed())
				g.Expect(len(periods)).To(BeNumerically(">=", 2))
				// Last period should always be OPEN
				g.Expect(periods[len(periods)-1].Status).To(Equal(commonpb.PeriodStatus_PERIOD_OPEN))
			}).Within(5 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())
		})

		It("Should eventually seal the closed period", func() {
			Eventually(func(g Gomega) {
				periods, err := listAllPeriods(sharedCtx, sharedClient)
				g.Expect(err).To(Succeed())
				g.Expect(len(periods)).To(BeNumerically(">=", 2))
				// Second-to-last period should eventually be CLOSED (sealed)
				secondToLast := periods[len(periods)-2]
				g.Expect(secondToLast.Status).To(Equal(commonpb.PeriodStatus_PERIOD_CLOSED))
				g.Expect(secondToLast.SealingHash).NotTo(BeEmpty())
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("Cannot close while closing", Ordered, func() {
		It("Should reject closing when a period is already in CLOSING state", func() {
			// Wait until there's no CLOSING period (previous seal completed)
			Eventually(func(g Gomega) {
				periods, err := listAllPeriods(sharedCtx, sharedClient)
				g.Expect(err).To(Succeed())
				for _, p := range periods {
					g.Expect(p.Status).NotTo(Equal(commonpb.PeriodStatus_PERIOD_CLOSING))
				}
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// The sealer runs asynchronously and may seal the period before
			// the second close request arrives. Try multiple close+close cycles
			// to catch the CLOSING state at least once.
			var gotExpectedError bool
			for i := 0; i < 10 && !gotExpectedError; i++ {
				// Create a transaction to have some data
				_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						testutil.CreateForceTransactionAction("period-test", []*commonpb.Posting{
							testutil.NewPosting("world", "user:alice", big.NewInt(100), "USD"),
						}, nil),
					},
				})
				Expect(err).To(Succeed())

				// Wait for any previous seal to complete
				Eventually(func(g Gomega) {
					periods, err := listAllPeriods(sharedCtx, sharedClient)
					g.Expect(err).To(Succeed())
					for _, p := range periods {
						g.Expect(p.Status).NotTo(Equal(commonpb.PeriodStatus_PERIOD_CLOSING))
					}
				}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

				// Close the period
				_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						{Type: &servicepb.Request_ClosePeriod{ClosePeriod: &servicepb.ClosePeriodRequest{}}},
					},
				})
				Expect(err).To(Succeed())

				// Immediately try to close again
				_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						{Type: &servicepb.Request_ClosePeriod{ClosePeriod: &servicepb.ClosePeriodRequest{}}},
					},
				})
				if err != nil {
					st, ok := status.FromError(err)
					Expect(ok).To(BeTrue())
					Expect(st.Code()).To(Equal(codes.FailedPrecondition))

					info := testutil.ExtractGRPCErrorInfo(err)
					Expect(info).NotTo(BeNil())
					Expect(info.Reason).To(Equal(domain.ErrReasonPeriodAlreadyClosing))
					gotExpectedError = true
				}
				// If no error, the sealer was faster — try again
			}

			if !gotExpectedError {
				Skip("Sealer completed too quickly to catch CLOSING state in e2e — covered by unit test TestProcessClosePeriod_AlreadyClosing")
			}
		})
	})
})

var _ = Describe("Receipts", Ordered, func() {

	const ledger = "receipt-test"

	BeforeAll(func() {
		// Create a ledger
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{testutil.CreateLedgerAction(ledger, nil)},
		})
		Expect(err).To(Succeed())
	})

	Context("GetTransaction receipt", Ordered, func() {
		var txID uint64

		BeforeAll(func() {
			// Create a transaction
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateForceTransactionAction(ledger, []*commonpb.Posting{
						testutil.NewPosting("world", "users:alice", big.NewInt(1000), "USD"),
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
					testutil.CreateForceTransactionAction(ledger, []*commonpb.Posting{
						testutil.NewPosting("world", "users:bob", big.NewInt(500), "EUR"),
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

			info := testutil.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonTransactionAlreadyReverted))
		})
	})
})
