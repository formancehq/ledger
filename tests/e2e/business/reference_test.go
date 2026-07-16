//go:build e2e

package business

import (
	"math/big"
	"strconv"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("Transaction Reference Uniqueness", Ordered, func() {

	Context("Within a single ledger", Ordered, func() {
		var ledgerName = "ref-test-ledger"

		BeforeAll(func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should create a transaction with a reference", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.WithReference(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
				}, nil, nil),
				"ref-001",
			)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should fail when creating a transaction with a duplicate reference", func() {
			// First transaction with reference succeeds
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.WithReference(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
				}, nil, nil),
				"dup-ref",
			)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Second transaction with the same reference fails
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.WithReference(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "account-2", big.NewInt(200), "USD"),
				}, nil, nil),
				"dup-ref",
			)))
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.AlreadyExists))

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonTransactionReferenceConflict))
			Expect(info.Domain).To(Equal("ledger"))
			Expect(info.Metadata["reference"]).To(Equal("dup-ref"))
		})

		It("Should allow transactions without a reference", func() {
			// Multiple transactions without reference should all succeed
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
			}, nil, nil),
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "account-2", big.NewInt(200), "USD"),
				}, nil, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))
		})

		It("Should allow different references in the same ledger", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.WithReference(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
				}, nil, nil),
				"ref-a",
			),
				actions.WithReference(
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "account-2", big.NewInt(200), "USD"),
					}, nil, nil),
					"ref-b",
				)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))
		})
	})

	Context("With skippable_reasons opt-in", Ordered, func() {
		var skipLedger = "ref-skip-ledger"

		BeforeAll(func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(skipLedger, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})

		It("Should succeed normally when the reference does not pre-exist (skip is a no-op)", func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.WithSkippableReasons(
				actions.WithReference(
					actions.CreateTransactionAction(skipLedger, []*commonpb.Posting{
						actions.NewPosting("world", "account-fresh", big.NewInt(100), "USD"),
					}, nil, nil),
					"skip-fresh-ref",
				),
				commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
			)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			created := resp.Logs[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction()
			Expect(created).NotTo(BeNil())
			Expect(created.GetTransaction().GetId()).NotTo(BeZero())
		})

		It("Should convert the duplicate-reference failure into an OrderSkipped log", func() {
			firstResp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.WithReference(
				actions.CreateTransactionAction(skipLedger, []*commonpb.Posting{
					actions.NewPosting("world", "account-skip", big.NewInt(100), "USD"),
				}, nil, nil),
				"skip-dup-ref",
			)))
			Expect(err).To(Succeed())
			Expect(firstResp.Logs).To(HaveLen(1))

			firstLog := firstResp.Logs[0].GetPayload().GetApply().GetLog()
			firstTxID := firstLog.GetData().GetCreatedTransaction().GetTransaction().GetId()
			firstLogID := firstLog.GetId()

			Expect(firstTxID).NotTo(BeZero())
			Expect(firstLogID).NotTo(BeZero())

			// Same reference, this time the caller authorises the skip.
			skipResp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.WithSkippableReasons(
				actions.WithReference(
					actions.CreateTransactionAction(skipLedger, []*commonpb.Posting{
						actions.NewPosting("world", "account-skip", big.NewInt(200), "USD"),
					}, nil, nil),
					"skip-dup-ref",
				),
				commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
			)))
			Expect(err).To(Succeed())
			Expect(skipResp.Logs).To(HaveLen(1))

			skipLog := skipResp.Logs[0].GetPayload().GetApply().GetLog()
			skipped := skipLog.GetData().GetOrderSkipped()
			Expect(skipped).NotTo(BeNil())
			Expect(skipped.GetReason()).To(Equal(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT))
			Expect(skipped.GetContext()["reference"]).To(Equal("skip-dup-ref"))
			Expect(skipped.GetContext()["existingTransactionId"]).To(Equal(strconv.FormatUint(firstTxID, 10)))

			// The OrderSkipped log MUST have a real per-ledger Id (consecutive
			// to the preceding one) and a Date — the read-side index keys
			// per-ledger logs by Id, so a skip persisted with Id=0 would
			// overwrite every prior skip on the same ledger.
			Expect(skipLog.GetId()).To(BeNumerically(">", firstLogID))
			Expect(skipLog.GetDate()).NotTo(BeNil())
			Expect(skipLog.GetDate().GetData()).NotTo(BeZero())
		})

		It("Should still fail loudly when skippable_reasons is empty (default behaviour preserved)", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.WithReference(
				actions.CreateTransactionAction(skipLedger, []*commonpb.Posting{
					actions.NewPosting("world", "account-skip", big.NewInt(300), "USD"),
				}, nil, nil),
				"skip-dup-ref",
			)))
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.AlreadyExists))
		})

		It("Should keep an atomic batch's other orders intact when one order skips", func() {
			// Use a dedicated destination account so the test asserts only
			// against postings produced in this scenario — other tests in
			// the suite write to "account-batch" and would skew the running
			// totals if shared.
			const account = "account-batch-mixed"

			// Pre-create the colliding reference so the skip-tolerant order
			// in the batch hits TRANSACTION_REFERENCE_CONFLICT.
			seedResp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.WithReference(
				actions.CreateTransactionAction(skipLedger, []*commonpb.Posting{
					actions.NewPosting("world", account, big.NewInt(50), "USD"),
				}, nil, nil),
				"mixed-batch-dup-ref",
			)))
			Expect(err).To(Succeed())
			Expect(seedResp.Logs).To(HaveLen(1))

			seedTxID := seedResp.Logs[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetId()

			batchResp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				// Order 0 — strict, must succeed.
				actions.CreateTransactionAction(skipLedger, []*commonpb.Posting{
					actions.NewPosting("world", account, big.NewInt(10), "USD"),
				}, nil, nil),
				// Order 1 — skip-tolerant, must convert the conflict and
				// NOT leak its 11-unit posting into the destination
				// account's volume.
				actions.WithSkippableReasons(
					actions.WithReference(
						actions.CreateTransactionAction(skipLedger, []*commonpb.Posting{
							actions.NewPosting("world", account, big.NewInt(11), "USD"),
						}, nil, nil),
						"mixed-batch-dup-ref",
					),
					commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
				),
				// Order 2 — strict again, must still succeed even though
				// the previous order produced a skip log (atomic batch
				// continues on whitelisted business failures).
				actions.CreateTransactionAction(skipLedger, []*commonpb.Posting{
					actions.NewPosting("world", account, big.NewInt(12), "USD"),
				}, nil, nil),
			))
			Expect(err).To(Succeed())
			Expect(batchResp.Logs).To(HaveLen(3))

			Expect(batchResp.Logs[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction()).NotTo(BeNil())

			skipped := batchResp.Logs[1].GetPayload().GetApply().GetLog().GetData().GetOrderSkipped()
			Expect(skipped).NotTo(BeNil())
			Expect(skipped.GetReason()).To(Equal(commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT))
			Expect(skipped.GetContext()["existingTransactionId"]).To(Equal(strconv.FormatUint(seedTxID, 10)))

			// The skip log carries a real per-ledger id and date even
			// though no transaction was created — every ledger log needs
			// one for the read-side index to work.
			skipLog := batchResp.Logs[1].GetPayload().GetApply().GetLog()
			Expect(skipLog.GetId()).NotTo(BeZero())
			Expect(skipLog.GetDate()).NotTo(BeNil())

			Expect(batchResp.Logs[2].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction()).NotTo(BeNil())

			// Volume invariant: the skipped order's 11 units must NOT have
			// landed. Expected total received = 50 (seed) + 10 (order 0)
			// + 12 (order 2) = 72.
			accountResp, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  skipLedger,
				Address: account,
			})
			Expect(err).To(Succeed())
			Expect(accountResp.FindVolume("USD", "").GetInput()).To(Equal("72"))
		})

		It("Should reject a skippable_reasons entry that is not in the operation's whitelist", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.WithSkippableReasons(
				actions.WithReference(
					actions.CreateTransactionAction(skipLedger, []*commonpb.Posting{
						actions.NewPosting("world", "account-skip", big.NewInt(400), "USD"),
					}, nil, nil),
					"never-existed",
				),
				commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS,
			)))
			Expect(err).To(HaveOccurred())

			info := actions.ExtractGRPCErrorInfo(err)
			Expect(info).NotTo(BeNil())
			Expect(info.Reason).To(Equal(domain.ErrReasonValidation))
		})
	})

	Context("Across different ledgers", Ordered, func() {
		var (
			ledgerA = "ref-ledger-a"
			ledgerB = "ref-ledger-b"
		)

		BeforeAll(func() {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerA, nil),
				actions.CreateLedgerAction(ledgerB, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(2))
		})

		It("Should allow the same reference in different ledgers", func() {
			// Create transaction with reference in ledger A
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.WithReference(
				actions.CreateTransactionAction(ledgerA, []*commonpb.Posting{
					actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
				}, nil, nil),
				"shared-ref",
			)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Same reference in ledger B should succeed
			resp, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.WithReference(
				actions.CreateTransactionAction(ledgerB, []*commonpb.Posting{
					actions.NewPosting("world", "account-1", big.NewInt(100), "USD"),
				}, nil, nil),
				"shared-ref",
			)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))
		})
	})
})
