//go:build e2e

package business

import (
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("SkipResponse", Ordered, func() {
	var ledgerName = "skip-response-ledger"

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
		})
		Expect(err).To(Succeed())
	})

	It("Should strip log payloads when skip_response=true", func() {
		resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "skip-resp-1", big.NewInt(100), "USD"),
				}, nil, nil),
			},
			SkipResponse: true,
		})
		Expect(err).To(Succeed())
		Expect(resp).NotTo(BeNil())
		Expect(resp.Logs).To(HaveLen(1))

		log := resp.Logs[0]
		Expect(log.Sequence).NotTo(BeZero())
		Expect(log.Payload).To(BeNil())
		Expect(log.Idempotency).To(BeNil())
		Expect(log.Hash).To(BeEmpty())
		Expect(log.Signature).To(BeNil())
		Expect(log.Receipt).To(BeEmpty())
		Expect(log.ResponseSignature).To(BeNil())
	})

	It("Should include full payloads by default", func() {
		resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "skip-resp-2", big.NewInt(200), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())
		Expect(resp).NotTo(BeNil())
		Expect(resp.Logs).To(HaveLen(1))

		log := resp.Logs[0]
		Expect(log.Sequence).NotTo(BeZero())
		Expect(log.Payload).NotTo(BeNil())
		Expect(log.Payload.GetApply()).NotTo(BeNil())
		Expect(log.Payload.GetApply().GetLog().GetData().GetCreatedTransaction()).NotTo(BeNil())
	})

	It("Should strip payloads for batch operations", func() {
		resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "skip-resp-batch-1", big.NewInt(100), "USD"),
				}, nil, nil),
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "skip-resp-batch-2", big.NewInt(200), "USD"),
				}, nil, nil),
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "skip-resp-batch-3", big.NewInt(300), "USD"),
				}, nil, nil),
			},
			SkipResponse: true,
		})
		Expect(err).To(Succeed())
		Expect(resp).NotTo(BeNil())
		Expect(resp.Logs).To(HaveLen(3))

		for _, log := range resp.Logs {
			Expect(log.Sequence).NotTo(BeZero())
			Expect(log.Payload).To(BeNil())
		}
	})

	It("Should still apply transactions even when response is skipped", func() {
		// Create a transaction with skip_response
		resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "skip-resp-verify", big.NewInt(500), "USD"),
				}, nil, nil),
			},
			SkipResponse: true,
		})
		Expect(err).To(Succeed())
		Expect(resp.Logs).To(HaveLen(1))

		// Verify the transaction was actually applied by reading the account
		account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
			Ledger:  ledgerName,
			Address: "skip-resp-verify",
		})
		Expect(err).To(Succeed())
		Expect(account).NotTo(BeNil())
		Expect(account.Volumes).NotTo(BeEmpty())
	})
})
