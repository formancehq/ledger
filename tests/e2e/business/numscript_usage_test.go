//go:build e2e

package business

import (
	"time"

	"github.com/formancehq/ledger/v3/pkg/actions"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("GetTemplateUsage", Ordered, func() {

	Context("When a template is invoked via a script reference", Ordered, func() {
		const (
			ledgerName   = "template-usage-ledger"
			templateName = "payout"
		)

		const payoutScript = `
vars {
  account $destination
  monetary $amount
}

send $amount (
  source = @world
  destination = $destination
)
`

		BeforeAll(func() {
			// Create the ledger and register the template in the numscript library.
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
				actions.CreateLedgerAction(ledgerName, nil),
				actions.SaveNumscriptWithVersionAction(ledgerName, templateName, payoutScript, "1.0.0")))
			Expect(err).To(Succeed())
		})

		It("Should start at zero before any invocation", func() {
			usage, err := actions.GetTemplateUsage(sharedCtx, sharedClient, ledgerName, templateName)
			Expect(err).To(Succeed())
			Expect(usage.GetCount()).To(BeZero())
			Expect(usage.GetLastUsed()).To(BeNil(), "a never-invoked template has no lastUsed")
		})

		It("Should increment count and set lastUsed after a real invocation", func() {
			// Invoke the template twice via script-reference transactions.
			for i := 0; i < 2; i++ {
				_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
					actions.CreateScriptRefTransactionAction(ledgerName, templateName, "1.0.0", map[string]string{
						"destination": "users:alice",
						"amount":      "USD/2 100",
					}, nil)))
				Expect(err).To(Succeed())
			}

			// The usagebuilder folds the counter asynchronously (one background
			// tick behind the FSM), so poll rather than assume immediate
			// visibility. Never time.Sleep.
			Eventually(func(g Gomega) {
				usage, err := actions.GetTemplateUsage(sharedCtx, sharedClient, ledgerName, templateName)
				g.Expect(err).To(Succeed())
				g.Expect(usage.GetCount()).To(Equal(uint64(2)), "count must reflect both invocations")
				g.Expect(usage.GetLastUsed()).NotTo(BeNil(), "lastUsed must be populated after invocation")
				g.Expect(usage.GetLastUsed().GetData()).NotTo(BeZero(), "lastUsed timestamp must be a real value")
			}).Within(30 * time.Second).WithPolling(200 * time.Millisecond).Should(Succeed())
		})
	})

	Context("Unknown-template / unknown-ledger contract", func() {
		const ledgerName = "template-usage-contract-ledger"

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
		})

		It("Should return zero (not NotFound) for an unknown template on an existing ledger", func() {
			// The endpoint's contract is zero-valued, not 404: clients treat a
			// never-seen template uniformly as count 0. Only an unknown ledger 404s.
			usage, err := actions.GetTemplateUsage(sharedCtx, sharedClient, ledgerName, "never-registered")
			Expect(err).To(Succeed())
			Expect(usage.GetCount()).To(BeZero())
			Expect(usage.GetLastUsed()).To(BeNil())
		})

		It("Should return NotFound for an unknown ledger", func() {
			_, err := actions.GetTemplateUsage(sharedCtx, sharedClient, "non-existent-ledger", "payout")
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.NotFound))
		})
	})
})
