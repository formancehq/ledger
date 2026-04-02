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

var _ = Describe("Barrier", Ordered, func() {
	var ledgerName = "test-barrier"

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
		})
		Expect(err).To(Succeed())
	})

	It("Should succeed on an idle cluster", func() {
		_, err := sharedClient.Barrier(sharedCtx, &servicepb.BarrierRequest{})
		Expect(err).To(Succeed())
	})

	It("Should guarantee prior writes are visible after return", func() {
		// Create a transaction
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "barrier-test-account", big.NewInt(500), "USD"),
				}, nil, nil),
			},
		})
		Expect(err).To(Succeed())

		// Call Barrier to ensure the write is fully applied
		_, err = sharedClient.Barrier(sharedCtx, &servicepb.BarrierRequest{})
		Expect(err).To(Succeed())

		// The account should be visible with correct balance
		account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
			Ledger:  ledgerName,
			Address: "barrier-test-account",
		})
		Expect(err).To(Succeed())
		Expect(account).NotTo(BeNil())
		Expect(account.Volumes).To(HaveKey("USD"))
		Expect(account.Volumes["USD"].GetBalance()).To(Equal("500"))
	})
})
