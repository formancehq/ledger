//go:build e2e

package business

import (
	"fmt"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Ledger Deletion", Ordered, func() {

	Context("When deleting a ledger", Ordered, func() {
		var ledgerName = "test-ledger-to-delete"

		BeforeAll(func() {
			// Create a ledger
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify the ledger exists
			ledger, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(ledger.Name).To(Equal(ledgerName))
		})

		It("Should successfully delete the ledger (soft delete)", func() {
			// Delete the ledger (soft delete)
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.DeleteLedgerAction(ledgerName)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify the ledger is no longer accessible via GetLedger (filtered out)
			Eventually(func(g Gomega) bool {
				_, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{
					Ledger: ledgerName,
				})
				return err != nil
			}).Within(15 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())

			// Verify the ledger is not in the list of all ledgers (soft-deleted are filtered)
			ledgers, err := actions.ListLedgers(sharedCtx, sharedClient)
			Expect(err).To(Succeed())
			for name := range ledgers {
				Expect(name).NotTo(Equal(ledgerName))
			}

			// Verify the ledger cannot be retrieved (soft-deleted)
			_, err = sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(HaveOccurred())

			// Verify the ledger does not appear in the list
			ledgers, err = actions.ListLedgers(sharedCtx, sharedClient)
			Expect(err).To(Succeed())
			_, found := ledgers[ledgerName]
			Expect(found).To(BeFalse())
		})

		It("Should return error when trying to delete a non-existent ledger", func() {
			// Try to delete a non-existent ledger
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.DeleteLedgerAction("non-existent-ledger")))
			Expect(err).To(HaveOccurred())
		})
	})

	Context("When deleting a ledger with transactions", Ordered, func() {
		var ledgerName = "ledger-with-transactions"

		BeforeAll(func() {
			// Create a ledger
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Create some transactions
			for i := 0; i < 5; i++ {
				_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", fmt.Sprintf("account-%d", i), big.NewInt(100*int64(i+1)), "USD"),
				}, nil, nil)))
				Expect(err).To(Succeed())
			}
		})

		It("Should successfully soft-delete the ledger even with transactions", func() {
			// Soft-delete the ledger (should succeed even with transactions)
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.DeleteLedgerAction(ledgerName)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify the ledger is no longer accessible (soft-deleted)
			Eventually(func(g Gomega) bool {
				_, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{
					Ledger: ledgerName,
				})
				return err != nil
			}).Within(15 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())
		})
	})
})
