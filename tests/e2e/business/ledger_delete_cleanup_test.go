//go:build e2e

package business

import (
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Ledger Deletion Data Cleanup", Ordered, func() {

	Context("When deleting a ledger with transactions and metadata", Ordered, func() {
		var ledgerName = "cleanup-ledger-with-data"

		It("Should start with a ledger with transactions and metadata", func() {
			// Create the ledger
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create transactions
			for i := 0; i < 3; i++ {
				_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
							actions.NewPosting("world", fmt.Sprintf("user:%d", i), big.NewInt(100*int64(i+1)), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}

			// Set account metadata
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.SaveAccountMetadataAction(ledgerName, "user:0", map[string]string{
						"role": "admin",
						"tier": "premium",
					}),
				},
			})
			Expect(err).To(Succeed())

			// Verify transactions exist
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "user:0",
				})
				g.Expect(err).To(Succeed())
				g.Expect(account.Volumes).To(HaveKey("USD"))
				g.Expect(account.Volumes["USD"].Balance).To(Equal("100"))
			}).Within(15 * time.Second).WithPolling(500 * time.Millisecond).Should(Succeed())
		})

		It("Should delete the ledger", func() {
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.DeleteLedgerAction(ledgerName)},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Wait until the ledger is no longer accessible
			Eventually(func(g Gomega) bool {
				_, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{
					Ledger: ledgerName,
				})
				return err != nil
			}).Within(15 * time.Second).WithPolling(500 * time.Millisecond).Should(BeTrue())
		})

		It("Should reject operations on the deleted ledger", func() {
			// Creating a transaction on a deleted ledger should fail
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "new:user", big.NewInt(50), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(HaveOccurred())
		})

		It("Should reject re-creating a deleted ledger", func() {
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(HaveOccurred())
		})
	})

	Context("When deleting a ledger with reverted transactions", Ordered, func() {
		var ledgerName = "cleanup-revert-ledger"

		BeforeAll(func() {
			// Create ledger
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.CreateLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())

			// Create a transaction
			resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(500), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())

			txID, ok := actions.GetCreatedTransactionID(resp)
			Expect(ok).To(BeTrue())

			// Revert the transaction
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.RevertTransactionAction(ledgerName, txID, false, false, nil),
				},
			})
			Expect(err).To(Succeed())

			// Verify alice has zero balance after revert
			Eventually(func(g Gomega) {
				account, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
					Ledger:  ledgerName,
					Address: "alice",
				})
				g.Expect(err).To(Succeed())
				g.Expect(account.Volumes).To(HaveKey("USD"))
				g.Expect(account.Volumes["USD"].Balance).To(Equal("0"))
			}).Within(15 * time.Second).WithPolling(500 * time.Millisecond).Should(Succeed())
		})

		It("Should delete and reject further operations", func() {
			// Delete the ledger
			_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{actions.DeleteLedgerAction(ledgerName)},
			})
			Expect(err).To(Succeed())

			// Wait for deletion
			Eventually(func(g Gomega) bool {
				_, err := sharedClient.GetLedger(sharedCtx, &servicepb.GetLedgerRequest{
					Ledger: ledgerName,
				})
				return err != nil
			}).Within(15 * time.Second).WithPolling(500 * time.Millisecond).Should(BeTrue())

			// Operations should be rejected
			_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
						actions.NewPosting("world", "bob", big.NewInt(300), "EUR"),
					}, nil, nil),
				},
			})
			Expect(err).To(HaveOccurred())
		})
	})
})
