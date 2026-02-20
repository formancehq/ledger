//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Ledger Deletion", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		httpPort = testSingleHTTPPort
		grpcPort = testSingleGRPCPort
	)

	BeforeAll(func() {
		ctx, client, _ = setupSingleNode(httpPort, grpcPort)
	})

	Context("When deleting a ledger", Ordered, func() {
		var ledgerName = "test-ledger-to-delete"

		BeforeAll(func() {
			// Create a ledger
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Verify the ledger exists
			ledger, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(Succeed())
			Expect(ledger.Name).To(Equal(ledgerName))
		})

		It("Should successfully delete the ledger (soft delete)", func() {
			// Delete the ledger (soft delete)
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deleteLedgerAction(ledgerName)},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify the ledger is no longer accessible via GetLedger (filtered out)
			Eventually(func(g Gomega) bool {
				_, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{
					Ledger: ledgerName,
				})
				return err != nil
			}).Within(15 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())

			// Verify the ledger is not in the list of all ledgers (soft-deleted are filtered)
			ledgers, err := listLedgers(ctx, client)
			Expect(err).To(Succeed())
			for name := range ledgers {
				Expect(name).NotTo(Equal(ledgerName))
			}

			// Verify the ledger cannot be retrieved (soft-deleted)
			_, err = client.GetLedger(ctx, &servicepb.GetLedgerRequest{
				Ledger: ledgerName,
			})
			Expect(err).To(HaveOccurred())

			// Verify the ledger does not appear in the list
			ledgers, err = listLedgers(ctx, client)
			Expect(err).To(Succeed())
			_, found := ledgers[ledgerName]
			Expect(found).To(BeFalse())
		})

		It("Should return error when trying to delete a non-existent ledger", func() {
			// Try to delete a non-existent ledger
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deleteLedgerAction("non-existent-ledger")},
			})
			Expect(err).To(HaveOccurred())
		})
	})

	Context("When deleting a ledger with transactions", Ordered, func() {
		var ledgerName = "ledger-with-transactions"

		BeforeAll(func() {
			// Create a ledger
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{createLedgerAction(ledgerName, nil)},
			})
			Expect(err).To(Succeed())
			Expect(resp.Logs).To(HaveLen(1))

			// Create some transactions
			for i := 0; i < 5; i++ {
				_, err := client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						createTransactionAction(ledgerName, []*commonpb.Posting{
							newPosting("world", fmt.Sprintf("account-%d", i), big.NewInt(100*int64(i+1)), "USD"),
						}, nil, nil),
					},
				})
				Expect(err).To(Succeed())
			}
		})

		It("Should successfully soft-delete the ledger even with transactions", func() {
			// Soft-delete the ledger (should succeed even with transactions)
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{deleteLedgerAction(ledgerName)},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify the ledger is no longer accessible (soft-deleted)
			Eventually(func(g Gomega) bool {
				_, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{
					Ledger: ledgerName,
				})
				return err != nil
			}).Within(15 * time.Second).WithPolling(500 * time.Millisecond).To(BeTrue())
		})
	})
})
