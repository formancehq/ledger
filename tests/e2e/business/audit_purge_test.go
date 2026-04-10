//go:build e2e

package business

import (
	"context"
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
)

// archivePeriodFull closes, waits for seal, archives, and waits for ARCHIVED.
func archivePeriodFull(ctx context.Context, client servicepb.BucketServiceClient) {
	// Close the current period.
	_, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{actions.ClosePeriodAction()},
	})
	Expect(err).To(Succeed())

	// Wait for a CLOSED period (sealed).
	var closedPeriodID uint64

	Eventually(func(g Gomega) {
		periods, err := actions.ListAllPeriods(ctx, client)
		g.Expect(err).To(Succeed())

		for _, p := range periods {
			if p.Status == commonpb.PeriodStatus_PERIOD_CLOSED {
				closedPeriodID = p.GetId()

				return
			}
		}

		g.Expect(false).To(BeTrue(), "no CLOSED period found yet")
	}).Within(15 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

	// Archive the closed period.
	_, err = client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{actions.ArchivePeriodAction(closedPeriodID)},
	})
	Expect(err).To(Succeed())

	// Wait for ARCHIVED.
	Eventually(func(g Gomega) {
		periods, err := actions.ListAllPeriods(ctx, client)
		g.Expect(err).To(Succeed())

		for _, p := range periods {
			if p.GetId() == closedPeriodID {
				g.Expect(p.Status).To(Equal(commonpb.PeriodStatus_PERIOD_ARCHIVED))

				return
			}
		}

		g.Expect(false).To(BeTrue(), "archived period not found")
	}).Within(30 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
}

var _ = Describe("Audit entry purge after period archive", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const (
		httpPort = 15700
		grpcPort = 15800
	)

	BeforeAll(func() {
		ctx, client, _ = testutil.SetupSingleNode(httpPort, grpcPort,
			testserver.WithColdStorageDriver("filesystem"),
		)
	})

	// This test verifies that audit entries are correctly purged when a period
	// is archived, even when log and audit sequence counters have diverged due
	// to batched proposals.
	//
	// The bug scenario: when a proposal batches N orders, it produces N logs but
	// only 1 audit entry. After several batched proposals, the log sequence
	// counter is much higher than the audit sequence counter. The period purge
	// uses log sequence ranges [startLogSeq, closeLogSeq] for the DeleteRange
	// on the audit prefix (0x02). But audit entries have lower sequence numbers
	// that fall below startLogSeq, so they are missed by the purge.
	//
	// This test creates the divergence by submitting batches of 5 transactions
	// (5 logs per 1 audit entry), then archives two periods. Period 2 is where
	// the bug manifests: the purge range is too high to catch the audit entries.
	It("should purge all audit entries from period 2 after archive", func() {
		const ledger = "audit-purge-test"

		// Enable audit, create ledger.
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.SetAuditConfigAction(true)},
		})
		Expect(err).To(Succeed())

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{actions.CreateLedgerAction(ledger, nil)},
		})
		Expect(err).To(Succeed())

		// Period 1: create transactions in batches to diverge counters.
		// Each Apply with 5 transactions = 1 proposal = 5 logs + 1 audit entry.
		for range 4 {
			txns := make([]*servicepb.Request, 5)
			for j := range txns {
				txns[j] = actions.CreateForceTransactionAction(ledger,
					[]*commonpb.Posting{
						actions.NewPosting("world", "users:alice", big.NewInt(100), "USD"),
					}, nil)
			}

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{Requests: txns})
			Expect(err).To(Succeed())
		}

		entriesBefore, err := actions.ListAuditEntries(ctx, client, false)
		Expect(err).To(Succeed())
		Expect(len(entriesBefore)).To(BeNumerically(">=", 6), "should have audit entries from period 1")

		GinkgoWriter.Printf("Period 1: %d audit entries, max audit seq = %d\n",
			len(entriesBefore), entriesBefore[len(entriesBefore)-1].Sequence)

		// Archive period 1.
		archivePeriodFull(ctx, client)

		// Verify period 1 audit entries were purged.
		Eventually(func(g Gomega) {
			entries, err := actions.ListAuditEntries(ctx, client, false)
			g.Expect(err).To(Succeed())
			g.Expect(len(entries)).To(BeNumerically("<", len(entriesBefore)),
				"period 1 audit entries should have been purged")
		}).Within(10 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())

		// Period 2: create more batched transactions.
		for range 4 {
			txns := make([]*servicepb.Request, 5)
			for j := range txns {
				txns[j] = actions.CreateForceTransactionAction(ledger,
					[]*commonpb.Posting{
						actions.NewPosting("world", "users:bob", big.NewInt(100), "EUR"),
					}, nil)
			}

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{Requests: txns})
			Expect(err).To(Succeed())
		}

		entriesBeforeP2, err := actions.ListAuditEntries(ctx, client, false)
		Expect(err).To(Succeed())
		period2AuditCount := len(entriesBeforeP2)

		GinkgoWriter.Printf("Period 2 before archive: %d audit entries (max audit seq=%d)\n",
			period2AuditCount, entriesBeforeP2[period2AuditCount-1].Sequence)

		// Archive period 2.
		archivePeriodFull(ctx, client)

		// Verify period 2 audit entries were purged.
		// After archive, only audit entries from period 3 operations
		// (ClosePeriod, ArchivePeriod, ConfirmArchive) should remain.
		Eventually(func(g Gomega) {
			entries, err := actions.ListAuditEntries(ctx, client, false)
			g.Expect(err).To(Succeed())

			GinkgoWriter.Printf("After period 2 archive: %d audit entries remain\n", len(entries))

			// Period 3 operations produce ~3 audit entries (close, archive, confirm).
			// If period 2 entries leaked, we'd see significantly more.
			g.Expect(len(entries)).To(BeNumerically("<=", 5),
				"period 2 audit entries should have been purged; "+
					"if more than 5 remain, the purge missed entries due to sequence counter divergence")
		}).Within(10 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
	})
})
