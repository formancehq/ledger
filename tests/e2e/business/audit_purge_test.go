//go:build e2e

package business

import (
	"context"
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// archiveChapterFull closes, waits for seal, archives, and waits for ARCHIVED.
func archiveChapterFull(ctx context.Context, client servicepb.BucketServiceClient) {
	// Close the current chapter.
	_, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Envelopes: servicepb.UnsignedEnvelopes(actions.CloseChapterAction()),
	})
	Expect(err).To(Succeed())

	// Wait for a CLOSED chapter (sealed).
	var closedChapterID uint64

	Eventually(func(g Gomega) {
		chapters, err := actions.ListAllChapters(ctx, client)
		g.Expect(err).To(Succeed())

		for _, p := range chapters {
			if p.Status == commonpb.ChapterStatus_CHAPTER_CLOSED {
				closedChapterID = p.GetId()

				return
			}
		}

		g.Expect(false).To(BeTrue(), "no CLOSED chapter found yet")
	}).Within(15 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

	// Archive the closed chapter.
	_, err = client.Apply(ctx, &servicepb.ApplyRequest{
		Envelopes: servicepb.UnsignedEnvelopes(actions.ArchiveChapterAction(closedChapterID)),
	})
	Expect(err).To(Succeed())

	// Wait for ARCHIVED.
	Eventually(func(g Gomega) {
		chapters, err := actions.ListAllChapters(ctx, client)
		g.Expect(err).To(Succeed())

		for _, p := range chapters {
			if p.GetId() == closedChapterID {
				g.Expect(p.Status).To(Equal(commonpb.ChapterStatus_CHAPTER_ARCHIVED))

				return
			}
		}

		g.Expect(false).To(BeTrue(), "archived chapter not found")
	}).Within(30 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
}

var _ = Describe("Audit entry purge after chapter archive", Ordered, func() {
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

	// This test verifies that audit entries are correctly purged when a chapter
	// is archived, even when log and audit sequence counters have diverged due
	// to batched proposals.
	//
	// The bug scenario: when a proposal batches N orders, it produces N logs but
	// only 1 audit entry. After several batched proposals, the log sequence
	// counter is much higher than the audit sequence counter. The chapter purge
	// uses log sequence ranges [startLogSeq, closeLogSeq] for the DeleteRange
	// on the audit prefix (0x02). But audit entries have lower sequence numbers
	// that fall below startLogSeq, so they are missed by the purge.
	//
	// This test creates the divergence by submitting batches of 5 transactions
	// (5 logs per 1 audit entry), then archives two chapters. Chapter 2 is where
	// the bug manifests: the purge range is too high to catch the audit entries.
	It("should purge all audit entries from chapter 2 after archive", func() {
		const ledger = "audit-purge-test"

		// Create ledger.
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledger, nil)),
		})
		Expect(err).To(Succeed())

		// Chapter 1: create transactions in batches to diverge counters.
		// Each Apply with 5 transactions = 1 proposal = 5 logs + 1 audit entry.
		for range 4 {
			txns := make([]*servicepb.Request, 5)
			for j := range txns {
				txns[j] = actions.CreateForceTransactionAction(ledger,
					[]*commonpb.Posting{
						actions.NewPosting("world", "users:alice", big.NewInt(100), "USD"),
					}, nil)
			}

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{Envelopes: servicepb.UnsignedEnvelopes(txns...)})
			Expect(err).To(Succeed())
		}

		entriesBefore, err := actions.ListAuditEntries(ctx, client, false)
		Expect(err).To(Succeed())
		Expect(len(entriesBefore)).To(BeNumerically(">=", 5), "should have audit entries from chapter 1")

		GinkgoWriter.Printf("Chapter 1: %d audit entries, max audit seq = %d\n",
			len(entriesBefore), entriesBefore[len(entriesBefore)-1].Sequence)

		// Archive chapter 1.
		archiveChapterFull(ctx, client)

		// Verify chapter 1 audit entries were purged.
		Eventually(func(g Gomega) {
			entries, err := actions.ListAuditEntries(ctx, client, false)
			g.Expect(err).To(Succeed())
			g.Expect(len(entries)).To(BeNumerically("<", len(entriesBefore)),
				"chapter 1 audit entries should have been purged")
		}).Within(10 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())

		// Chapter 2: create more batched transactions.
		for range 4 {
			txns := make([]*servicepb.Request, 5)
			for j := range txns {
				txns[j] = actions.CreateForceTransactionAction(ledger,
					[]*commonpb.Posting{
						actions.NewPosting("world", "users:bob", big.NewInt(100), "EUR"),
					}, nil)
			}

			_, err = client.Apply(ctx, &servicepb.ApplyRequest{Envelopes: servicepb.UnsignedEnvelopes(txns...)})
			Expect(err).To(Succeed())
		}

		entriesBeforeP2, err := actions.ListAuditEntries(ctx, client, false)
		Expect(err).To(Succeed())
		chapter2AuditCount := len(entriesBeforeP2)

		GinkgoWriter.Printf("Chapter 2 before archive: %d audit entries (max audit seq=%d)\n",
			chapter2AuditCount, entriesBeforeP2[chapter2AuditCount-1].Sequence)

		// Archive chapter 2.
		archiveChapterFull(ctx, client)

		// Verify chapter 2 audit entries were purged.
		// After archive, only audit entries from chapter 3 operations
		// (CloseChapter, ArchiveChapter, ConfirmArchive) should remain.
		Eventually(func(g Gomega) {
			entries, err := actions.ListAuditEntries(ctx, client, false)
			g.Expect(err).To(Succeed())

			GinkgoWriter.Printf("After chapter 2 archive: %d audit entries remain\n", len(entries))

			// Chapter 3 operations produce ~3 audit entries (close, archive, confirm).
			// If chapter 2 entries leaked, we'd see significantly more.
			g.Expect(len(entries)).To(BeNumerically("<=", 5),
				"chapter 2 audit entries should have been purged; "+
					"if more than 5 remain, the purge missed entries due to sequence counter divergence")
		}).Within(10 * time.Second).ProbeEvery(500 * time.Millisecond).Should(Succeed())
	})
})
