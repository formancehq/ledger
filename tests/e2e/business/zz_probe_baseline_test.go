//go:build e2e

package business

import (
	"context"
	"fmt"
	"math/big"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// Is the VOLUME_MISMATCH restore-specific, or does any store whose baseline sits
// past the archive boundary report it? Live node, no restore involved.
var _ = Describe("ZZProbeBaseline", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const ledger = "zz-probe-baseline"

	BeforeAll(func() {
		var node *testutil.ServiceWithClient
		ctx, node = testutil.SetupSingleNode(
			testserver.WithColdStorageDriver("filesystem"),
		)
		client = node.Client
	})

	It("checks a live store whose baseline is past the archive boundary", func() {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
		Expect(err).To(Succeed())

		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{
				actions.NewPosting("world", "acc:pre", big.NewInt(10), "USD"),
			}, nil, nil)))
		Expect(err).To(Succeed())

		// Chapter 1 archived.
		archiveChapterFull(ctx, client)

		// Activity after the archival, then a close so the baseline moves past the
		// archive boundary — the geometry the restored store was in.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{
				actions.NewPosting("world", "acc:post", big.NewInt(10), "USD"),
			}, nil, nil)))
		Expect(err).To(Succeed())

		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CloseChapterAction()))
		Expect(err).To(Succeed())

		Eventually(func(g Gomega) {
			chapters, err := actions.ListAllChapters(ctx, client)
			g.Expect(err).To(Succeed())

			for _, c := range chapters {
				if c.GetStatus() == commonpb.ChapterStatus_CHAPTER_CLOSED {
					return
				}
			}

			g.Expect(false).To(BeTrue(), "no sealed chapter yet")
		}).Should(Succeed())

		dump := func(label string) int {
			result, err := actions.CollectCheckStoreEvents(ctx, client)
			Expect(err).To(Succeed())

			f, _ := os.OpenFile("/var/tmp/probe-live.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			_, _ = f.WriteString(fmt.Sprintf("%s: errors=%d\n", label, len(result.Errors)))
			for _, e := range result.Errors {
				_, _ = f.WriteString(fmt.Sprintf("  %v | %s\n", e.GetErrorType(), e.GetMessage()))
			}
			_ = f.Close()

			return len(result.Errors)
		}

		// A: chapter 2 closed but NOT archived — baseline@close(2), boundary@close(1).
		dump("A closed-unarchived")

		// B: archive chapter 2, so the newest close IS the archive boundary.
		var closedID uint64
		chapters, err := actions.ListAllChapters(ctx, client)
		Expect(err).To(Succeed())
		for _, c := range chapters {
			if c.GetStatus() == commonpb.ChapterStatus_CHAPTER_CLOSED {
				closedID = c.GetId()
			}
		}
		Expect(closedID).NotTo(BeZero())
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.ArchiveChapterAction(closedID)))
		Expect(err).To(Succeed())
		Eventually(func(g Gomega) {
			cs, err := actions.ListAllChapters(ctx, client)
			g.Expect(err).To(Succeed())
			for _, c := range cs {
				if c.GetId() == closedID {
					g.Expect(c.GetStatus()).To(Equal(commonpb.ChapterStatus_CHAPTER_ARCHIVED))
					return
				}
			}
			g.Expect(false).To(BeTrue())
		}).Should(Succeed())
		dump("B all-closes-archived")

		// C: one more tx and a close, unarchived again.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{
				actions.NewPosting("world", "acc:post2", big.NewInt(10), "USD"),
			}, nil, nil)))
		Expect(err).To(Succeed())
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CloseChapterAction()))
		Expect(err).To(Succeed())
		Eventually(func(g Gomega) {
			cs, err := actions.ListAllChapters(ctx, client)
			g.Expect(err).To(Succeed())
			for _, c := range cs {
				if c.GetStatus() == commonpb.ChapterStatus_CHAPTER_CLOSED {
					return
				}
			}
			g.Expect(false).To(BeTrue())
		}).Should(Succeed())
		dump("C closed-unarchived-again")
	})
})
