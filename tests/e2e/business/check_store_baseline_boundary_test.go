//go:build e2e

package business

import (
	"context"
	"fmt"
	"math/big"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// store check seeds expected volumes with the baseline snapshot and replays
// everything above the archived boundary, so the baseline must be the state at
// exactly that boundary. It used to be overwritten at every chapter close: on any
// store with a closed-but-unarchived chapter — the lifecycle's normal
// intermediate state — every transaction between the boundary and the newest
// close was counted twice, and a healthy store was reported corrupt with one
// VOLUME_MISMATCH per touched account. The baseline is now staged per close and
// only promoted when the chapter's archival confirm moves the boundary onto it.
//
// The three phases walk the store through the alignments: boundary behind the
// newest close, boundary on it, and behind again.
var _ = Describe("CheckStoreBaselineBoundary", Ordered, func() {
	var (
		ctx    context.Context
		client servicepb.BucketServiceClient
	)

	const ledger = "baseline-boundary"

	BeforeAll(func() {
		var node *testutil.ServiceWithClient
		ctx, node = testutil.SetupSingleNode(
			testserver.WithColdStorageDriver("filesystem"),
		)
		client = node.Client
	})

	commitTx := func(dest string) {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{
				actions.NewPosting("world", dest, big.NewInt(10), "USD"),
			}, nil, nil)))
		Expect(err).To(Succeed())
	}

	closeAndAwaitSealed := func() {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CloseChapterAction()))
		Expect(err).To(Succeed())

		Eventually(func(g Gomega) {
			chapters, err := actions.ListAllChapters(ctx, client)
			g.Expect(err).To(Succeed())

			for _, chapter := range chapters {
				if chapter.GetStatus() == commonpb.ChapterStatus_CHAPTER_CLOSED {
					return
				}
			}

			g.Expect(false).To(BeTrue(), "no sealed chapter yet")
		}).Should(Succeed())
	}

	expectCleanCheck := func(label string) {
		result, err := actions.CollectCheckStoreEvents(ctx, client)
		Expect(err).To(Succeed())

		// Zero errors alone would also accept the degrade path: on a baseline that
		// does not match the archived boundary the checker skips entry-by-entry
		// verification and reports nothing. A healthy store in any boundary/close
		// alignment must get the full pass — the baseline must be present AND
		// aligned, which is exactly what promoting at the confirm provides.
		var checked uint64
		for _, progress := range result.Progress {
			if progress.GetLogsChecked() > checked {
				checked = progress.GetLogsChecked()
			}
		}
		Expect(checked).To(BeNumerically(">", 0),
			"%s: store check must have verified logs, not degraded on a missing or misaligned baseline", label)

		var messages []string
		for _, checkErr := range result.Errors {
			messages = append(messages, fmt.Sprintf("%v %s", checkErr.GetErrorType(), checkErr.GetMessage()))
		}

		Expect(messages).To(BeEmpty(), "%s: a healthy store must verify whatever the boundary/close alignment", label)
	}

	It("verifies cleanly with a closed chapter waiting above the archived boundary", func() {
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
		Expect(err).To(Succeed())

		commitTx("acc:pre")
		archiveChapterFull(ctx, client)

		// The lifecycle's normal intermediate state: transactions past the
		// boundary, then a close that is sealed but not archived. The baseline
		// staged at this close must NOT serve a boundary one chapter back.
		commitTx("acc:post")
		closeAndAwaitSealed()

		expectCleanCheck("closed chapter above the boundary")
	})

	It("verifies cleanly once the archival confirm promotes that close onto the boundary", func() {
		var closedID uint64

		chapters, err := actions.ListAllChapters(ctx, client)
		Expect(err).To(Succeed())
		for _, chapter := range chapters {
			if chapter.GetStatus() == commonpb.ChapterStatus_CHAPTER_CLOSED {
				closedID = chapter.GetId()
			}
		}
		Expect(closedID).NotTo(BeZero())

		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.ArchiveChapterAction(closedID)))
		Expect(err).To(Succeed())

		Eventually(func(g Gomega) {
			chapters, err := actions.ListAllChapters(ctx, client)
			g.Expect(err).To(Succeed())

			for _, chapter := range chapters {
				if chapter.GetId() == closedID {
					g.Expect(chapter.GetStatus()).To(Equal(commonpb.ChapterStatus_CHAPTER_ARCHIVED))

					return
				}
			}

			g.Expect(false).To(BeTrue())
		}).Should(Succeed())

		expectCleanCheck("boundary on the newest close")
	})

	It("verifies cleanly when another close lands above the moved boundary", func() {
		commitTx("acc:post2")
		closeAndAwaitSealed()

		expectCleanCheck("second closed chapter above the boundary")
	})
})
