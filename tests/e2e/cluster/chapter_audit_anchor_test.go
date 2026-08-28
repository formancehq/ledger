//go:build e2e

package cluster

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/zeebo/blake3"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// A chapter's audit anchor (last_audit_hash) is the chain input store check uses
// for the first audit entry that survives the chapter's purge, and one of the four
// fields its sealing hash commits to. So a sealed chapter must carry it, its row
// must reproduce its own sealing hash, and — since one committed seal is applied
// by every replica — all three must hold the same value.
//
// The anchor is known only to the apply that closes the chapter, and a replica can
// restart between that apply and the seal's; this pins the surviving contract from
// the outside, across a restart. The interleaving itself is not schedulable here:
// the seal completes inside the close's own Apply call, so the CLOSING state is
// not externally observable. TestApplyProposal_SealAfterRecoveryReproducesItsSealingHash
// drives that ordering directly, and the model test reaches it by killing nodes
// independently of the close.
var _ = Describe("ChapterAuditAnchor", Ordered, func() {
	const (
		countInstances = 3
		ledgerName     = "chapter-audit-anchor"
		leaderIdx      = 0
		followerIdx    = 1
		txCount        = 20
	)

	var (
		ctx     context.Context
		servers []*testutil.ServiceWithClient
	)

	BeforeAll(func() {
		ctx, servers, _, _ = testutil.SetupMultiNodeCluster(countInstances)
	})

	AfterAll(func() {
		testutil.StopServers(ctx, servers)
	})

	chapterFrom := func(idx int, id uint64) *commonpb.Chapter {
		chapters, err := actions.ListAllChapters(ctx, servers[idx].Client)
		Expect(err).To(Succeed())

		for _, chapter := range chapters {
			if chapter.GetId() == id {
				return chapter
			}
		}

		return nil
	}

	sealingHashOf := func(chapter *commonpb.Chapter) []byte {
		hasher := blake3.New()
		buf := make([]byte, 8)

		binary.BigEndian.PutUint64(buf, chapter.GetId())
		_, _ = hasher.Write(buf)
		binary.BigEndian.PutUint64(buf, chapter.GetCloseSequence())
		_, _ = hasher.Write(buf)

		if len(chapter.GetLastAuditHash()) > 0 {
			_, _ = hasher.Write(chapter.GetLastAuditHash())
		}

		_, _ = hasher.Write(chapter.GetStateHash())

		return hasher.Sum(nil)
	}

	It("keeps a sealed chapter verifiable on every replica across a restart", func() {
		_, err := servers[leaderIdx].Client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		// Audited history before the close, so the chapter closes over a non-empty
		// chain and its anchor is not legitimately empty.
		for i := range txCount {
			_, err = servers[leaderIdx].Client.Apply(ctx, servicepb.UnsignedApplyRequest("",
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", fmt.Sprintf("users:%d", i), big.NewInt(100), "USD"),
				}, nil, nil)))
			Expect(err).To(Succeed())
		}

		chapters, err := actions.ListAllChapters(ctx, servers[leaderIdx].Client)
		Expect(err).To(Succeed())

		var closingID uint64
		for _, chapter := range chapters {
			if chapter.GetStatus() == commonpb.ChapterStatus_CHAPTER_OPEN {
				closingID = chapter.GetId()
			}
		}
		Expect(closingID).NotTo(BeZero(), "the bucket must have an open chapter to close")

		_, err = servers[leaderIdx].Client.Apply(ctx, servicepb.UnsignedApplyRequest("",
			actions.CloseChapterAction()))
		Expect(err).To(Succeed())

		Eventually(func(g Gomega) {
			chapter := chapterFrom(leaderIdx, closingID)
			g.Expect(chapter).NotTo(BeNil())
			g.Expect(chapter.GetStatus()).To(Equal(commonpb.ChapterStatus_CHAPTER_CLOSED))
		}).Within(60 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

		// A replica that rebuilds its registry from Pebble must come back with the
		// anchor: the row is the only place it can survive a process.
		testutil.StopNode(ctx, servers[followerIdx])
		testutil.RestartNode(ctx, servers[followerIdx])

		Eventually(func(g Gomega) {
			chapter := chapterFrom(followerIdx, closingID)
			g.Expect(chapter).NotTo(BeNil())
			g.Expect(chapter.GetStatus()).To(Equal(commonpb.ChapterStatus_CHAPTER_CLOSED))
		}).Within(60 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

		expected := chapterFrom(leaderIdx, closingID)
		Expect(expected).NotTo(BeNil())
		Expect(expected.GetLastAuditHash()).NotTo(BeEmpty(),
			"a chapter sealed over audited history must carry the anchor store check chains from")

		for idx := range servers {
			chapter := chapterFrom(idx, closingID)
			Expect(chapter).NotTo(BeNil())
			Expect(chapter.GetLastAuditHash()).To(Equal(expected.GetLastAuditHash()),
				"node %d: one committed seal must land the same anchor on every replica", idx+1)
			Expect(sealingHashOf(chapter)).To(Equal(chapter.GetSealingHash()),
				"node %d: the sealed row must reproduce its own sealing hash", idx+1)
		}
	})
})
