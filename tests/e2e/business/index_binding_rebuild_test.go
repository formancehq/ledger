//go:build e2e

package business

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// The read store is a WAL-less Pebble database: a hard kill rewinds it to its
// last flush. A flush that captured an index SERVING (current != 0) with a
// fold cursor below later retype logs reopens exactly there — the node starts
// serving the flush-era binding immediately, and re-walks the retypes only as
// its fold catches back up. While that walk runs, the node must never SERVE a
// binding more than one schema revision behind at a state past the retypes:
// it reads as INDEX_BUILDING instead.
//
// The rewind is manufactured deterministically: the read-index directory is
// copied at "index ready, retypes not yet applied" (a graceful stop flushes,
// so the copy IS a valid flush point) and swapped back in after the retypes.
//
// This spec is a contract guard, not the detector: on an idle in-process
// store the re-walk completes faster than a client can observe it, so the
// mid-walk serving states are not reachable deterministically through the
// public API — the compile-gate unit tests (TestCompile_StaleBindingReadsAs-
// Building) drive those states directly, and the Antithesis model driver
// covers them continuously under real load. What this spec pins is the
// end-to-end contract around the rewind: the node comes back, never emits a
// creation-era page, and converges to the current binding.
//
// The chain INT32 → UINT32 → INT8 makes the creation-era binding the only
// distinguishable one for the value -300 under the query [-1000, 0]:
//
//   - INT32 (creation): -300 indexed as an int, matches → serving it leaks.
//   - UINT32 (legal one-revision window): a negative bound is a compilation
//     rejection, and -300 coerces to null anyway.
//   - INT8 (converged): -300 overflows to null — a clean empty page.
var _ = Describe("Index binding across a read-store rebuild", Ordered, func() {
	var (
		ctx  context.Context
		node *testutil.ServiceWithClient
	)

	const (
		ledger = "bind-rebuild"
		key    = "grade"
		filler = 15000
	)

	acct := commonpb.TargetType_TARGET_TYPE_ACCOUNT

	BeforeAll(func() {
		ctx, node = testutil.SetupSingleNode()
	})

	apply := func(reqs ...*servicepb.Request) {
		_, err := node.Client.Apply(ctx, servicepb.UnsignedApplyRequest("", reqs...))
		Expect(err).To(Succeed())
	}

	retype := func(t commonpb.MetadataType) {
		apply(actions.SetMetadataFieldTypeAction(ledger, acct, key, t))
		Expect(actions.WaitForMetadataIndexReady(ctx, node.Client, ledger, acct, key)).To(Succeed())
	}

	lo, hi := int64(-1000), int64(0)
	negRange := actions.Int64RangeMetadataFilter(key, &lo, &hi)

	It("converges after a read-store rewind without ever emitting a creation-era page", func() {
		apply(actions.CreateLedgerWithSchemaAction(ledger, nil, []*commonpb.SetMetadataFieldTypeCommand{
			{TargetType: acct, Key: key, Type: commonpb.MetadataType_METADATA_TYPE_INT32},
		}))
		apply(actions.CreateAccountMetadataIndexAction(ledger, key))
		Expect(actions.WaitForMetadataIndexReady(ctx, node.Client, ledger, acct, key)).To(Succeed())

		// -300 is the discriminator (see the chain rationale above); 7 keeps a
		// row that survives every binding.
		apply(actions.SaveAccountMetadataAction(ledger, "acc:neg", map[string]string{key: "-300"}))
		apply(actions.SaveAccountMetadataAction(ledger, "acc:ok", map[string]string{key: "7"}))

		// Discriminator sanity under the creation binding: -300 is indexed as
		// an INT32 and the range matches it. Without this, a silently
		// unindexed value would make the whole spec vacuous.
		Eventually(func(g Gomega) {
			accounts, err := actions.ListAccountsFiltered(ctx, node.Client, ledger, 0, "", negRange)
			g.Expect(err).To(Succeed())
			g.Expect(accounts).To(HaveLen(1))
			g.Expect(accounts[0].GetAddress()).To(Equal("acc:neg"))
		}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

		// The flush point to rewind to: index serving the creation binding,
		// retypes not yet applied. A graceful stop flushes the read store, so
		// the copied directory is exactly what a hard kill would have kept.
		readIndexDir := filepath.Join(node.DataDir, "read-indexes")
		flushPoint := readIndexDir + ".flush-point"
		testutil.StopNode(ctx, node)
		Expect(exec.Command("cp", "-a", readIndexDir, flushPoint).Run()).To(Succeed())
		testutil.RestartNode(ctx, node)

		// Filler between the flush point and the retypes: indexed metadata
		// rows. After the rewind their logs are re-folded and — the part that
		// paces the walk — each retype's rewrite must re-encode every one of
		// them under its target type in budgeted ticks, which is the stretch
		// where the flush-era binding stays the serving one.
		for start := 0; start < filler; start += 100 {
			var batch []*servicepb.Request
			for i := start; i < start+100 && i < filler; i++ {
				batch = append(batch, actions.SaveAccountMetadataAction(ledger, fmt.Sprintf("acc:f%d", i),
					map[string]string{key: fmt.Sprintf("%d", i%100+1)}))
			}
			apply(batch...)
		}

		retype(commonpb.MetadataType_METADATA_TYPE_UINT32)
		retype(commonpb.MetadataType_METADATA_TYPE_INT8)

		// Converged semantics: -300 overflows INT8 to null — the range is empty.
		Eventually(func(g Gomega) {
			accounts, err := actions.ListAccountsFiltered(ctx, node.Client, ledger, 0, "", negRange)
			g.Expect(err).To(Succeed())
			g.Expect(accounts).To(BeEmpty())
		}).Within(20 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

		// The rewind: swap the flush point back in. The node reopens with the
		// creation-era binding CURRENT and a fold cursor below both retypes.
		testutil.StopNode(ctx, node)
		Expect(os.RemoveAll(readIndexDir)).To(Succeed())
		Expect(os.Rename(flushPoint, readIndexDir)).To(Succeed())
		testutil.RestartNode(ctx, node)

		// Stale consistency skips only the Raft barrier; a filtered read still
		// fold-aligns its index snapshot, so each probe returns only once the
		// rewound node's fold covers the main store — post-alignment state, not
		// the walk itself (the header above concedes mid-walk states are not
		// reachable through the public API). What the loop pins is exactly the
		// post-alignment contract: probes may refuse with the walk's legal
		// retryable errors, and no page — ever — holds acc:neg, the
		// creation-era binding's discriminator.
		staleCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "stale")

		Eventually(func(g Gomega) {
			accounts, err := actions.ListAccountsFiltered(staleCtx, node.Client, ledger, 0, "", negRange)
			g.Expect(err).To(Succeed()) // legal retryable refusals — retry

			for _, a := range accounts {
				if a.GetAddress() == "acc:neg" {
					StopTrying("the rebuilt node served the creation-era INT32 binding at a post-retype state").Now()
				}
			}

			g.Expect(accounts).To(BeEmpty(), "not yet converged to the current binding")
		}).Within(30*time.Second).ProbeEvery(2*time.Millisecond).Should(Succeed(),
			"the rebuilt node must converge without ever serving the creation-era binding")
	})
})
