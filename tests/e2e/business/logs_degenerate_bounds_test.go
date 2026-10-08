//go:build e2e

package business

import (
	"context"
	"io"
	"math/big"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// A range condition whose bounds cross — an exclusive interval (x, x) —
// matches nothing, so its complement is the whole universe: not(logId in
// (x, x)) must list every log the unfiltered read lists.
var _ = Describe("Degenerate range bounds resolve to the empty match", Ordered, func() {
	var (
		ctx    context.Context
		client ledgerpb.BucketServiceClient
	)

	const ledger = "degenerate-bounds"

	BeforeAll(func() {
		var node *testutil.ServiceWithClient
		ctx, node = testutil.SetupSingleNode()
		client = node.Client
	})

	countLogs := func(g Gomega, filter *ledgerpb.QueryFilter) int {
		stream, err := client.ListLogs(ctx, &ledgerpb.ListLogsRequest{
			Ledger:  ledger,
			Options: &ledgerpb.ListOptions{Filter: filter},
		})
		g.Expect(err).To(Succeed())

		count := 0
		for {
			_, re := stream.Recv()
			if re == io.EOF {
				break
			}
			g.Expect(re).To(Succeed(), "streaming under a degenerate-bounds complement must not error")
			count++
		}

		return count
	}

	It("lists the whole universe under not() of an empty interval", func() {
		_, err := client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledger, nil)))
		Expect(err).To(Succeed())

		for range 3 {
			_, err = client.Apply(ctx, ledgerpb.UnsignedApplyRequest("",
				actions.CreateForceTransactionAction(ledger, []*ledgerpb.Posting{
					actions.NewPosting("world", "acc:1", big.NewInt(10), "USD"),
				}, nil)))
			Expect(err).To(Succeed())
		}

		bound := uint64(2)
		notEmptyInterval := &ledgerpb.QueryFilter{Filter: &ledgerpb.QueryFilter_Not{Not: &ledgerpb.NotFilter{
			Filter: &ledgerpb.QueryFilter{Filter: &ledgerpb.QueryFilter_LogId{LogId: &ledgerpb.LogIdCondition{
				Cond: &ledgerpb.UintCondition{
					Min: &bound, MinExclusive: true,
					Max: &bound, MaxExclusive: true,
				},
			}}},
		}}}

		Eventually(func(g Gomega) {
			all := countLogs(g, nil)
			g.Expect(all).To(BeNumerically(">=", 3), "the three committed transactions")
			g.Expect(countLogs(g, notEmptyInterval)).To(Equal(all),
				"not(empty interval) must match every log the unfiltered read lists")
		}).Should(Succeed())
	})
})
