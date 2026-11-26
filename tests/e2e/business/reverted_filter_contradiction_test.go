//go:build e2e

package business

import (
	"fmt"
	"math/big"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Reverted-filter set logic. The reverted condition is served from the reversion
// bitset (no index), composed with And/Or/Not via the merge iterators. A filter
// that is logically unsatisfiable — reverted=false AND reverted=true — must
// return no transactions, on its own and under boolean rewrites that reduce to
// the same contradiction. Reproduces a case the model-based antithesis driver
// flagged: and(not(not(reverted=false)), reverted=true) returned a row (EN-1597).
var _ = Describe("Reverted filter set logic", Ordered, func() {
	const ledgerName = "reverted-contradiction-ledger"

	var revertedIDs []uint64

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		// Six funding transactions to distinct accounts (ids assigned in order).
		var ids []uint64
		for i := 0; i < 6; i++ {
			resp, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", fmt.Sprintf("acc:%d", i), big.NewInt(100), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
			ids = append(ids, resp.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetId())
		}

		// Revert two originals so the reversion bitset is non-empty (each revert
		// also commits a compensating transaction, which is itself not reverted).
		for _, idx := range []int{1, 3} {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.RevertTransactionAction(ledgerName, ids[idx], true, false, nil)))
			Expect(err).To(Succeed())
			revertedIDs = append(revertedIDs, ids[idx])
		}
	})

	It("reverted=true returns exactly the reverted originals", func() {
		txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, actions.RevertedFilter(true))
		Expect(err).To(Succeed())
		Expect(idsOf(txs)).To(ConsistOf(revertedIDs))
	})

	It("and(reverted=false, reverted=true) is unsatisfiable and returns nothing", func() {
		f := actions.AndFilter(actions.RevertedFilter(false), actions.RevertedFilter(true))
		txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, f)
		Expect(err).To(Succeed())
		Expect(txs).To(BeEmpty())
	})

	It("and(reverted=true, reverted=false) is unsatisfiable and returns nothing", func() {
		f := actions.AndFilter(actions.RevertedFilter(true), actions.RevertedFilter(false))
		txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, f)
		Expect(err).To(Succeed())
		Expect(txs).To(BeEmpty())
	})

	It("and(not(not(reverted=false)), reverted=true) reduces to the same contradiction", func() {
		f := actions.AndFilter(actions.NotFilter(actions.NotFilter(actions.RevertedFilter(false))), actions.RevertedFilter(true))
		txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, f)
		Expect(err).To(Succeed())
		Expect(txs).To(BeEmpty())
	})
})
