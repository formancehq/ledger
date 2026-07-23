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

// Nested-NOT filters over index-free leaves (tx-id range, account address).
// A NOT drives its child forward — exhausting a finite leaf — before the AND
// merge seeks the tree back to a smaller key; the leaf must reposition on that
// backward SeekGE rather than latch on exhaustion (EN-1597). Each filter here
// is a contradiction (`and(not(F), F)`, spelled with an extra NOT to force the
// exhaust-then-seek-back path) and must return nothing.
var _ = Describe("Nested-NOT filter reposition", Ordered, ContinueOnFailure, func() {
	const ledgerName = "nested-not-reposition-ledger"

	tripleNot := func(f *commonpb.QueryFilter) *commonpb.QueryFilter {
		return actions.NotFilter(actions.NotFilter(actions.NotFilter(f)))
	}

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		for i := 0; i < 8; i++ {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", fmt.Sprintf("acc:%d", i), big.NewInt(100), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
		}
	})

	It("tx-id range: and(not(not(not(id[3,5]))), id[3,5]) is empty", func() {
		f := actions.AndFilter(tripleNot(actions.TxIDRangeFilter(3, 5)), actions.TxIDRangeFilter(3, 5))
		txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, f)
		Expect(err).To(Succeed())
		Expect(txs).To(BeEmpty())
	})

	It("tx-id range: and(not(not(id[3,5])), not(id[3,5])) is empty", func() {
		f := actions.AndFilter(actions.NotFilter(actions.NotFilter(actions.TxIDRangeFilter(3, 5))), actions.NotFilter(actions.TxIDRangeFilter(3, 5)))
		txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, f)
		Expect(err).To(Succeed())
		Expect(txs).To(BeEmpty())
	})

	It("address: and(not(not(not(addr^acc:))), addr^acc:) is empty", func() {
		f := actions.AndFilter(tripleNot(actions.AddressPrefixFilter("acc:")), actions.AddressPrefixFilter("acc:"))
		accts, err := actions.ListAccountsFiltered(sharedCtx, sharedClient, ledgerName, 0, "", f)
		Expect(err).To(Succeed())
		Expect(accts).To(BeEmpty())
	})

	It("address: and(not(not(addr^acc:)), not(addr^acc:)) is empty", func() {
		f := actions.AndFilter(actions.NotFilter(actions.NotFilter(actions.AddressPrefixFilter("acc:"))), actions.NotFilter(actions.AddressPrefixFilter("acc:")))
		accts, err := actions.ListAccountsFiltered(sharedCtx, sharedClient, ledgerName, 0, "", f)
		Expect(err).To(Succeed())
		Expect(accts).To(BeEmpty())
	})
})
