//go:build e2e

package business

import (
	"fmt"
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// Nested-NOT filters over three leaf shapes: tx-id range and account address
// (index-free, main-store scans), and address-on-transactions (through the
// tx-address index and AddressTxIterator). A NOT drives its child forward —
// exhausting a finite leaf — before the AND merge seeks the tree back to a
// smaller key; the leaf must reposition on that backward SeekGE rather than
// latch on exhaustion (EN-1597). Each filter here is a contradiction
// (`and(not(F), F)`, spelled with an extra NOT to force the
// exhaust-then-seek-back path) and must return nothing.
var _ = Describe("Nested-NOT filter reposition", Ordered, func() {
	const ledgerName = "nested-not-reposition-ledger"

	tripleNot := func(f *commonpb.QueryFilter) *commonpb.QueryFilter {
		return actions.NotFilter(actions.NotFilter(actions.NotFilter(f)))
	}

	// The trailing guard aborts the container on failure (a BeforeAll failure
	// skips every spec), so the BeEmpty contradictions below can never pass
	// vacuously against a view that lost the seeded data. The tx-id and
	// account-address leaves read the main store's attributes zone, written
	// synchronously by the FSM before Apply returns — for those the Eventually
	// is a freshness backstop. The address-on-transactions leaf compiles only
	// once the tx-address index is READY on this replica, which the explicit
	// wait guarantees.
	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_ANY)))
		Expect(err).To(Succeed())

		for i := 0; i < 8; i++ {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", fmt.Sprintf("acc:%d", i), big.NewInt(100), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
		}

		Expect(actions.WaitForAddressIndexReady(sharedCtx, sharedClient, ledgerName, commonpb.AddressRole_ADDRESS_ROLE_ANY)).To(Succeed())

		Eventually(func(g Gomega) {
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, actions.TxIDRangeFilter(3, 5))
			g.Expect(err).To(Succeed())
			g.Expect(txs).To(HaveLen(3))

			accts, err := actions.ListAccountsFiltered(sharedCtx, sharedClient, ledgerName, 0, "", actions.AddressPrefixFilter("acc:"))
			g.Expect(err).To(Succeed())
			g.Expect(accts).To(HaveLen(8))

			txsByAddr, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, actions.AddressPrefixFilter("acc:"))
			g.Expect(err).To(Succeed())
			g.Expect(txsByAddr).To(HaveLen(8))
		}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
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

	// On the TRANSACTIONS target an address leaf compiles to
	// NotIterator{universe: PebbleTxIterator, child: AddressTxIterator} — the
	// materialized-union iterator whose SeekGE must be an absolute reposition.
	// These two specs pin the compile-path reachability (no other e2e builds an
	// AddressTxIterator); the absolute-seek conformance itself is pinned by the
	// unit tests in readstore/iterator_address_test.go.
	It("tx-address: and(not(not(not(addr^acc:))), addr^acc:) is empty on transactions", func() {
		f := actions.AndFilter(tripleNot(actions.AddressPrefixFilter("acc:")), actions.AddressPrefixFilter("acc:"))
		txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, f)
		Expect(err).To(Succeed())
		Expect(txs).To(BeEmpty())
	})

	It("tx-address: and(not(not(addr^acc:)), not(addr^acc:)) is empty on transactions", func() {
		f := actions.AndFilter(actions.NotFilter(actions.NotFilter(actions.AddressPrefixFilter("acc:"))), actions.NotFilter(actions.AddressPrefixFilter("acc:")))
		txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0, f)
		Expect(err).To(Succeed())
		Expect(txs).To(BeEmpty())
	})
})
