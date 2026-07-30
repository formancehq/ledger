//go:build e2e

package business

import (
	"math/big"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// The index builder's live log cursor reuses one proto message per scan
// (dal.WithReuse + resetLogForReuse). A log whose wire bytes carry no volume
// annotations must not inherit the PREVIOUS log's purged/ephemeral lists:
// those feed excludedForLog, whose (account, asset, color) exclusion keys
// carry no ledger dimension, so a stale entry silently skips the posting-index
// writes of whatever log the cursor reads next.
//
// The trigger here is deterministic: one cross-ledger bulk commits an
// ephemeral wash on ledger A (its log carries purged=[e:1 USD]) and a plain
// funding of the same-named account on ledger B (its log carries no purged
// list). The two logs are adjacent in the same proposal, hence always read
// back-to-back through one reused cursor message; with a leaky reset, B's
// funding inherits A's purged entry and loses its destination-address row —
// permanently, since nothing later repairs index rows.
var _ = Describe("Address index across a cross-ledger purge bulk", Ordered, func() {
	const (
		ledgerA = "idx-reuse-leak-a"
		ledgerB = "idx-reuse-leak-b"
	)

	roleFilter := func(addr string, role commonpb.AddressRole) *commonpb.QueryFilter {
		f := actions.AddressExactFilter(addr)
		f.GetAddress().Role = role

		return f
	}

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateLedgerAction(ledgerA, nil),
			actions.CreateLedgerAction(ledgerB, nil),
			actions.AddEphemeralAccountTypeAction(ledgerA, "e", "e:{id}"),
			actions.CreateAddressIndexAction(ledgerB, commonpb.AddressRole_ADDRESS_ROLE_DESTINATION),
			actions.CreateAddressIndexAction(ledgerB, commonpb.AddressRole_ADDRESS_ROLE_ANY),
		))
		Expect(err).To(Succeed())

		// Wait for ledger B's indexes so the bulk below is indexed by the LIVE
		// path (the backfill parses raw wire bytes with its own reset and is
		// not subject to the reuse leak).
		Eventually(func(g Gomega) {
			indexes, err := listLedgerIndexes(sharedCtx, sharedClient, ledgerB)
			g.Expect(err).To(Succeed())
			g.Expect(hasTxBuiltinIndex(indexes, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS)).To(BeTrue())
			g.Expect(hasTxBuiltinIndex(indexes, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS)).To(BeTrue())
		}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

		// One bulk, two orders, two adjacent logs: the ledger-A wash ends its
		// (e:1, USD) cell at zero — purged, so its log carries the annotation —
		// while the ledger-B funding leaves its own (e:1, USD) cell non-zero
		// and must be indexed.
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateTransactionAction(ledgerA, []*commonpb.Posting{
				actions.NewPosting("world", "e:1", big.NewInt(5), "USD"),
				actions.NewPosting("e:1", "world", big.NewInt(5), "USD"),
			}, nil, nil),
			actions.CreateTransactionAction(ledgerB, []*commonpb.Posting{
				actions.NewPosting("world", "e:1", big.NewInt(5), "USD"),
			}, nil, nil),
		))
		Expect(err).To(Succeed())
	})

	It("indexes the sibling ledger's funding despite the adjacent purge log", func() {
		Eventually(func(g Gomega) {
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerB, 0, 0,
				roleFilter("e:1", commonpb.AddressRole_ADDRESS_ROLE_DESTINATION))
			g.Expect(err).To(Succeed())

			ids := make([]uint64, len(txs))
			for i, tx := range txs {
				ids[i] = tx.GetId()
			}
			g.Expect(ids).To(ConsistOf(uint64(1)))
		}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})
})
