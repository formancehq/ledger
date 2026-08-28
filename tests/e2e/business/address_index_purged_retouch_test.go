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

// An ephemeral account whose volume cell was purged (drained to zero) and then
// re-touched by a later transaction must be reachable through the address
// indexes for that later transaction: its cell survives the second commit
// (non-zero via force), so the posting is not in the exclusion projection and
// the account→tx mapping row must exist (EN-1625, found by
// singleton_driver_model: source-role window missing exactly the post-purge
// transaction).
var _ = Describe("Address index after ephemeral purge and re-touch", Ordered, func() {
	const ledgerName = "idx-src-purged-retouch"

	roleFilter := func(addr string, role commonpb.AddressRole) *commonpb.QueryFilter {
		f := actions.AddressExactFilter(addr)
		f.GetAddress().Role = role

		return f
	}

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateLedgerAction(ledgerName, nil),
			actions.AddEphemeralAccountTypeAction(ledgerName, "e", "e:{id}"),
			actions.CreateAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_SOURCE),
			actions.CreateAddressIndexAction(ledgerName, commonpb.AddressRole_ADDRESS_ROLE_ANY),
		))
		Expect(err).To(Succeed())

		// tx 1: fund e:1 (kept non-zero). tx 2: drain to zero — the cell is
		// purged, so tx 2's e:1 rows are excluded. tx 3: force-drain the purged
		// cell into a non-zero (negative) balance — kept, so tx 3's e:1 rows
		// must be written. Separate bulks, adjacent logs, mirroring the driver
		// sequence that surfaced the miss.
		for _, req := range []*servicepb.Request{
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "e:1", big.NewInt(5), "USD"),
			}, nil, nil),
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("e:1", "world", big.NewInt(5), "USD"),
			}, nil, nil),
			actions.CreateForceTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("e:1", "world", big.NewInt(5), "USD"),
			}, nil),
		} {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", req))
			Expect(err).To(Succeed())
		}
	})

	It("source filter reaches the post-purge transaction", func() {
		Eventually(func(g Gomega) {
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0,
				roleFilter("e:1", commonpb.AddressRole_ADDRESS_ROLE_SOURCE))
			g.Expect(err).To(Succeed())

			ids := make([]uint64, len(txs))
			for i, tx := range txs {
				ids[i] = tx.GetId()
			}
			// tx 2's rows are excluded (purged); tx 3's must exist.
			g.Expect(ids).To(ConsistOf(uint64(3)))
		}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})

	It("any-role filter sees the funding and the post-purge transaction", func() {
		Eventually(func(g Gomega) {
			txs, err := actions.ListTransactionsFiltered(sharedCtx, sharedClient, ledgerName, 0, 0,
				roleFilter("e:1", commonpb.AddressRole_ADDRESS_ROLE_ANY))
			g.Expect(err).To(Succeed())

			ids := make([]uint64, len(txs))
			for i, tx := range txs {
				ids[i] = tx.GetId()
			}
			g.Expect(ids).To(ConsistOf(uint64(1), uint64(3)))
		}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})
})
