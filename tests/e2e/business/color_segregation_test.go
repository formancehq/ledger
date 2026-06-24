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

// Color segregation invariant: a posting on (account, asset, color) only ever
// touches that bucket. Funds in color=GRANTS cannot satisfy a draw from
// color=OPS, and the uncolored bucket is itself segregated from any colored
// bucket. This test drives the invariant end to end via gRPC.
var _ = Describe("ColorSegregation", Ordered, func() {
	const ledgerName = "color-segregation"

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
		})
		Expect(err).To(Succeed())

		// Seed alice with three segregated buckets on USD/2:
		//   uncolored ""  : 100
		//   GRANTS         :  50
		//   OPS            :  25
		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewColoredPosting("world", "alice", big.NewInt(100), "USD/2", ""),
					actions.NewColoredPosting("world", "alice", big.NewInt(50), "USD/2", "GRANTS"),
					actions.NewColoredPosting("world", "alice", big.NewInt(25), "USD/2", "OPS"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())
	})

	It("Should expose every (asset, color) bucket on GetAccount", func() {
		Eventually(func(g Gomega) {
			acct, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "alice",
			})
			g.Expect(err).To(Succeed())

			uncolored := acct.FindVolume("USD/2", "")
			grants := acct.FindVolume("USD/2", "GRANTS")
			ops := acct.FindVolume("USD/2", "OPS")

			g.Expect(uncolored).NotTo(BeNil())
			g.Expect(grants).NotTo(BeNil())
			g.Expect(ops).NotTo(BeNil())

			g.Expect(uncolored.GetBalance()).To(Equal("100"))
			g.Expect(grants.GetBalance()).To(Equal("50"))
			g.Expect(ops.GetBalance()).To(Equal("25"))

			// volumes list must be sorted deterministically by (asset, color)
			vols := acct.GetVolumes()
			g.Expect(vols).To(HaveLen(3))
			g.Expect(vols[0].GetColor()).To(Equal(""))
			g.Expect(vols[1].GetColor()).To(Equal("GRANTS"))
			g.Expect(vols[2].GetColor()).To(Equal("OPS"))
		}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})

	It("Should collapse colors into a single per-asset entry when requested", func() {
		acct, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
			Ledger:         ledgerName,
			Address:        "alice",
			CollapseColors: true,
		})
		Expect(err).To(Succeed())

		// All three buckets summed under color = ""
		Expect(acct.GetVolumes()).To(HaveLen(1))
		entry := acct.GetVolumes()[0]
		Expect(entry.GetAsset()).To(Equal("USD/2"))
		Expect(entry.GetColor()).To(Equal(""))
		Expect(entry.GetVolumes().GetBalance()).To(Equal("175")) // 100 + 50 + 25
	})

	It("Should reject a draw from a color that has insufficient funds", func() {
		// alice's OPS bucket has 25; ask for 100 OPS → MissingFunds.
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewColoredPosting("alice", "bob", big.NewInt(100), "USD/2", "OPS"),
				}, nil, nil),
			),
		})
		Expect(err).NotTo(BeNil(), "expected color isolation to refuse spending more than the bucket holds")
	})

	It("Should refuse drawing colored funds from the uncolored bucket", func() {
		// alice's uncolored bucket has 100; drawing 100 from "" should succeed.
		// But drawing 100 from "GRANTS" must NOT dip into the 100 uncolored.
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewColoredPosting("alice", "bob", big.NewInt(100), "USD/2", "GRANTS"),
				}, nil, nil),
			),
		})
		Expect(err).NotTo(BeNil(), "uncolored funds must not satisfy a GRANTS-colored draw")
	})

	It("Should drain a color independently of the others", func() {
		// Spend exactly the GRANTS bucket (50) → success.
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewColoredPosting("alice", "bob", big.NewInt(50), "USD/2", "GRANTS"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())

		Eventually(func(g Gomega) {
			alice, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "alice",
			})
			g.Expect(err).To(Succeed())

			// GRANTS is drained; other buckets are untouched.
			g.Expect(alice.FindVolume("USD/2", "GRANTS").GetBalance()).To(Equal("0"))
			g.Expect(alice.FindVolume("USD/2", "").GetBalance()).To(Equal("100"))
			g.Expect(alice.FindVolume("USD/2", "OPS").GetBalance()).To(Equal("25"))

			// bob received under GRANTS, color preserved on the destination side.
			bob, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "bob",
			})
			g.Expect(err).To(Succeed())
			g.Expect(bob.FindVolume("USD/2", "GRANTS").GetBalance()).To(Equal("50"))
			g.Expect(bob.FindVolume("USD/2", "")).To(BeNil(),
				"bob must not have an uncolored USD/2 bucket — color stays with the funds")
		}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})

	It("Should expose color on the emitted posting", func() {
		// Re-fetch the GRANTS transfer we just executed and verify the
		// posting still carries color = "GRANTS" on the wire.
		resp, err := sharedClient.ListTransactions(sharedCtx, &servicepb.ListTransactionsRequest{
			Ledger:  ledgerName,
			Options: &commonpb.ListOptions{PageSize: 32},
		})
		Expect(err).To(Succeed())

		var foundColored bool
		for {
			tx, recvErr := resp.Recv()
			if recvErr != nil {
				break
			}
			for _, p := range tx.GetPostings() {
				if p.GetColor() == "GRANTS" {
					foundColored = true
				}
			}
		}
		Expect(foundColored).To(BeTrue(),
			"expected at least one persisted posting with color = GRANTS")
	})
})
