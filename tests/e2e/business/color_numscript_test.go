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

// Color × Numscript: the color dimension flows through the Numscript adapter
// the same way it flows through native postings. A script restricting its
// source to a color must consume from exactly that bucket, and the resulting
// posting must carry the color all the way to the FSM and back through the
// read side.
var _ = Describe("ColorNumscript", Ordered, func() {
	const ledgerName = "color-numscript"

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
		})
		Expect(err).To(Succeed())

		// Seed alice with three segregated buckets on USD/2:
		//   uncolored ""  : 300
		//   GRANTS         : 200
		//   OPS            : 100
		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewColoredPosting("world", "alice", big.NewInt(300), "USD/2", ""),
					actions.NewColoredPosting("world", "alice", big.NewInt(200), "USD/2", "GRANTS"),
					actions.NewColoredPosting("world", "alice", big.NewInt(100), "USD/2", "OPS"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())
	})

	It("Should restrict the numscript source to a single color bucket", func() {
		// Draw 60 from alice's GRANTS bucket via Numscript. The resulting
		// posting must carry color = "GRANTS", and only the GRANTS bucket
		// must move — uncolored and OPS must be untouched.
		script := `
#![feature("experimental-asset-colors")]

send [USD/2 60] (
  source = @alice \ "GRANTS"
  destination = @bob
)
`
		resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil),
			),
		})
		Expect(err).To(Succeed())
		Expect(resp.Logs).To(HaveLen(1))

		createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
		Expect(createdTx).NotTo(BeNil())
		Expect(createdTx.Transaction.Postings).To(HaveLen(1))
		Expect(createdTx.Transaction.Postings[0].GetSource()).To(Equal("alice"))
		Expect(createdTx.Transaction.Postings[0].GetDestination()).To(Equal("bob"))
		Expect(createdTx.Transaction.Postings[0].GetAsset()).To(Equal("USD/2"))
		Expect(createdTx.Transaction.Postings[0].GetColor()).To(Equal("GRANTS"),
			"the color restriction on the source must propagate to the emitted posting")
		Expect(createdTx.Transaction.Postings[0].Amount.ToBigInt().Int64()).To(Equal(int64(60)))

		Eventually(func(g Gomega) {
			alice, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "alice",
			})
			g.Expect(err).To(Succeed())

			// GRANTS drained by 60 (200 - 60 = 140); others unchanged.
			g.Expect(alice.FindVolume("USD/2", "GRANTS").GetBalance()).To(Equal("140"))
			g.Expect(alice.FindVolume("USD/2", "").GetBalance()).To(Equal("300"))
			g.Expect(alice.FindVolume("USD/2", "OPS").GetBalance()).To(Equal("100"))

			// bob received under the same color and only under that color.
			bob, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "bob",
			})
			g.Expect(err).To(Succeed())
			g.Expect(bob.FindVolume("USD/2", "GRANTS").GetBalance()).To(Equal("60"))
			g.Expect(bob.FindVolume("USD/2", "")).To(BeNil(),
				"the color stays with the funds — bob must not have an uncolored bucket")
		}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})

	It("Should refuse a numscript draw when the colored bucket is empty", func() {
		// MISSING is a color alice never received; the script must fail
		// rather than fall back to other buckets.
		script := `
#![feature("experimental-asset-colors")]

send [USD/2 1] (
  source = @alice \ "MISSING"
  destination = @bob
)
`
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil),
			),
		})
		Expect(err).To(HaveOccurred(),
			"a colored draw must not be satisfied from uncolored funds even via numscript")
	})

	It("Should treat the uncolored bucket as its own color in numscript", func() {
		// Drawing from @alice without a restrict must consume the
		// uncolored bucket only — the colored buckets stay segregated
		// from the default scope.
		script := `
send [USD/2 90] (
  source = @alice
  destination = @bob
)
`
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateScriptTransactionAction(ledgerName, script, nil, nil),
			),
		})
		Expect(err).To(Succeed())

		Eventually(func(g Gomega) {
			alice, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "alice",
			})
			g.Expect(err).To(Succeed())

			// Uncolored shrunk by 90 (300 - 90 = 210); colored stays intact.
			g.Expect(alice.FindVolume("USD/2", "").GetBalance()).To(Equal("210"))
			g.Expect(alice.FindVolume("USD/2", "GRANTS").GetBalance()).To(Equal("140"))
			g.Expect(alice.FindVolume("USD/2", "OPS").GetBalance()).To(Equal("100"))
		}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})

	It("Should expose every (asset, color) bucket when GetAccount is called without collapse", func() {
		acct, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
			Ledger:  ledgerName,
			Address: "alice",
		})
		Expect(err).To(Succeed())

		// Volumes list is sorted (asset, color); three buckets for alice.
		vols := acct.GetVolumes()
		Expect(vols).To(HaveLen(3))
		Expect(vols[0].GetAsset()).To(Equal("USD/2"))
		Expect(vols[0].GetColor()).To(Equal(""))
		Expect(vols[0].GetVolumes().GetBalance()).To(Equal("210"))
		Expect(vols[1].GetColor()).To(Equal("GRANTS"))
		Expect(vols[1].GetVolumes().GetBalance()).To(Equal("140"))
		Expect(vols[2].GetColor()).To(Equal("OPS"))
		Expect(vols[2].GetVolumes().GetBalance()).To(Equal("100"))
	})

	It("Should collapse colors on GetAccount when collapseColors=true", func() {
		acct, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
			Ledger:         ledgerName,
			Address:        "alice",
			CollapseColors: true,
		})
		Expect(err).To(Succeed())

		// All three buckets summed under color = "" (210 + 140 + 100 = 450)
		Expect(acct.GetVolumes()).To(HaveLen(1))
		entry := acct.GetVolumes()[0]
		Expect(entry.GetAsset()).To(Equal("USD/2"))
		Expect(entry.GetColor()).To(Equal(""))
		Expect(entry.GetVolumes().GetBalance()).To(Equal("450"))
	})
})

// AggregateVolumes × color: the aggregate endpoint preserves the color
// dimension by default, and collapses it to a single per-asset entry when
// asked. These tests pin both shapes to lock the contract.
var _ = Describe("AggregateVolumesColor", Ordered, func() {
	const ledgerName = "agg-vol-color"

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
		})
		Expect(err).To(Succeed())

		// Fund three (asset, color) buckets on alice. We mix assets and
		// colors so the aggregator must handle both axes:
		//   alice / USD/2 / ""       :  10
		//   alice / USD/2 / "GRANTS" : 100
		//   alice / USD/2 / "OPS"    :  40
		//   alice / EUR/2 / "GRANTS" :  50
		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewColoredPosting("world", "alice", big.NewInt(10), "USD/2", ""),
					actions.NewColoredPosting("world", "alice", big.NewInt(100), "USD/2", "GRANTS"),
					actions.NewColoredPosting("world", "alice", big.NewInt(40), "USD/2", "OPS"),
					actions.NewColoredPosting("world", "alice", big.NewInt(50), "EUR/2", "GRANTS"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())
	})

	It("Should return one entry per (asset, color) by default", func() {
		Eventually(func(g Gomega) {
			result, err := sharedClient.AggregateVolumes(sharedCtx, &servicepb.AggregateVolumesRequest{
				Ledger: ledgerName,
			})
			g.Expect(err).To(Succeed())
			// 3 USD/2 buckets + 1 EUR/2 bucket = 4 entries.
			g.Expect(result.Volumes).To(HaveLen(4))

			type key struct{ asset, color string }
			byKey := make(map[key]*commonpb.AggregatedVolume, len(result.Volumes))
			for _, v := range result.Volumes {
				byKey[key{v.GetAsset(), v.GetColor()}] = v
			}

			usdUncolored := byKey[key{"USD/2", ""}]
			g.Expect(usdUncolored).NotTo(BeNil())
			g.Expect(usdUncolored.Input.ToBigInt().Int64()).To(Equal(int64(10)))
			g.Expect(usdUncolored.Output.ToBigInt().Int64()).To(Equal(int64(10)),
				"world's output for USD/2 uncolored must match alice's input")

			usdGrants := byKey[key{"USD/2", "GRANTS"}]
			g.Expect(usdGrants).NotTo(BeNil())
			g.Expect(usdGrants.Input.ToBigInt().Int64()).To(Equal(int64(100)))

			usdOps := byKey[key{"USD/2", "OPS"}]
			g.Expect(usdOps).NotTo(BeNil())
			g.Expect(usdOps.Input.ToBigInt().Int64()).To(Equal(int64(40)))

			eurGrants := byKey[key{"EUR/2", "GRANTS"}]
			g.Expect(eurGrants).NotTo(BeNil())
			g.Expect(eurGrants.Input.ToBigInt().Int64()).To(Equal(int64(50)))
		}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})

	It("Should collapse colors to one entry per asset when collapseColors=true", func() {
		Eventually(func(g Gomega) {
			result, err := sharedClient.AggregateVolumes(sharedCtx, &servicepb.AggregateVolumesRequest{
				Ledger:         ledgerName,
				CollapseColors: true,
			})
			g.Expect(err).To(Succeed())
			// USD/2 (10+100+40 = 150) and EUR/2 (50), each under color = "".
			g.Expect(result.Volumes).To(HaveLen(2))

			byAsset := make(map[string]*commonpb.AggregatedVolume, len(result.Volumes))
			for _, v := range result.Volumes {
				g.Expect(v.GetColor()).To(Equal(""),
					"collapse must surface every aggregated entry under the empty color bucket")
				byAsset[v.GetAsset()] = v
			}

			usd := byAsset["USD/2"]
			g.Expect(usd).NotTo(BeNil())
			g.Expect(usd.Input.ToBigInt().Int64()).To(Equal(int64(150)))
			g.Expect(usd.Output.ToBigInt().Int64()).To(Equal(int64(150)),
				"world's output must collapse the same way alice's input does")

			eur := byAsset["EUR/2"]
			g.Expect(eur).NotTo(BeNil())
			g.Expect(eur.Input.ToBigInt().Int64()).To(Equal(int64(50)))
		}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})
})

// Reverting a colored transaction must return the funds to the same color
// bucket they came from — not to the uncolored default. Pins the contract
// that Color carries through the revert path.
var _ = Describe("ColorRevert", Ordered, func() {
	const ledgerName = "color-revert"

	var revertTargetTxID uint64

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(actions.CreateLedgerAction(ledgerName, nil)),
		})
		Expect(err).To(Succeed())

		// world → alice 200 USD/2 color=GRANTS, world → alice 100 USD/2 uncolored.
		_, err = sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewColoredPosting("world", "alice", big.NewInt(100), "USD/2", ""),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())

		// The colored transaction is the one we'll revert.
		resp, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewColoredPosting("world", "alice", big.NewInt(200), "USD/2", "GRANTS"),
				}, nil, nil),
			),
		})
		Expect(err).To(Succeed())
		Expect(resp.Logs).To(HaveLen(1))
		createdTx := resp.Logs[0].Payload.GetApply().Log.Data.GetCreatedTransaction()
		Expect(createdTx).NotTo(BeNil())
		revertTargetTxID = createdTx.Transaction.GetId()
	})

	It("Should drive the revert against the same color bucket", func() {
		_, err := sharedClient.Apply(sharedCtx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(actions.RevertTransactionAction(ledgerName, revertTargetTxID, false, false, nil)),
		})
		Expect(err).To(Succeed())

		Eventually(func(g Gomega) {
			alice, err := sharedClient.GetAccount(sharedCtx, &servicepb.GetAccountRequest{
				Ledger:  ledgerName,
				Address: "alice",
			})
			g.Expect(err).To(Succeed())

			// GRANTS bucket back to 0 (200 - 200); uncolored untouched at 100.
			g.Expect(alice.FindVolume("USD/2", "GRANTS").GetBalance()).To(Equal("0"))
			g.Expect(alice.FindVolume("USD/2", "").GetBalance()).To(Equal("100"))
		}).Within(5 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	})
})
