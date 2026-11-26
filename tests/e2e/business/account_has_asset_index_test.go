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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = Describe("AccountHasAssetIndex", Ordered, func() {

	Context("When querying accounts with the has-asset filter", Ordered, func() {
		const ledgerName = "acct-has-asset-idx"

		// hasUSD2Filter builds a `has asset USD/2` condition: accounts that
		// have ever touched a volume cell for the exact (asset base "USD",
		// precision 2) pair.
		hasUSD2Filter := &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_AccountHasAsset{
				AccountHasAsset: &commonpb.AccountHasAssetCondition{
					AssetBase: "USD",
					Precision: 2,
				},
			},
		}

		BeforeAll(func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
			Expect(err).To(Succeed())

			// alice & bob touch USD/2; carol touches only EUR. The asset
			// cell key carries (base, precision), so "EUR" (precision 0) is
			// distinct from "USD/2" and carol must never match has-asset USD/2.
			_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "accounts:alice", big.NewInt(100), "USD/2"),
			}, nil, nil),
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "accounts:bob", big.NewInt(200), "USD/2"),
				}, nil, nil),
				actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
					actions.NewPosting("world", "accounts:carol", big.NewInt(300), "EUR"),
				}, nil, nil)))
			Expect(err).To(Succeed())
		})

		It("Should reject the has-asset filter before the index exists", func() {
			_, err := actions.ListAccountsFiltered(sharedCtx, sharedClient, ledgerName, 0, "", hasUSD2Filter)
			Expect(err).To(HaveOccurred())

			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.FailedPrecondition))
			Expect(actions.ExtractGRPCErrorInfo(err).Reason).To(Equal("INDEX_NOT_FOUND"))
		})

		It("Should return exactly the accounts that touched USD/2 once the index is READY", func() {
			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateAccountAssetIndexAction(ledgerName)))
			Expect(err).To(Succeed())

			Expect(actions.WaitForAccountAssetIndexReady(sharedCtx, sharedClient, ledgerName)).To(Succeed())

			// Volume-cell presence semantics: an account matches `has asset
			// USD/2` if it has ever touched a USD/2 cell, on either side of a
			// posting. alice & bob are destinations; world is the source of
			// both USD/2 postings, so it also touched USD/2 and matches.
			// carol only ever touched EUR (precision 0, a distinct cell) and
			// must be excluded.
			//
			// The backfill runs asynchronously after the atomic switch, so
			// poll until the inverted index has caught up with the seeded
			// transactions.
			Eventually(func(g Gomega) {
				accounts, err := actions.ListAccountsFiltered(sharedCtx, sharedClient, ledgerName, 0, "", hasUSD2Filter)
				g.Expect(err).To(Succeed())

				addresses := make([]string, len(accounts))
				for i, a := range accounts {
					addresses[i] = a.Address
				}
				g.Expect(addresses).To(ConsistOf("accounts:alice", "accounts:bob", "world"))
				g.Expect(addresses).NotTo(ContainElement("accounts:carol"))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})

		It("Should index a transaction applied live (processLogs) after the index is READY", func() {
			// The previous It seeded all transactions before the index
			// existed, so it only exercises the backfill path. Apply a fresh
			// transaction touching a new account/asset now that the index is
			// already READY: this rides the live runtime path (processLogs →
			// indexCreatedTransaction → indexPostingAddressMappings(cfg, ...)),
			// which a backfill-only test would never cover.
			hasCHF2Filter := &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_AccountHasAsset{
					AccountHasAsset: &commonpb.AccountHasAssetCondition{
						AssetBase: "CHF",
						Precision: 2,
					},
				},
			}

			_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "accounts:dave", big.NewInt(400), "CHF/2"),
			}, nil, nil)))
			Expect(err).To(Succeed())

			// dave is the destination and world the source of the CHF/2 posting,
			// so both touched the CHF/2 cell and must appear. The earlier USD/2
			// accounts never touched CHF/2 and must be excluded.
			Eventually(func(g Gomega) {
				accounts, err := actions.ListAccountsFiltered(sharedCtx, sharedClient, ledgerName, 0, "", hasCHF2Filter)
				g.Expect(err).To(Succeed())

				addresses := make([]string, len(accounts))
				for i, a := range accounts {
					addresses[i] = a.Address
				}
				g.Expect(addresses).To(ConsistOf("accounts:dave", "world"))
				g.Expect(addresses).NotTo(ContainElement("accounts:alice"))
			}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
		})
	})
})
