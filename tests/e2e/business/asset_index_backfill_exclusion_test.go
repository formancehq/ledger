//go:build e2e

package business

import (
	"math/big"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// A transient account whose asset cell was
// washed to zero and purged BEFORE the asset index exists must stay excluded
// when the index is later created and BACKFILLED — mirroring the live fold's
// exclusion of transient touches.
var _ = Describe("Asset-index backfill exclusion", Ordered, func() {
	const ledgerName = "asset-backfill-exclusion-ledger"

	hasEUR := &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_AccountHasAsset{
			AccountHasAsset: &commonpb.AccountHasAssetCondition{AssetBase: "EUR"},
		},
	}

	BeforeAll(func() {
		_, err := sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction(ledgerName, nil)))
		Expect(err).To(Succeed())

		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.AddAccountTypeWithPersistenceAction(ledgerName, "t", "t:{id}", commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT),
			actions.AddAccountTypeWithPersistenceAction(ledgerName, "keep", "keep:{id}", commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL)))
		Expect(err).To(Succeed())

		// Washed transient EUR touch (excluded), plus a durable EUR toucher.
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("",
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "t:1", big.NewInt(5), "EUR"),
			}, nil, nil),
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("t:1", "world", big.NewInt(5), "EUR"),
			}, nil, nil),
			actions.CreateTransactionAction(ledgerName, []*commonpb.Posting{
				actions.NewPosting("world", "keep:1", big.NewInt(5), "EUR"),
			}, nil, nil)))
		Expect(err).To(Succeed())

		// Index created AFTER the wash: the rows come from the backfill.
		_, err = sharedClient.Apply(sharedCtx, servicepb.UnsignedApplyRequest("", actions.CreateAccountAssetIndexAction(ledgerName)))
		Expect(err).To(Succeed())
		Expect(actions.WaitForAccountAssetIndexReady(sharedCtx, sharedClient, ledgerName)).To(Succeed())
	})

	It("backfill excludes the washed transient touch", func() {
		accts, err := actions.ListAccountsFiltered(sharedCtx, sharedClient, ledgerName, 0, "", hasEUR)
		Expect(err).To(Succeed())

		addrs := make([]string, len(accts))
		for i, a := range accts {
			addrs[i] = a.GetAddress()
		}

		Expect(addrs).To(ConsistOf("world", "keep:1"), "t:1's washed touch must stay excluded")
	})
})
