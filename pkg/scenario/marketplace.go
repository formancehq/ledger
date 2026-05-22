package scenario

import (
	"context"
	"fmt"
	"math/big"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
)

func init() { Register("marketplace", RunMarketplace) }

const (
	// MarketplaceLedger is the ledger name used by the marketplace scenario.
	MarketplaceLedger       = "marketplace"
	MarketplaceNumCustomers = 50
	MarketplaceNumMerchants = 10
	MarketplaceDepositAmt   = 1_000_000
)

// MarketplaceBlocks returns the atomic blocks for the marketplace scenario.
func MarketplaceBlocks() *BlockGroup {
	return &BlockGroup{
		Setup: MarketplaceSetupActions,
		Blocks: []*Block{
			{Name: "marketplace/deposit", Run: marketplaceDeposit},
			{Name: "marketplace/purchase", Run: marketplacePurchase},
			{Name: "marketplace/revert", Run: marketplaceRevert},
			{Name: "marketplace/payout", Run: marketplacePayout},
			{Name: "marketplace/metadata", Run: marketplaceMetadata},
		},
	}
}

func marketplaceDeposit(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	customerID := 1 + RandIntN(r, MarketplaceNumCustomers)
	address := fmt.Sprintf("customer:%d", customerID)
	amount := int64(100_000) + RandInt64N(r, int64(MarketplaceDepositAmt))

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(MarketplaceLedger, "deposit", "1.0.0", map[string]string{
			"customer": address,
			"amount":   fmt.Sprintf("USD/2 %d", amount),
		}, nil),
	)
}

func marketplacePurchase(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	customerID := 1 + RandIntN(r, MarketplaceNumCustomers)
	merchantID := 1 + RandIntN(r, MarketplaceNumMerchants)
	customer := fmt.Sprintf("customer:%d", customerID)
	merchant := fmt.Sprintf("merchant:%d", merchantID)

	bal, ok := GetAccountBalance(ctx, client, MarketplaceLedger, customer, "USD/2")
	if !ok || bal.Cmp(big.NewInt(1000)) < 0 {
		return nil, ErrSkip
	}

	maxAmt := min(bal.Int64(), 50_000)
	amount := int64(1000) + RandInt64N(r, maxAmt-1000+1)

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(MarketplaceLedger, "purchase", "1.0.0", map[string]string{
			"customer": customer,
			"merchant": merchant,
			"amount":   fmt.Sprintf("USD/2 %d", amount),
		}, nil),
	)
}

func marketplaceRevert(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	tx, ok := GetNonRevertedTransaction(ctx, client, MarketplaceLedger, r)
	if !ok {
		return nil, ErrSkip
	}

	return ApplyActions(ctx, client,
		actions.RevertTransactionAction(MarketplaceLedger, tx.GetId(), true, false, nil),
	)
}

func marketplacePayout(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	merchantID := 1 + RandIntN(r, MarketplaceNumMerchants)
	merchant := fmt.Sprintf("merchant:%d", merchantID)

	bal, ok := GetAccountBalance(ctx, client, MarketplaceLedger, merchant, "USD/2")
	if !ok || bal.Sign() <= 0 {
		return nil, ErrSkip
	}

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(MarketplaceLedger, "payout", "1.0.0", map[string]string{
			"merchant": merchant,
			"amount":   fmt.Sprintf("USD/2 %d", bal.Int64()),
		}, nil),
	)
}

func marketplaceMetadata(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	var address string
	if RandIntN(r, 2) == 0 {
		address = fmt.Sprintf("customer:%d", 1+RandIntN(r, MarketplaceNumCustomers))
	} else {
		address = fmt.Sprintf("merchant:%d", 1+RandIntN(r, MarketplaceNumMerchants))
	}

	resp, err := ApplyActions(ctx, client,
		actions.SaveAccountMetadataAction(MarketplaceLedger, address, map[string]string{
			"tier":      fmt.Sprintf("tier-%d", RandIntN(r, 5)),
			"last_seen": strconv.FormatUint(r(), 10),
		}),
	)
	if err != nil {
		return nil, err
	}

	if RandIntN(r, 3) == 0 {
		resp, err = ApplyActions(ctx, client,
			actions.DeleteAccountMetadataAction(MarketplaceLedger, address, "last_seen"),
		)
	}

	return resp, err
}

// MarketplaceSetupActions returns the Apply requests that create the ledger,
// account types, and numscript library for the marketplace scenario.
func MarketplaceSetupActions() []*servicepb.Request {
	return []*servicepb.Request{
		actions.CreateLedgerAction(MarketplaceLedger, nil),
		actions.AddAccountTypeAction(MarketplaceLedger, "customer", "customer:{id}"),
		actions.AddAccountTypeAction(MarketplaceLedger, "merchant", "merchant:{id}"),
		actions.AddAccountTypeAction(MarketplaceLedger, "platform-fees", "platform:fees"),
		actions.AddAccountTypeAction(MarketplaceLedger, "platform-payouts", "platform:payouts"),
		actions.SaveNumscriptWithVersionAction(MarketplaceLedger, "deposit", `vars {
  account $customer
  monetary $amount
}
send $amount (
  source = @world
  destination = $customer
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(MarketplaceLedger, "purchase", `vars {
  account $customer
  account $merchant
  monetary $amount
}
send $amount (
  source = $customer
  destination = {
    3/100 to @platform:fees
    remaining to $merchant
  }
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(MarketplaceLedger, "payout", `vars {
  account $merchant
  monetary $amount
}
send $amount (
  source = $merchant
  destination = @platform:payouts
)`, "1.0.0"),
		actions.CreatePreparedQueryAction("customer-query", MarketplaceLedger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			actions.AddressPrefixFilter("customer:"),
		),
		actions.CreatePreparedQueryAction("accounts-by-prefix", MarketplaceLedger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			actions.ParamAddressPrefixFilter("prefix"),
		),
	}
}

// RunMarketplace provisions a high-volume e-commerce marketplace scenario:
// 50 customers, 10 merchants, 200 purchases with fees, reverts, payouts,
// metadata operations, inline numscript, raw postings, and numscript deletion.
func RunMarketplace(r *Runner) error {
	const (
		numCustomers = 50
		numMerchants = 10
		depositAmt   = 1_000_000
		feePercent   = 3
	)

	var (
		numPurchases = r.Iterations(200)
		numReverts   = r.Iterations(10)
	)

	// Balance tracking for merchant payouts
	merchantBalance := make(map[int]*big.Int, numMerchants)
	for i := 1; i <= numMerchants; i++ {
		merchantBalance[i] = new(big.Int)
	}
	totalFees := new(big.Int)

	// Track purchase details for reverts and payout calculations
	type purchaseRecord struct {
		customer int
		merchant int
		amount   int64
		reverted bool
	}
	var purchaseTxIDs []uint64
	purchaseRecords := make([]purchaseRecord, 0, numPurchases)

	// --- Setup ---
	if _, err := r.Step("Setup", MarketplaceSetupActions()...); err != nil {
		return err
	}

	// --- Customer Deposits ---
	{
		reqs := make([]*servicepb.Request, 0, numCustomers)
		for i := 1; i <= numCustomers; i++ {
			reqs = append(reqs, actions.CreateScriptRefTransactionAction(MarketplaceLedger, "deposit", "1.0.0", map[string]string{
				"customer": fmt.Sprintf("customer:%d", i),
				"amount":   fmt.Sprintf("USD/2 %d", depositAmt),
			}, nil))
		}
		if _, err := r.Step("CustomerDeposits", reqs...); err != nil {
			return err
		}
	}

	// --- Purchases with Fees (batched) ---
	{
		reqs := make([]*servicepb.Request, 0, numPurchases)
		for i := range numPurchases {
			customer := 1 + i%numCustomers
			merchant := 1 + i%numMerchants
			amount := int64(1000 + i*100)

			reqs = append(reqs, actions.CreateScriptRefTransactionAction(MarketplaceLedger, "purchase", "1.0.0", map[string]string{
				"customer": fmt.Sprintf("customer:%d", customer),
				"merchant": fmt.Sprintf("merchant:%d", merchant),
				"amount":   fmt.Sprintf("USD/2 %d", amount),
			}, nil))

			purchaseRecords = append(purchaseRecords, purchaseRecord{
				customer: customer,
				merchant: merchant,
				amount:   amount,
			})

			fee := amount * feePercent / 100
			net := amount - fee
			merchantBalance[merchant].Add(merchantBalance[merchant], big.NewInt(net))
			totalFees.Add(totalFees, big.NewInt(fee))
		}

		resp, err := r.Step("Purchases", reqs...)
		if err != nil {
			return err
		}
		purchaseTxIDs = actions.GetAllCreatedTransactionIDs(resp)
	}

	// --- Reverts ---
	step := numPurchases / numReverts
	for rv := range numReverts {
		idx := rv * step
		if idx >= len(purchaseTxIDs) || purchaseRecords[idx].reverted {
			continue
		}
		p := purchaseRecords[idx]

		if _, err := r.Step(fmt.Sprintf("Revert/%d", idx),
			actions.RevertTransactionAction(MarketplaceLedger, purchaseTxIDs[idx], true, false, nil),
		); err != nil {
			return err
		}
		purchaseRecords[idx].reverted = true

		fee := p.amount * feePercent / 100
		net := p.amount - fee
		merchantBalance[p.merchant].Sub(merchantBalance[p.merchant], big.NewInt(net))
		totalFees.Sub(totalFees, big.NewInt(fee))
	}

	// --- Merchant Payouts ---
	for i := 1; i <= numMerchants; i++ {
		bal := merchantBalance[i]
		if bal.Sign() <= 0 {
			continue
		}
		if _, err := r.Step(fmt.Sprintf("Payout/merchant:%d", i),
			actions.CreateScriptRefTransactionAction(MarketplaceLedger, "payout", "1.0.0", map[string]string{
				"merchant": fmt.Sprintf("merchant:%d", i),
				"amount":   fmt.Sprintf("USD/2 %d", bal.Int64()),
			}, nil),
		); err != nil {
			return err
		}
	}

	// --- Metadata Operations ---
	if _, err := r.Step("Metadata/Save",
		actions.SaveAccountMetadataAction(MarketplaceLedger, "customer:1", map[string]string{
			"tier": "gold",
			"kyc":  "verified",
		}),
		actions.SaveAccountMetadataAction(MarketplaceLedger, "merchant:1", map[string]string{
			"category": "electronics",
		}),
	); err != nil {
		return err
	}
	if len(purchaseTxIDs) > 0 {
		if _, err := r.Step("Metadata/Transaction",
			actions.SaveTransactionMetadataAction(MarketplaceLedger, purchaseTxIDs[0], map[string]string{
				"flagged": "true",
				"reason":  "review",
			}),
		); err != nil {
			return err
		}
	}
	if _, err := r.Step("Metadata/DeleteAccount",
		actions.DeleteAccountMetadataAction(MarketplaceLedger, "customer:1", "kyc"),
	); err != nil {
		return err
	}
	if len(purchaseTxIDs) > 0 {
		if _, err := r.Step("Metadata/DeleteTransaction",
			actions.DeleteTransactionMetadataAction(MarketplaceLedger, purchaseTxIDs[0], "reason"),
		); err != nil {
			return err
		}
	}

	// --- Inline Numscript ---
	if _, err := r.Step("InlineNumscript",
		actions.CreateScriptTransactionAction(MarketplaceLedger, `vars {
  account $src
  account $dst
  monetary $amount
}
send $amount (
  source = $src
  destination = $dst
)`, map[string]string{
			"src":    "customer:1",
			"dst":    "customer:2",
			"amount": "USD/2 100",
		}, nil),
	); err != nil {
		return err
	}

	// --- Raw Postings ---
	if _, err := r.Step("RawPostings",
		actions.CreateTransactionAction(MarketplaceLedger, []*commonpb.Posting{
			actions.NewPosting("customer:2", "customer:3", big.NewInt(50), "USD/2"),
		}, nil, nil),
	); err != nil {
		return err
	}

	// --- DeleteNumscript ---
	if _, err := r.Step("SaveTempNumscript",
		actions.SaveNumscriptWithVersionAction(MarketplaceLedger, "temp_script", `vars {
  monetary $amount
}
send $amount (
  source = @world
  destination = @customer:1
)`, "1.0.0"),
	); err != nil {
		return err
	}
	if _, err := r.Step("DeleteNumscript",
		actions.DeleteNumscriptAction(MarketplaceLedger, "temp_script"),
	); err != nil {
		return err
	}

	return nil
}
