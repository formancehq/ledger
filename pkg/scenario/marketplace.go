package scenario

import (
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/scenario/actions"
)

func init() { Register("marketplace", RunMarketplace) }

// RunMarketplace provisions a high-volume e-commerce marketplace scenario:
// 50 customers, 10 merchants, 200 purchases with fees, reverts, payouts,
// metadata operations, inline numscript, raw postings, and numscript deletion.
func RunMarketplace(r *Runner) error {
	const (
		ledger       = "marketplace"
		numCustomers = 50
		numMerchants = 10
		numPurchases = 200
		numReverts   = 10
		depositAmt   = 1_000_000
		feePercent   = 3
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
	purchaseTxIDs := make([]uint64, 0, numPurchases)
	purchaseRecords := make([]purchaseRecord, 0, numPurchases)

	// --- Setup ---
	if _, err := r.Step("Setup",
		actions.CreateLedgerAction(ledger, nil),
		actions.AddAccountTypeAction(ledger, "customer", "customer:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(ledger, "merchant", "merchant:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(ledger, "platform-fees", "platform:fees", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(ledger, "platform-payouts", "platform:payouts", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.SaveNumscriptWithVersionAction(ledger, "deposit", `vars {
  account $customer
  monetary $amount
}
send $amount (
  source = @world
  destination = $customer
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(ledger, "purchase", `vars {
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
		actions.SaveNumscriptWithVersionAction(ledger, "payout", `vars {
  account $merchant
  monetary $amount
}
send $amount (
  source = $merchant
  destination = @platform:payouts
)`, "1.0.0"),
	); err != nil {
		return err
	}

	// --- Customer Deposits ---
	{
		reqs := make([]*servicepb.Request, 0, numCustomers)
		for i := 1; i <= numCustomers; i++ {
			reqs = append(reqs, actions.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
				"customer": fmt.Sprintf("customer:%d", i),
				"amount":   fmt.Sprintf("USD/2 %d", depositAmt),
			}, nil))
		}
		if _, err := r.Step("CustomerDeposits", reqs...); err != nil {
			return err
		}
	}

	// --- Purchases with Fees ---
	for i := range numPurchases {
		customer := 1 + i%numCustomers
		merchant := 1 + i%numMerchants
		amount := int64(1000 + i*100)

		resp, err := r.Step(fmt.Sprintf("Purchase/%d", i),
			actions.CreateScriptRefTransactionAction(ledger, "purchase", "1.0.0", map[string]string{
				"customer": fmt.Sprintf("customer:%d", customer),
				"merchant": fmt.Sprintf("merchant:%d", merchant),
				"amount":   fmt.Sprintf("USD/2 %d", amount),
			}, nil),
		)
		if err != nil {
			return err
		}

		txID, ok := actions.GetCreatedTransactionID(resp)
		if ok {
			purchaseTxIDs = append(purchaseTxIDs, txID)
		}
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

	// --- Reverts ---
	step := numPurchases / numReverts
	for rv := range numReverts {
		idx := rv * step
		if idx >= len(purchaseTxIDs) || purchaseRecords[idx].reverted {
			continue
		}
		p := purchaseRecords[idx]

		if _, err := r.Step(fmt.Sprintf("Revert/%d", idx),
			actions.RevertTransactionAction(ledger, purchaseTxIDs[idx], true, false, nil),
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
			actions.CreateScriptRefTransactionAction(ledger, "payout", "1.0.0", map[string]string{
				"merchant": fmt.Sprintf("merchant:%d", i),
				"amount":   fmt.Sprintf("USD/2 %d", bal.Int64()),
			}, nil),
		); err != nil {
			return err
		}
	}

	// --- Metadata Operations ---
	if _, err := r.Step("Metadata/Save",
		actions.SaveAccountMetadataAction(ledger, "customer:1", map[string]string{
			"tier": "gold",
			"kyc":  "verified",
		}),
		actions.SaveAccountMetadataAction(ledger, "merchant:1", map[string]string{
			"category": "electronics",
		}),
	); err != nil {
		return err
	}
	if len(purchaseTxIDs) > 0 {
		if _, err := r.Step("Metadata/Transaction",
			actions.SaveTransactionMetadataAction(ledger, purchaseTxIDs[0], map[string]string{
				"flagged": "true",
				"reason":  "review",
			}),
		); err != nil {
			return err
		}
	}
	if _, err := r.Step("Metadata/DeleteAccount",
		actions.DeleteAccountMetadataAction(ledger, "customer:1", "kyc"),
	); err != nil {
		return err
	}
	if len(purchaseTxIDs) > 0 {
		if _, err := r.Step("Metadata/DeleteTransaction",
			actions.DeleteTransactionMetadataAction(ledger, purchaseTxIDs[0], "reason"),
		); err != nil {
			return err
		}
	}

	// --- Inline Numscript ---
	if _, err := r.Step("InlineNumscript",
		actions.CreateScriptTransactionAction(ledger, `vars {
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
		actions.CreateTransactionAction(ledger, []*commonpb.Posting{
			actions.NewPosting("customer:2", "customer:3", big.NewInt(50), "USD/2"),
		}, nil, nil),
	); err != nil {
		return err
	}

	// --- DeleteNumscript ---
	if _, err := r.Step("SaveTempNumscript",
		actions.SaveNumscriptWithVersionAction(ledger, "temp_script", `vars {
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
		actions.DeleteNumscriptAction(ledger, "temp_script"),
	); err != nil {
		return err
	}

	return nil
}
