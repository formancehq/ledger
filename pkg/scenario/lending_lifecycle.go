package scenario

import (
	"fmt"
	"math/big"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/scenario/actions"
)

func init() { Register("lending-lifecycle", RunLendingLifecycle) }

// RunLendingLifecycle provisions a consumer lending scenario:
// 10 loan disbursements, 6 months of repayments (with early repayer and defaulters),
// provisions for doubtful debts, write-offs, and metadata enrichment.
func RunLendingLifecycle(r *Runner) error {
	const (
		ledger       = "lending"
		numBorrowers = 10
		numMonths    = 6
		loanAmount   = 100_000
		monthlyRate  = 2
	)

	// Balance tracking (needed for repayment logic)
	fundingBalance := new(big.Int)
	borrowerLoanBalance := make(map[int]*big.Int, numBorrowers)
	borrowerWalletBalance := make(map[int]*big.Int, numBorrowers)
	for i := 1; i <= numBorrowers; i++ {
		borrowerLoanBalance[i] = new(big.Int)
		borrowerWalletBalance[i] = new(big.Int)
	}

	defaulters := map[int]bool{3: true, 7: true}
	earlyRepayer := 5

	// --- Setup ---
	if _, err := r.Step("Setup",
		actions.CreateLedgerAction(ledger, nil),
		actions.AddAccountTypeAction(ledger, "funding", "funding:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(ledger, "borrower-loan", "borrower:{id}:loan", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(ledger, "borrower-wallet", "borrower:{id}:wallet", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(ledger, "revenue", "revenue:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(ledger, "expense", "expense:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(ledger, "recovery", "recovery:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.SaveNumscriptWithVersionAction(ledger, "fund_pool", `vars {
  monetary $amount
}
send $amount (
  source = @world
  destination = @funding:pool
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(ledger, "disburse_loan", `vars {
  account $borrower_loan
  account $borrower_wallet
  monetary $amount
}
send $amount (
  source = @funding:pool
  destination = $borrower_loan
)
send $amount (
  source = @world
  destination = $borrower_wallet
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(ledger, "repay_principal", `vars {
  account $borrower_wallet
  account $borrower_loan
  monetary $amount
}
send $amount (
  source = $borrower_wallet
  destination = @world
)
send $amount (
  source = $borrower_loan
  destination = @funding:pool
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(ledger, "accrue_interest", `vars {
  account $borrower_wallet
  monetary $amount
}
send $amount (
  source = $borrower_wallet
  destination = @revenue:interest
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(ledger, "provision", `vars {
  monetary $amount
}
send $amount (
  source = @world
  destination = @expense:provision
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(ledger, "write_off", `vars {
  account $borrower_loan
  monetary $amount
}
send $amount (
  source = $borrower_loan
  destination = @recovery:pool
)`, "1.0.0"),
	); err != nil {
		return err
	}

	// Fund the lending pool
	if _, err := r.Step("FundPool",
		actions.CreateScriptRefTransactionAction(ledger, "fund_pool", "1.0.0", map[string]string{
			"amount": fmt.Sprintf("USD/2 %d", loanAmount*numBorrowers),
		}, nil),
	); err != nil {
		return err
	}
	fundingBalance.SetInt64(int64(loanAmount * numBorrowers))

	// --- Disbursements ---
	{
		reqs := make([]*servicepb.Request, 0, numBorrowers)
		for i := 1; i <= numBorrowers; i++ {
			action := actions.CreateScriptRefTransactionAction(ledger, "disburse_loan", "1.0.0", map[string]string{
				"borrower_loan":   fmt.Sprintf("borrower:%d:loan", i),
				"borrower_wallet": fmt.Sprintf("borrower:%d:wallet", i),
				"amount":          fmt.Sprintf("USD/2 %d", loanAmount),
			}, nil)
			action.GetApply().GetCreateTransaction().Reference = fmt.Sprintf("loan-disburse-%d", i)
			reqs = append(reqs, action)

			fundingBalance.Sub(fundingBalance, big.NewInt(loanAmount))
			borrowerLoanBalance[i].SetInt64(loanAmount)
			borrowerWalletBalance[i].SetInt64(loanAmount)
		}
		if _, err := r.Step("Disbursements", reqs...); err != nil {
			return err
		}
	}

	// --- Monthly Repayment Cycles ---
	for month := 1; month <= numMonths; month++ {
		var reqs []*servicepb.Request

		for i := 1; i <= numBorrowers; i++ {
			outstanding := borrowerLoanBalance[i]
			wallet := borrowerWalletBalance[i]

			if outstanding.Sign() <= 0 {
				continue
			}

			// Early repayer: full repayment in month 2
			if i == earlyRepayer && month == 2 {
				interest := new(big.Int).Mul(outstanding, big.NewInt(monthlyRate))
				interest.Div(interest, big.NewInt(100))

				reqs = append(reqs,
					actions.CreateScriptRefTransactionAction(ledger, "accrue_interest", "1.0.0", map[string]string{
						"borrower_wallet": fmt.Sprintf("borrower:%d:wallet", i),
						"amount":          "USD/2 " + interest.String(),
					}, map[string]string{"type": "early-repay-interest"}),
				)
				wallet.Sub(wallet, interest)

				repayAmount := new(big.Int).Set(outstanding)
				if repayAmount.Cmp(wallet) > 0 {
					repayAmount.Set(wallet)
				}
				reqs = append(reqs,
					actions.CreateScriptRefTransactionAction(ledger, "repay_principal", "1.0.0", map[string]string{
						"borrower_wallet": fmt.Sprintf("borrower:%d:wallet", i),
						"borrower_loan":   fmt.Sprintf("borrower:%d:loan", i),
						"amount":          "USD/2 " + repayAmount.String(),
					}, map[string]string{"type": "early-repay-principal"}),
				)
				wallet.Sub(wallet, repayAmount)
				fundingBalance.Add(fundingBalance, repayAmount)
				outstanding.Sub(outstanding, repayAmount)

				continue
			}

			// Defaulters stop paying after month 2
			if defaulters[i] && month > 2 {
				continue
			}

			interest := new(big.Int).Mul(outstanding, big.NewInt(monthlyRate))
			interest.Div(interest, big.NewInt(100))

			principal := new(big.Int).Div(big.NewInt(loanAmount), big.NewInt(numMonths))
			if principal.Cmp(outstanding) > 0 {
				principal.Set(outstanding)
			}

			// Ensure wallet has enough
			totalPayment := new(big.Int).Add(interest, principal)
			if totalPayment.Cmp(wallet) > 0 {
				if interest.Cmp(wallet) >= 0 {
					continue
				}
				principal.Sub(wallet, interest)
			}

			if interest.Sign() > 0 {
				reqs = append(reqs,
					actions.CreateScriptRefTransactionAction(ledger, "accrue_interest", "1.0.0", map[string]string{
						"borrower_wallet": fmt.Sprintf("borrower:%d:wallet", i),
						"amount":          "USD/2 " + interest.String(),
					}, map[string]string{
						"month": strconv.Itoa(month),
						"type":  "interest",
					}),
				)
				wallet.Sub(wallet, interest)
			}

			if principal.Sign() > 0 {
				reqs = append(reqs,
					actions.CreateScriptRefTransactionAction(ledger, "repay_principal", "1.0.0", map[string]string{
						"borrower_wallet": fmt.Sprintf("borrower:%d:wallet", i),
						"borrower_loan":   fmt.Sprintf("borrower:%d:loan", i),
						"amount":          "USD/2 " + principal.String(),
					}, map[string]string{
						"month": strconv.Itoa(month),
						"type":  "principal",
					}),
				)
				wallet.Sub(wallet, principal)
				outstanding.Sub(outstanding, principal)
				fundingBalance.Add(fundingBalance, principal)
			}
		}

		if len(reqs) > 0 {
			if _, err := r.Step(fmt.Sprintf("RepayCycle/Month%d", month), reqs...); err != nil {
				return err
			}
		}
	}

	// --- Provisions for Doubtful Debts ---
	{
		var reqs []*servicepb.Request
		for id := range defaulters {
			outstanding := borrowerLoanBalance[id]
			if outstanding.Sign() > 0 {
				reqs = append(reqs,
					actions.CreateScriptRefTransactionAction(ledger, "provision", "1.0.0", map[string]string{
						"amount": "USD/2 " + outstanding.String(),
					}, map[string]string{
						"borrower": fmt.Sprintf("borrower:%d", id),
						"reason":   "default",
					}),
				)
			}
		}
		if len(reqs) > 0 {
			if _, err := r.Step("Provisions", reqs...); err != nil {
				return err
			}
		}
	}

	// --- Write-offs ---
	{
		var reqs []*servicepb.Request
		for id := range defaulters {
			outstanding := borrowerLoanBalance[id]
			if outstanding.Sign() > 0 {
				reqs = append(reqs,
					actions.CreateScriptRefTransactionAction(ledger, "write_off", "1.0.0", map[string]string{
						"borrower_loan": fmt.Sprintf("borrower:%d:loan", id),
						"amount":        "USD/2 " + outstanding.String(),
					}, map[string]string{
						"reason": "write-off-default",
					}),
				)
				outstanding.SetInt64(0)
			}
		}
		if len(reqs) > 0 {
			if _, err := r.Step("WriteOffs", reqs...); err != nil {
				return err
			}
		}
	}

	// --- Metadata Enrichment ---
	{
		var metaReqs []*servicepb.Request
		for id := range defaulters {
			metaReqs = append(metaReqs,
				actions.SaveAccountMetadataAction(ledger, fmt.Sprintf("borrower:%d:loan", id), map[string]string{
					"status": "written-off",
				}),
			)
		}
		metaReqs = append(metaReqs,
			actions.SaveAccountMetadataAction(ledger, fmt.Sprintf("borrower:%d:loan", earlyRepayer), map[string]string{
				"status": "repaid-early",
			}),
		)
		if _, err := r.Step("Metadata", metaReqs...); err != nil {
			return err
		}
	}

	return nil
}
