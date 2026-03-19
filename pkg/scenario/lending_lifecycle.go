package scenario

import (
	"context"
	"fmt"
	"math/big"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/scenario/actions"
)

func init() { Register("lending-lifecycle", RunLendingLifecycle) }

const (
	// LendingLifecycleLedger is the ledger name used by the lending lifecycle scenario.
	LendingLifecycleLedger       = "lending"
	LendingLifecycleNumBorrowers = 10
	LendingLifecycleLoanAmount   = 100_000
)

// LendingLifecycleBlocks returns the atomic blocks for the lending lifecycle scenario.
func LendingLifecycleBlocks() *BlockGroup {
	return &BlockGroup{
		Setup: LendingLifecycleSetupActions,
		Blocks: []*Block{
			{Name: "lending/fund_pool", Run: lendingFundPool},
			{Name: "lending/disburse", Run: lendingDisburse},
			{Name: "lending/repay", Run: lendingRepay},
			{Name: "lending/provision", Run: lendingProvision},
			{Name: "lending/write_off", Run: lendingWriteOff},
		},
	}
}

func lendingFundPool(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	amount := int64(LendingLifecycleLoanAmount) * (1 + RandInt64N(r, 5))

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(LendingLifecycleLedger, "fund_pool", "1.0.0", map[string]string{
			"amount": fmt.Sprintf("USD/2 %d", amount),
		}, nil),
	)
}

func lendingDisburse(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	poolBal, ok := GetAccountBalance(ctx, client, LendingLifecycleLedger, "funding:pool", "USD/2")
	if !ok || poolBal.Cmp(big.NewInt(LendingLifecycleLoanAmount)) < 0 {
		return nil, ErrSkip
	}

	borrowerID := 1 + RandIntN(r, LendingLifecycleNumBorrowers)

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(LendingLifecycleLedger, "disburse_loan", "1.0.0", map[string]string{
			"borrower_loan":   fmt.Sprintf("borrower:%d:loan", borrowerID),
			"borrower_wallet": fmt.Sprintf("borrower:%d:wallet", borrowerID),
			"amount":          fmt.Sprintf("USD/2 %d", LendingLifecycleLoanAmount),
		}, nil),
	)
}

func lendingRepay(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	borrowerID := 1 + RandIntN(r, LendingLifecycleNumBorrowers)
	loanAddr := fmt.Sprintf("borrower:%d:loan", borrowerID)
	walletAddr := fmt.Sprintf("borrower:%d:wallet", borrowerID)

	loanBal, ok := GetAccountBalance(ctx, client, LendingLifecycleLedger, loanAddr, "USD/2")
	if !ok || loanBal.Sign() <= 0 {
		return nil, ErrSkip
	}
	walletBal, ok := GetAccountBalance(ctx, client, LendingLifecycleLedger, walletAddr, "USD/2")
	if !ok || walletBal.Sign() <= 0 {
		return nil, ErrSkip
	}

	// Interest: 2% of outstanding.
	interest := new(big.Int).Mul(loanBal, big.NewInt(2))
	interest.Div(interest, big.NewInt(100))
	if interest.Cmp(walletBal) > 0 {
		return nil, ErrSkip
	}

	// Principal: min(outstanding, wallet-interest, loanAmount/6).
	remaining := new(big.Int).Sub(walletBal, interest)
	principal := new(big.Int).Div(big.NewInt(LendingLifecycleLoanAmount), big.NewInt(6))
	if principal.Cmp(remaining) > 0 {
		principal.Set(remaining)
	}
	if principal.Cmp(loanBal) > 0 {
		principal.Set(loanBal)
	}
	if principal.Sign() <= 0 {
		return nil, ErrSkip
	}

	var reqs []*servicepb.Request
	if interest.Sign() > 0 {
		reqs = append(reqs, actions.CreateScriptRefTransactionAction(LendingLifecycleLedger, "accrue_interest", "1.0.0", map[string]string{
			"borrower_wallet": walletAddr,
			"amount":          "USD/2 " + interest.String(),
		}, map[string]string{"type": "interest"}))
	}
	reqs = append(reqs, actions.CreateScriptRefTransactionAction(LendingLifecycleLedger, "repay_principal", "1.0.0", map[string]string{
		"borrower_wallet": walletAddr,
		"borrower_loan":   loanAddr,
		"amount":          "USD/2 " + principal.String(),
	}, map[string]string{"type": "principal"}))

	return ApplyActions(ctx, client, reqs...)
}

func lendingProvision(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	borrowerID := 1 + RandIntN(r, LendingLifecycleNumBorrowers)
	loanAddr := fmt.Sprintf("borrower:%d:loan", borrowerID)

	loanBal, ok := GetAccountBalance(ctx, client, LendingLifecycleLedger, loanAddr, "USD/2")
	if !ok || loanBal.Sign() <= 0 {
		return nil, ErrSkip
	}

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(LendingLifecycleLedger, "provision", "1.0.0", map[string]string{
			"amount": "USD/2 " + loanBal.String(),
		}, map[string]string{"borrower": fmt.Sprintf("borrower:%d", borrowerID), "reason": "default"}),
	)
}

func lendingWriteOff(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	borrowerID := 1 + RandIntN(r, LendingLifecycleNumBorrowers)
	loanAddr := fmt.Sprintf("borrower:%d:loan", borrowerID)

	loanBal, ok := GetAccountBalance(ctx, client, LendingLifecycleLedger, loanAddr, "USD/2")
	if !ok || loanBal.Sign() <= 0 {
		return nil, ErrSkip
	}

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(LendingLifecycleLedger, "write_off", "1.0.0", map[string]string{
			"borrower_loan": loanAddr,
			"amount":        "USD/2 " + loanBal.String(),
		}, map[string]string{"reason": "write-off"}),
	)
}

// LendingLifecycleSetupActions returns the Apply requests that create the ledger,
// account types, and numscript library for the lending lifecycle scenario.
func LendingLifecycleSetupActions() []*servicepb.Request {
	return []*servicepb.Request{
		actions.CreateLedgerAction(LendingLifecycleLedger, nil),
		actions.AddAccountTypeAction(LendingLifecycleLedger, "funding", "funding:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(LendingLifecycleLedger, "borrower-loan", "borrower:{id}:loan", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(LendingLifecycleLedger, "borrower-wallet", "borrower:{id}:wallet", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(LendingLifecycleLedger, "revenue", "revenue:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(LendingLifecycleLedger, "expense", "expense:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(LendingLifecycleLedger, "recovery", "recovery:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.SaveNumscriptWithVersionAction(LendingLifecycleLedger, "fund_pool", `vars {
  monetary $amount
}
send $amount (
  source = @world
  destination = @funding:pool
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(LendingLifecycleLedger, "disburse_loan", `vars {
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
		actions.SaveNumscriptWithVersionAction(LendingLifecycleLedger, "repay_principal", `vars {
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
		actions.SaveNumscriptWithVersionAction(LendingLifecycleLedger, "accrue_interest", `vars {
  account $borrower_wallet
  monetary $amount
}
send $amount (
  source = $borrower_wallet
  destination = @revenue:interest
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(LendingLifecycleLedger, "provision", `vars {
  monetary $amount
}
send $amount (
  source = @world
  destination = @expense:provision
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(LendingLifecycleLedger, "write_off", `vars {
  account $borrower_loan
  monetary $amount
}
send $amount (
  source = $borrower_loan
  destination = @recovery:pool
)`, "1.0.0"),
		actions.CreateAccountMetadataIndexAction(LendingLifecycleLedger, "status"),
		actions.CreatePreparedQueryAction("loans-by-status", LendingLifecycleLedger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			actions.ParamStringMetadataFilter("status", "status_value"),
		),
		actions.CreatePreparedQueryAction("accounts-by-prefix", LendingLifecycleLedger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			actions.ParamAddressPrefixFilter("prefix"),
		),
	}
}

// RunLendingLifecycle provisions a consumer lending scenario:
// 10 loan disbursements, 6 months of repayments (with early repayer and defaulters),
// provisions for doubtful debts, write-offs, and metadata enrichment.
func RunLendingLifecycle(r *Runner) error {
	const (
		numBorrowers = 10
		loanAmount   = 100_000
		monthlyRate  = 2
	)

	numMonths := r.Iterations(6)

	ledger := LendingLifecycleLedger

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
	if _, err := r.Step("Setup", LendingLifecycleSetupActions()...); err != nil {
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

			principal := new(big.Int).Div(big.NewInt(loanAmount), big.NewInt(int64(numMonths)))
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
