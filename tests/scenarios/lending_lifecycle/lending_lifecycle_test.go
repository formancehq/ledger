//go:build scenario

package lendinglifecycle

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/tests/scenarios/scenariotest"
)

// TestLendingLifecycle models a consumer lending operation:
// - 10 loan disbursements from a funding pool
// - Monthly repayment cycles (principal + interest) over 6 months
// - 2 borrowers default (partial payments then write-off)
// - 1 early full repayment
// - Interest accrual to a revenue account
// - Provisions for doubtful debts
// - Period closes per cycle
//
// Account structure:
//
//	funding:pool         — bank's lending pool (funded from @world)
//	borrower:{id}:loan   — outstanding principal per borrower
//	borrower:{id}:wallet — borrower's wallet (receives disbursement, pays from here)
//	revenue:interest     — accrued interest income
//	expense:provision    — provision for doubtful debts
//	recovery:pool        — collections/write-off contra account
//
// Generates ~130 Apply calls, triggers 2+ cache rotations.
func TestLendingLifecycle(t *testing.T) {
	const (
		ledger       = "lending"
		numBorrowers = 10
		numMonths    = 6
		loanAmount   = 100_000 // USD/2 cents
		monthlyRate  = 2       // 2% monthly interest (simplified)
	)

	sc := scenariotest.SetupSingleNode(t, scenariotest.HTTPPort+5, scenariotest.GRPCPort+5)
	ctx, client := sc.Ctx(), sc.Client

	// Expected balances tracking
	fundingBalance := new(big.Int)
	borrowerLoanBalance := make(map[int]*big.Int, numBorrowers)    // outstanding principal
	borrowerWalletBalance := make(map[int]*big.Int, numBorrowers)  // wallet balance
	interestRevenue := new(big.Int)
	provisionBalance := new(big.Int)
	recoveryBalance := new(big.Int)

	for i := 1; i <= numBorrowers; i++ {
		borrowerLoanBalance[i] = new(big.Int)
		borrowerWalletBalance[i] = new(big.Int)
	}

	// Borrowers who will default (partial payments then write-off)
	defaulters := map[int]bool{3: true, 7: true}
	// Borrower who repays early (full repayment in month 2)
	earlyRepayer := 5

	// --- Phase 1: Setup ---
	t.Run("Setup", func(t *testing.T) {
		scenariotest.ApplyActions(t, ctx, client,
			testutil.CreateLedgerAction(ledger, nil),
			// Account types
			testutil.AddAccountTypeAction(ledger, "funding", "funding:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction(ledger, "borrower-loan", "borrower:{id}:loan", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction(ledger, "borrower-wallet", "borrower:{id}:wallet", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction(ledger, "revenue", "revenue:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction(ledger, "expense", "expense:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction(ledger, "recovery", "recovery:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),

			// Numscripts
			testutil.SaveNumscriptWithVersionAction(ledger, "fund_pool", `vars {
  monetary $amount
}
send $amount (
  source = @world
  destination = @funding:pool
)`, "1.0.0"),

			testutil.SaveNumscriptWithVersionAction(ledger, "disburse_loan", `vars {
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

			testutil.SaveNumscriptWithVersionAction(ledger, "repay_principal", `vars {
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

			testutil.SaveNumscriptWithVersionAction(ledger, "accrue_interest", `vars {
  account $borrower_wallet
  monetary $amount
}
send $amount (
  source = $borrower_wallet
  destination = @revenue:interest
)`, "1.0.0"),

			testutil.SaveNumscriptWithVersionAction(ledger, "provision", `vars {
  monetary $amount
}
send $amount (
  source = @world
  destination = @expense:provision
)`, "1.0.0"),

			testutil.SaveNumscriptWithVersionAction(ledger, "write_off", `vars {
  account $borrower_loan
  monetary $amount
}
send $amount (
  source = $borrower_loan
  destination = @recovery:pool
)`, "1.0.0"),
		)

		// Fund the lending pool
		scenariotest.ApplyActions(t, ctx, client,
			testutil.CreateScriptRefTransactionAction(ledger, "fund_pool", "1.0.0", map[string]string{
				"amount": fmt.Sprintf("USD/2 %d", loanAmount*numBorrowers),
			}, nil),
		)
		fundingBalance.SetInt64(int64(loanAmount * numBorrowers))
	})

	// --- Phase 2: Loan Disbursements ---
	t.Run("Disbursements", func(t *testing.T) {
		actions := make([]*servicepb.Request, 0, numBorrowers)
		for i := 1; i <= numBorrowers; i++ {
			action := testutil.CreateScriptRefTransactionAction(ledger, "disburse_loan", "1.0.0", map[string]string{
				"borrower_loan":   fmt.Sprintf("borrower:%d:loan", i),
				"borrower_wallet": fmt.Sprintf("borrower:%d:wallet", i),
				"amount":          fmt.Sprintf("USD/2 %d", loanAmount),
			}, nil)
			action.GetApply().GetCreateTransaction().Reference = fmt.Sprintf("loan-disburse-%d", i)
			actions = append(actions, action)

			fundingBalance.Sub(fundingBalance, big.NewInt(loanAmount))
			borrowerLoanBalance[i].SetInt64(loanAmount)
			borrowerWalletBalance[i].SetInt64(loanAmount)
		}
		scenariotest.ApplyActions(t, ctx, client, actions...)

		// Verify funding pool is depleted
		scenariotest.CheckAccountBalance(t, ctx, client, ledger, "funding:pool", "USD/2", fundingBalance)
	})

	// --- Phase 3: Monthly Repayment Cycles ---
	t.Run("RepayCycles", func(t *testing.T) {
		for month := 1; month <= numMonths; month++ {
			t.Run(fmt.Sprintf("Month%d", month), func(t *testing.T) {
				var actions []*servicepb.Request

				for i := 1; i <= numBorrowers; i++ {
					outstanding := borrowerLoanBalance[i]
					wallet := borrowerWalletBalance[i]

					// Skip if loan already fully repaid or written off
					if outstanding.Sign() <= 0 {
						continue
					}

					// Early repayer: full repayment in month 2
					if i == earlyRepayer && month == 2 {
						// Pay interest first, then repay as much principal as wallet allows
						interest := new(big.Int).Mul(outstanding, big.NewInt(monthlyRate))
						interest.Div(interest, big.NewInt(100))

						// Interest payment
						actions = append(actions,
							testutil.CreateScriptRefTransactionAction(ledger, "accrue_interest", "1.0.0", map[string]string{
								"borrower_wallet": fmt.Sprintf("borrower:%d:wallet", i),
								"amount":          fmt.Sprintf("USD/2 %s", interest.String()),
							}, map[string]string{"type": "early-repay-interest"}),
						)
						wallet.Sub(wallet, interest)
						interestRevenue.Add(interestRevenue, interest)

						// Repay principal — capped by remaining wallet balance
						repayAmount := new(big.Int).Set(outstanding)
						if repayAmount.Cmp(wallet) > 0 {
							repayAmount.Set(wallet)
						}
						actions = append(actions,
							testutil.CreateScriptRefTransactionAction(ledger, "repay_principal", "1.0.0", map[string]string{
								"borrower_wallet": fmt.Sprintf("borrower:%d:wallet", i),
								"borrower_loan":   fmt.Sprintf("borrower:%d:loan", i),
								"amount":          fmt.Sprintf("USD/2 %s", repayAmount.String()),
							}, map[string]string{"type": "early-repay-principal"}),
						)
						wallet.Sub(wallet, repayAmount)
						fundingBalance.Add(fundingBalance, repayAmount)
						outstanding.Sub(outstanding, repayAmount)
						continue
					}

					// Defaulters: pay only in months 1-2, then stop
					if defaulters[i] && month > 2 {
						// No payment from defaulters after month 2
						continue
					}

					// Calculate monthly interest
					interest := new(big.Int).Mul(outstanding, big.NewInt(monthlyRate))
					interest.Div(interest, big.NewInt(100))

					// Calculate monthly principal (equal installments over 6 months, simplified)
					principal := new(big.Int).Div(big.NewInt(loanAmount), big.NewInt(numMonths))

					// Cap principal to outstanding
					if principal.Cmp(outstanding) > 0 {
						principal.Set(outstanding)
					}

					// Ensure wallet has enough (interest first, then principal)
					totalPayment := new(big.Int).Add(interest, principal)
					if totalPayment.Cmp(wallet) > 0 {
						// Reduce principal to fit
						if interest.Cmp(wallet) >= 0 {
							continue // Cannot even pay interest
						}
						principal.Sub(wallet, interest)
						totalPayment.Set(wallet)
					}

					// Interest payment
					if interest.Sign() > 0 {
						actions = append(actions,
							testutil.CreateScriptRefTransactionAction(ledger, "accrue_interest", "1.0.0", map[string]string{
								"borrower_wallet": fmt.Sprintf("borrower:%d:wallet", i),
								"amount":          fmt.Sprintf("USD/2 %s", interest.String()),
							}, map[string]string{
								"month": fmt.Sprintf("%d", month),
								"type":  "interest",
							}),
						)
						wallet.Sub(wallet, interest)
						interestRevenue.Add(interestRevenue, interest)
					}

					// Principal repayment
					if principal.Sign() > 0 {
						actions = append(actions,
							testutil.CreateScriptRefTransactionAction(ledger, "repay_principal", "1.0.0", map[string]string{
								"borrower_wallet": fmt.Sprintf("borrower:%d:wallet", i),
								"borrower_loan":   fmt.Sprintf("borrower:%d:loan", i),
								"amount":          fmt.Sprintf("USD/2 %s", principal.String()),
							}, map[string]string{
								"month": fmt.Sprintf("%d", month),
								"type":  "principal",
							}),
						)
						wallet.Sub(wallet, principal)
						outstanding.Sub(outstanding, principal)
						fundingBalance.Add(fundingBalance, principal)
					}
				}

				if len(actions) > 0 {
					scenariotest.ApplyActions(t, ctx, client, actions...)
				}

				// Close period each month
				scenariotest.ClosePeriodAndWait(t, ctx, client, "period close month %d", month)
			})
		}
	})

	// --- Phase 4: Provision for Doubtful Debts ---
	t.Run("Provisions", func(t *testing.T) {
		// Provision the full outstanding balance of defaulters
		var actions []*servicepb.Request
		for id := range defaulters {
			outstanding := borrowerLoanBalance[id]
			if outstanding.Sign() > 0 {
				actions = append(actions,
					testutil.CreateScriptRefTransactionAction(ledger, "provision", "1.0.0", map[string]string{
						"amount": fmt.Sprintf("USD/2 %s", outstanding.String()),
					}, map[string]string{
						"borrower": fmt.Sprintf("borrower:%d", id),
						"reason":   "default",
					}),
				)
				provisionBalance.Add(provisionBalance, outstanding)
			}
		}
		if len(actions) > 0 {
			scenariotest.ApplyActions(t, ctx, client, actions...)
		}
	})

	// --- Phase 5: Write-off Defaulted Loans ---
	t.Run("WriteOffs", func(t *testing.T) {
		var actions []*servicepb.Request
		for id := range defaulters {
			outstanding := borrowerLoanBalance[id]
			if outstanding.Sign() > 0 {
				actions = append(actions,
					testutil.CreateScriptRefTransactionAction(ledger, "write_off", "1.0.0", map[string]string{
						"borrower_loan": fmt.Sprintf("borrower:%d:loan", id),
						"amount":        fmt.Sprintf("USD/2 %s", outstanding.String()),
					}, map[string]string{
						"reason": "write-off-default",
					}),
				)
				recoveryBalance.Add(recoveryBalance, outstanding)
				outstanding.SetInt64(0)
			}
		}
		if len(actions) > 0 {
			scenariotest.ApplyActions(t, ctx, client, actions...)
		}
	})

	// --- Phase 6: Metadata Enrichment ---
	t.Run("Metadata", func(t *testing.T) {
		// Tag defaulted borrowers
		for id := range defaulters {
			scenariotest.ApplyActions(t, ctx, client,
				testutil.SaveAccountMetadataAction(ledger, fmt.Sprintf("borrower:%d:loan", id), map[string]string{
					"status": "written-off",
				}),
			)
		}
		// Tag early repayer
		scenariotest.ApplyActions(t, ctx, client,
			testutil.SaveAccountMetadataAction(ledger, fmt.Sprintf("borrower:%d:loan", earlyRepayer), map[string]string{
				"status": "repaid-early",
			}),
		)
	})

	// --- Phase 7: Prepared Queries with typed parameters ---
	t.Run("PreparedQueries", func(t *testing.T) {
		// Create a metadata index on "status" before querying
		scenariotest.ApplyActions(t, ctx, client,
			testutil.CreateAccountMetadataIndexAction(ledger, "status"),
		)

		// Wait for index backfill to complete (lag=0 and no backfill in progress)
		require.Eventually(t, func() bool {
			indexStatus, err := testutil.GetIndexStatus(ctx, client)
			if err != nil {
				return false
			}
			return indexStatus.GetLag() == 0 && len(indexStatus.GetBackfillProgress()) == 0
		}, 15*time.Second, 200*time.Millisecond, "index backfill should complete")

		// 1. Parameterized string metadata — query by loan status at runtime
		err := testutil.CreatePreparedQuery(ctx, client, "loans-by-status", ledger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			testutil.ParamStringMetadataFilter("status", "status_value"),
		)
		require.NoError(t, err, "CreatePreparedQuery(loans-by-status) failed")

		// Query for written-off loans — should find exactly the defaulters
		resp, err := testutil.ExecutePreparedQueryWithParams(ctx, client, ledger, "loans-by-status",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"status_value": testutil.StringParam("written-off")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(written-off) failed")
		cursor := resp.GetCursor()
		require.NotNil(t, cursor, "expected cursor result from prepared query")
		require.Equal(t, len(defaulters), len(cursor.GetAccountData()),
			"should find exactly %d defaulted loan accounts", len(defaulters))

		// Query for early-repaid loans — should find exactly 1 (borrower 5)
		resp, err = testutil.ExecutePreparedQueryWithParams(ctx, client, ledger, "loans-by-status",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"status_value": testutil.StringParam("repaid-early")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(repaid-early) failed")
		require.Equal(t, 1, len(resp.GetCursor().GetAccountData()),
			"should find exactly 1 early-repaid loan account")

		// Query for a status nobody has — should return 0
		resp, err = testutil.ExecutePreparedQueryWithParams(ctx, client, ledger, "loans-by-status",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"status_value": testutil.StringParam("active")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(active) failed")
		require.Empty(t, resp.GetCursor().GetAccountData(),
			"no loans should have status=active")

		// 2. Parameterized address prefix — filter borrower accounts by prefix
		err = testutil.CreatePreparedQuery(ctx, client, "accounts-by-prefix", ledger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			testutil.ParamAddressPrefixFilter("prefix"),
		)
		require.NoError(t, err, "CreatePreparedQuery(accounts-by-prefix) failed")

		// Query for all borrower loan accounts
		resp, err = testutil.ExecutePreparedQueryWithParams(ctx, client, ledger, "accounts-by-prefix",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"prefix": testutil.StringParam("borrower:")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(borrower:) failed")
		// 10 borrowers × 2 accounts each (loan + wallet) = 20
		require.Equal(t, numBorrowers*2, len(resp.GetCursor().GetAccountData()),
			"should find all borrower accounts (loan + wallet)")

		// Query for funding accounts only
		resp, err = testutil.ExecutePreparedQueryWithParams(ctx, client, ledger, "accounts-by-prefix",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"prefix": testutil.StringParam("funding:")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(funding:) failed")
		require.Equal(t, 1, len(resp.GetCursor().GetAccountData()),
			"should find exactly 1 funding account")

		// Cleanup
		require.NoError(t, testutil.DeletePreparedQuery(ctx, client, ledger, "loans-by-status"))
		require.NoError(t, testutil.DeletePreparedQuery(ctx, client, ledger, "accounts-by-prefix"))
	})

	// --- Phase 8: Final Invariants ---
	t.Run("FinalInvariants", func(t *testing.T) {
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
		scenariotest.CheckNoNegativeBalances(t, ctx, client, ledger, []string{"world"})

		// Verify interest revenue is positive
		scenariotest.CheckPositiveBalance(t, ctx, client, ledger, "revenue:interest", "USD/2")

		// Verify all non-defaulted, non-early borrower loans are zero
		for i := 1; i <= numBorrowers; i++ {
			if defaulters[i] || i == earlyRepayer {
				continue
			}
			scenariotest.CheckAccountBalance(t, ctx, client, ledger, fmt.Sprintf("borrower:%d:loan", i), "USD/2", borrowerLoanBalance[i])
		}

		// Verify written-off loans are zero
		for id := range defaulters {
			scenariotest.CheckAccountBalance(t, ctx, client, ledger, fmt.Sprintf("borrower:%d:loan", id), "USD/2", big.NewInt(0))
		}

		// Verify recovery pool got the write-offs
		scenariotest.CheckPositiveBalance(t, ctx, client, ledger, "recovery:pool", "USD/2")

		// Stats
		stats, err := testutil.GetLedgerStats(ctx, client, ledger)
		require.NoError(t, err)
		t.Logf("LedgerStats: %d accounts, %d transactions",
			stats.GetAccountCount(), stats.GetTransactionCount())
	})

	// --- Tail phases ---
	scenariotest.RunPostTestPhases(t, sc, func(t *testing.T, client servicepb.BucketServiceClient) {
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
		scenariotest.CheckNoNegativeBalances(t, ctx, client, ledger, []string{"world"})
	})
}
