//go:build scenario

package multiledgerpayroll

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/scenario"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/tests/scenarios/scenariotest"
)

// TestMultiLedgerPayroll models a company with 3 departments, each with its own ledger.
// A central "clearing" ledger handles inter-department transfers and payroll funding.
//
// This tests multi-ledger isolation:
// - Same account names in different ledgers must not interfere
// - Transactions in one ledger don't affect another
// - Numscripts and account types are scoped per ledger
// - Cross-ledger balance reconciliation via clearing
//
// Departments: engineering (15 employees), sales (10), operations (5)
// Each month: company funds clearing → clearing funds depts → depts pay employees
// 3 monthly cycles, plus inter-department cost allocations.
//
// Generates ~150 Apply calls across 4 ledgers.
func TestMultiLedgerPayroll(t *testing.T) {
	const (
		numMonths    = 3
		baseSalary   = 500_000 // USD/2 cents
		bonusPercent = 10
	)

	departments := scenario.MultiLedgerPayrollDepartments()

	sc := scenariotest.SetupSingleNode(t, scenariotest.HTTPPort+6, scenariotest.GRPCPort+6)
	ctx, client := sc.Ctx(), sc.Client

	// Balance tracking per ledger
	type ledgerBalances struct {
		payrollPool *big.Int
		employees   map[int]*big.Int
	}
	balances := make(map[string]*ledgerBalances, len(departments))
	clearingFunded := new(big.Int) // total funded into clearing

	// Initialize balance tracking
	for _, dept := range departments {
		balances[dept.Ledger] = &ledgerBalances{
			payrollPool: new(big.Int),
			employees:   make(map[int]*big.Int, dept.Employees),
		}
		for i := 1; i <= dept.Employees; i++ {
			balances[dept.Ledger].employees[i] = new(big.Int)
		}
	}

	// --- Phase 1: Setup All Ledgers ---
	t.Run("Setup", func(t *testing.T) {
		scenariotest.ApplyActions(t, ctx, client, scenario.MultiLedgerPayrollSetupActions()...)
	})

	// --- Phase 2: Monthly Payroll Cycles ---
	t.Run("PayrollCycles", func(t *testing.T) {
		for month := 1; month <= numMonths; month++ {
			t.Run(fmt.Sprintf("Month%d", month), func(t *testing.T) {
				// Step 1: Fund clearing ledger from @world
				totalNeeded := int64(0)
				for _, dept := range departments {
					totalNeeded += int64(dept.Employees) * baseSalary
				}
				scenariotest.ApplyActions(t, ctx, client,
					actions.CreateScriptRefTransactionAction("clearing", "fund_clearing", "1.0.0", map[string]string{
						"amount": fmt.Sprintf("USD/2 %d", totalNeeded),
					}, map[string]string{"month": fmt.Sprintf("%d", month)}),
				)
				clearingFunded.Add(clearingFunded, big.NewInt(totalNeeded))

				// Step 2: Distribute from clearing to department accounts
				var deptActions []*servicepb.Request
				for _, dept := range departments {
					amount := int64(dept.Employees) * baseSalary
					deptActions = append(deptActions,
						actions.CreateScriptRefTransactionAction("clearing", "fund_dept", "1.0.0", map[string]string{
							"dept_account": fmt.Sprintf("dept:%s", dept.Name),
							"amount":       fmt.Sprintf("USD/2 %d", amount),
						}, map[string]string{"month": fmt.Sprintf("%d", month)}),
					)
				}
				scenariotest.ApplyActions(t, ctx, client, deptActions...)

				// Step 3: Fund department payroll pools (from @world, mirroring clearing allocation)
				var payrollActions []*servicepb.Request
				for _, dept := range departments {
					amount := int64(dept.Employees) * baseSalary
					payrollActions = append(payrollActions,
						actions.CreateScriptRefTransactionAction(dept.Ledger, "fund_payroll", "1.0.0", map[string]string{
							"amount": fmt.Sprintf("USD/2 %d", amount),
						}, map[string]string{"month": fmt.Sprintf("%d", month)}),
					)
					balances[dept.Ledger].payrollPool.Add(balances[dept.Ledger].payrollPool, big.NewInt(amount))
				}
				scenariotest.ApplyActions(t, ctx, client, payrollActions...)

				// Step 4: Pay employees in each department
				for _, dept := range departments {
					var salaryActions []*servicepb.Request
					for emp := 1; emp <= dept.Employees; emp++ {
						salaryActions = append(salaryActions,
							actions.CreateScriptRefTransactionAction(dept.Ledger, "pay_salary", "1.0.0", map[string]string{
								"employee": fmt.Sprintf("employee:%d", emp),
								"amount":   fmt.Sprintf("USD/2 %d", baseSalary),
							}, map[string]string{
								"month": fmt.Sprintf("%d", month),
								"type":  "salary",
							}),
						)
						balances[dept.Ledger].payrollPool.Sub(balances[dept.Ledger].payrollPool, big.NewInt(baseSalary))
						balances[dept.Ledger].employees[emp].Add(balances[dept.Ledger].employees[emp], big.NewInt(baseSalary))
					}
					scenariotest.ApplyActions(t, ctx, client, salaryActions...)
				}

				// Close period after each cycle
				scenariotest.ClosePeriodAndWait(t, ctx, client, "period close month %d", month)
			})
		}
	})

	// --- Phase 3: Bonuses (month 3 only) ---
	t.Run("Bonuses", func(t *testing.T) {
		// Only engineering gets bonuses
		dept := departments[0]
		bonusAmount := int64(baseSalary * bonusPercent / 100)

		// Fund bonus pool
		totalBonus := bonusAmount * int64(dept.Employees)
		scenariotest.ApplyActions(t, ctx, client,
			actions.CreateScriptRefTransactionAction(dept.Ledger, "fund_payroll", "1.0.0", map[string]string{
				"amount": fmt.Sprintf("USD/2 %d", totalBonus),
			}, map[string]string{"type": "bonus-funding"}),
		)
		balances[dept.Ledger].payrollPool.Add(balances[dept.Ledger].payrollPool, big.NewInt(totalBonus))

		var bonusActions []*servicepb.Request
		for emp := 1; emp <= dept.Employees; emp++ {
			bonusActions = append(bonusActions,
				actions.CreateScriptRefTransactionAction(dept.Ledger, "pay_salary", "1.0.0", map[string]string{
					"employee": fmt.Sprintf("employee:%d", emp),
					"amount":   fmt.Sprintf("USD/2 %d", bonusAmount),
				}, map[string]string{"type": "bonus"}),
			)
			balances[dept.Ledger].payrollPool.Sub(balances[dept.Ledger].payrollPool, big.NewInt(bonusAmount))
			balances[dept.Ledger].employees[emp].Add(balances[dept.Ledger].employees[emp], big.NewInt(bonusAmount))
		}
		scenariotest.ApplyActions(t, ctx, client, bonusActions...)
	})

	// --- Phase 4: Inter-department Cost Allocation ---
	t.Run("CostAllocation", func(t *testing.T) {
		// Engineering charges operations for cloud hosting
		// Sales charges engineering for lead generation
		allocations := []struct {
			from   string
			to     string
			amount int64
			reason string
		}{
			{"dept:operations", "dept:engineering", 50_000, "cloud-hosting"},
			{"dept:engineering", "dept:sales", 30_000, "lead-generation"},
			{"dept:sales", "dept:operations", 20_000, "office-supplies"},
		}

		var reqs []*servicepb.Request
		for _, alloc := range allocations {
			reqs = append(reqs,
				actions.CreateScriptRefTransactionAction("clearing", "cost_allocation", "1.0.0", map[string]string{
					"from_dept": alloc.from,
					"to_dept":   alloc.to,
					"amount":    fmt.Sprintf("USD/2 %d", alloc.amount),
				}, map[string]string{"reason": alloc.reason}),
			)
		}
		scenariotest.ApplyActions(t, ctx, client, reqs...)
	})

	// --- Phase 5: Verify Ledger Isolation ---
	t.Run("LedgerIsolation", func(t *testing.T) {
		// Verify each department ledger has the correct number of employees
		for _, dept := range departments {
			accounts, err := actions.ListAccountsFiltered(ctx, client, dept.Ledger, 100, "",
				actions.AddressPrefixFilter("employee:"))
			require.NoError(t, err, "failed to list employees for %s", dept.Name)
			require.Equal(t, dept.Employees, len(accounts),
				"%s should have %d employees, got %d", dept.Name, dept.Employees, len(accounts))
		}

		// Verify same account name "payroll:pool" exists independently in each dept ledger
		for _, dept := range departments {
			acct, err := actions.GetAccount(ctx, client, dept.Ledger, "payroll:pool")
			require.NoError(t, err, "payroll:pool should exist in %s", dept.Name)
			require.NotNil(t, acct)
		}

		// Verify clearing ledger has department accounts
		clearingAccounts, err := actions.ListAccountsFiltered(ctx, client, "clearing", 100, "",
			actions.AddressPrefixFilter("dept:"))
		require.NoError(t, err)
		require.Equal(t, len(departments), len(clearingAccounts), "clearing should have %d dept accounts", len(departments))

		// Verify employee accounts don't exist in clearing ledger
		clearingEmployees, err := actions.ListAccountsFiltered(ctx, client, "clearing", 100, "",
			actions.AddressPrefixFilter("employee:"))
		require.NoError(t, err)
		require.Empty(t, clearingEmployees, "clearing ledger should have no employee accounts")
	})

	// --- Phase 6: Numscript Versioning ---
	t.Run("NumscriptVersioning", func(t *testing.T) {
		// Save a v2 of pay_salary with a bonus metadata field
		dept := departments[1] // sales
		scenariotest.ApplyActions(t, ctx, client,
			actions.SaveNumscriptWithVersionAction(dept.Ledger, "pay_salary", `vars {
  account $employee
  monetary $amount
}
send $amount (
  source = @payroll:pool
  destination = $employee
)`, "2.0.0"),
		)

		// Verify both versions exist
		v1, err := actions.GetNumscript(ctx, client, dept.Ledger, "pay_salary", "1.0.0")
		require.NoError(t, err, "should find pay_salary v1.0.0")
		require.Equal(t, "1.0.0", v1.GetVersion())

		v2, err := actions.GetNumscript(ctx, client, dept.Ledger, "pay_salary", "2.0.0")
		require.NoError(t, err, "should find pay_salary v2.0.0")
		require.Equal(t, "2.0.0", v2.GetVersion())

		// Use v2 for one more payment
		scenariotest.ApplyActions(t, ctx, client,
			actions.CreateScriptRefTransactionAction(dept.Ledger, "fund_payroll", "1.0.0", map[string]string{
				"amount": "USD/2 50000",
			}, nil),
			actions.CreateScriptRefTransactionAction(dept.Ledger, "pay_salary", "2.0.0", map[string]string{
				"employee": "employee:1",
				"amount":   "USD/2 50000",
			}, map[string]string{"type": "v2-test"}),
		)
		balances[dept.Ledger].employees[1].Add(balances[dept.Ledger].employees[1], big.NewInt(50_000))

		// Duplicate semver should fail
		err = scenariotest.ApplyActionsExpectError(ctx, client,
			actions.SaveNumscriptWithVersionAction(dept.Ledger, "pay_salary", `send [USD/2 1] (source = @world destination = @world)`, "1.0.0"),
		)
		require.Error(t, err, "duplicate semver version should fail")
	})

	// --- Phase 7: Final Invariants ---
	t.Run("FinalInvariants", func(t *testing.T) {
		// Double-entry must hold in every ledger
		for _, dept := range departments {
			scenariotest.CheckDoubleEntryBalance(t, ctx, client, dept.Ledger)
			scenariotest.CheckNoNegativeBalances(t, ctx, client, dept.Ledger, []string{"world"})
		}
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, "clearing")
		scenariotest.CheckNoNegativeBalances(t, ctx, client, "clearing", []string{"world"})

		// Verify employee balances in each department
		for _, dept := range departments {
			for emp := 1; emp <= dept.Employees; emp++ {
				expected := balances[dept.Ledger].employees[emp]
				scenariotest.CheckAccountBalance(t, ctx, client, dept.Ledger,
					fmt.Sprintf("employee:%d", emp), "USD/2", expected)
			}
		}

		// Verify payroll pools are zero (all paid out)
		for _, dept := range departments {
			scenariotest.CheckAccountBalance(t, ctx, client, dept.Ledger, "payroll:pool", "USD/2", balances[dept.Ledger].payrollPool)
		}

		// Stats per ledger
		for _, dept := range departments {
			stats, err := actions.GetLedgerStats(ctx, client, dept.Ledger)
			require.NoError(t, err)
			t.Logf("%s: %d accounts, %d transactions",
				dept.Name, stats.GetAccountCount(), stats.GetTransactionCount())
		}
	})

	// --- Tail phases ---
	scenariotest.RunPostTestPhases(t, sc, func(t *testing.T, client servicepb.BucketServiceClient) {
		for _, dept := range departments {
			scenariotest.CheckDoubleEntryBalance(t, ctx, client, dept.Ledger)
			scenariotest.CheckNoNegativeBalances(t, ctx, client, dept.Ledger, []string{"world"})
		}
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, "clearing")
	})
}
