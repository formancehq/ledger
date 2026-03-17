//go:build scenario

package multiledgerpayroll

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
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

	type department struct {
		name      string
		ledger    string
		employees int
	}

	departments := []department{
		{name: "engineering", ledger: "dept-eng", employees: 15},
		{name: "sales", ledger: "dept-sales", employees: 10},
		{name: "operations", ledger: "dept-ops", employees: 5},
	}

	sc := scenariotest.SetupSingleNode(t, scenariotest.HTTPPort+6, scenariotest.GRPCPort+6)
	ctx, client := sc.Ctx(), sc.Client

	// Balance tracking per ledger
	type ledgerBalances struct {
		payrollPool *big.Int
		employees   map[int]*big.Int
	}
	balances := make(map[string]*ledgerBalances, len(departments))
	clearingFunded := new(big.Int) // total funded into clearing

	// --- Phase 1: Setup All Ledgers ---
	t.Run("Setup", func(t *testing.T) {
		// Create clearing ledger
		actions := []*servicepb.Request{
			testutil.CreateLedgerAction("clearing", nil),
			testutil.AddAccountTypeAction("clearing", "company", "company:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction("clearing", "dept", "dept:{name}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		}

		// Create department ledgers with account types
		for _, dept := range departments {
			actions = append(actions,
				testutil.CreateLedgerAction(dept.ledger, nil),
				testutil.AddAccountTypeAction(dept.ledger, "payroll", "payroll:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				testutil.AddAccountTypeAction(dept.ledger, "employee", "employee:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				testutil.AddAccountTypeAction(dept.ledger, "expense", "expense:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			)

			balances[dept.ledger] = &ledgerBalances{
				payrollPool: new(big.Int),
				employees:   make(map[int]*big.Int, dept.employees),
			}
			for i := 1; i <= dept.employees; i++ {
				balances[dept.ledger].employees[i] = new(big.Int)
			}
		}
		scenariotest.ApplyActions(t, ctx, client, actions...)

		// Save numscripts scoped per ledger
		clearingScripts := []*servicepb.Request{
			testutil.SaveNumscriptWithVersionAction("clearing", "fund_clearing", `vars {
  monetary $amount
}
send $amount (
  source = @world
  destination = @company:treasury
)`, "1.0.0"),

			testutil.SaveNumscriptWithVersionAction("clearing", "fund_dept", `vars {
  account $dept_account
  monetary $amount
}
send $amount (
  source = @company:treasury
  destination = $dept_account
)`, "1.0.0"),

			testutil.SaveNumscriptWithVersionAction("clearing", "cost_allocation", `vars {
  account $from_dept
  account $to_dept
  monetary $amount
}
send $amount (
  source = $from_dept
  destination = $to_dept
)`, "1.0.0"),
		}
		scenariotest.ApplyActions(t, ctx, client, clearingScripts...)

		// Save department-scoped numscripts
		for _, dept := range departments {
			scenariotest.ApplyActions(t, ctx, client,
				testutil.SaveNumscriptWithVersionAction(dept.ledger, "fund_payroll", `vars {
  monetary $amount
}
send $amount (
  source = @world
  destination = @payroll:pool
)`, "1.0.0"),

				testutil.SaveNumscriptWithVersionAction(dept.ledger, "pay_salary", `vars {
  account $employee
  monetary $amount
}
send $amount (
  source = @payroll:pool
  destination = $employee
)`, "1.0.0"),
			)
		}
	})

	// --- Phase 2: Monthly Payroll Cycles ---
	t.Run("PayrollCycles", func(t *testing.T) {
		for month := 1; month <= numMonths; month++ {
			t.Run(fmt.Sprintf("Month%d", month), func(t *testing.T) {
				// Step 1: Fund clearing ledger from @world
				totalNeeded := int64(0)
				for _, dept := range departments {
					totalNeeded += int64(dept.employees) * baseSalary
				}
				scenariotest.ApplyActions(t, ctx, client,
					testutil.CreateScriptRefTransactionAction("clearing", "fund_clearing", "1.0.0", map[string]string{
						"amount": fmt.Sprintf("USD/2 %d", totalNeeded),
					}, map[string]string{"month": fmt.Sprintf("%d", month)}),
				)
				clearingFunded.Add(clearingFunded, big.NewInt(totalNeeded))

				// Step 2: Distribute from clearing to department accounts
				var deptActions []*servicepb.Request
				for _, dept := range departments {
					amount := int64(dept.employees) * baseSalary
					deptActions = append(deptActions,
						testutil.CreateScriptRefTransactionAction("clearing", "fund_dept", "1.0.0", map[string]string{
							"dept_account": fmt.Sprintf("dept:%s", dept.name),
							"amount":       fmt.Sprintf("USD/2 %d", amount),
						}, map[string]string{"month": fmt.Sprintf("%d", month)}),
					)
				}
				scenariotest.ApplyActions(t, ctx, client, deptActions...)

				// Step 3: Fund department payroll pools (from @world, mirroring clearing allocation)
				var payrollActions []*servicepb.Request
				for _, dept := range departments {
					amount := int64(dept.employees) * baseSalary
					payrollActions = append(payrollActions,
						testutil.CreateScriptRefTransactionAction(dept.ledger, "fund_payroll", "1.0.0", map[string]string{
							"amount": fmt.Sprintf("USD/2 %d", amount),
						}, map[string]string{"month": fmt.Sprintf("%d", month)}),
					)
					balances[dept.ledger].payrollPool.Add(balances[dept.ledger].payrollPool, big.NewInt(amount))
				}
				scenariotest.ApplyActions(t, ctx, client, payrollActions...)

				// Step 4: Pay employees in each department
				for _, dept := range departments {
					var salaryActions []*servicepb.Request
					for emp := 1; emp <= dept.employees; emp++ {
						salaryActions = append(salaryActions,
							testutil.CreateScriptRefTransactionAction(dept.ledger, "pay_salary", "1.0.0", map[string]string{
								"employee": fmt.Sprintf("employee:%d", emp),
								"amount":   fmt.Sprintf("USD/2 %d", baseSalary),
							}, map[string]string{
								"month": fmt.Sprintf("%d", month),
								"type":  "salary",
							}),
						)
						balances[dept.ledger].payrollPool.Sub(balances[dept.ledger].payrollPool, big.NewInt(baseSalary))
						balances[dept.ledger].employees[emp].Add(balances[dept.ledger].employees[emp], big.NewInt(baseSalary))
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
		totalBonus := bonusAmount * int64(dept.employees)
		scenariotest.ApplyActions(t, ctx, client,
			testutil.CreateScriptRefTransactionAction(dept.ledger, "fund_payroll", "1.0.0", map[string]string{
				"amount": fmt.Sprintf("USD/2 %d", totalBonus),
			}, map[string]string{"type": "bonus-funding"}),
		)
		balances[dept.ledger].payrollPool.Add(balances[dept.ledger].payrollPool, big.NewInt(totalBonus))

		var bonusActions []*servicepb.Request
		for emp := 1; emp <= dept.employees; emp++ {
			bonusActions = append(bonusActions,
				testutil.CreateScriptRefTransactionAction(dept.ledger, "pay_salary", "1.0.0", map[string]string{
					"employee": fmt.Sprintf("employee:%d", emp),
					"amount":   fmt.Sprintf("USD/2 %d", bonusAmount),
				}, map[string]string{"type": "bonus"}),
			)
			balances[dept.ledger].payrollPool.Sub(balances[dept.ledger].payrollPool, big.NewInt(bonusAmount))
			balances[dept.ledger].employees[emp].Add(balances[dept.ledger].employees[emp], big.NewInt(bonusAmount))
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

		var actions []*servicepb.Request
		for _, alloc := range allocations {
			actions = append(actions,
				testutil.CreateScriptRefTransactionAction("clearing", "cost_allocation", "1.0.0", map[string]string{
					"from_dept": alloc.from,
					"to_dept":   alloc.to,
					"amount":    fmt.Sprintf("USD/2 %d", alloc.amount),
				}, map[string]string{"reason": alloc.reason}),
			)
		}
		scenariotest.ApplyActions(t, ctx, client, actions...)
	})

	// --- Phase 5: Verify Ledger Isolation ---
	t.Run("LedgerIsolation", func(t *testing.T) {
		// Verify each department ledger has the correct number of employees
		for _, dept := range departments {
			accounts, err := testutil.ListAccountsFiltered(ctx, client, dept.ledger, 100, "",
				testutil.AddressPrefixFilter("employee:"))
			require.NoError(t, err, "failed to list employees for %s", dept.name)
			require.Equal(t, dept.employees, len(accounts),
				"%s should have %d employees, got %d", dept.name, dept.employees, len(accounts))
		}

		// Verify same account name "payroll:pool" exists independently in each dept ledger
		for _, dept := range departments {
			acct, err := testutil.GetAccount(ctx, client, dept.ledger, "payroll:pool")
			require.NoError(t, err, "payroll:pool should exist in %s", dept.name)
			require.NotNil(t, acct)
		}

		// Verify clearing ledger has department accounts
		clearingAccounts, err := testutil.ListAccountsFiltered(ctx, client, "clearing", 100, "",
			testutil.AddressPrefixFilter("dept:"))
		require.NoError(t, err)
		require.Equal(t, len(departments), len(clearingAccounts), "clearing should have %d dept accounts", len(departments))

		// Verify employee accounts don't exist in clearing ledger
		clearingEmployees, err := testutil.ListAccountsFiltered(ctx, client, "clearing", 100, "",
			testutil.AddressPrefixFilter("employee:"))
		require.NoError(t, err)
		require.Empty(t, clearingEmployees, "clearing ledger should have no employee accounts")
	})

	// --- Phase 6: Numscript Versioning ---
	t.Run("NumscriptVersioning", func(t *testing.T) {
		// Save a v2 of pay_salary with a bonus metadata field
		dept := departments[1] // sales
		scenariotest.ApplyActions(t, ctx, client,
			testutil.SaveNumscriptWithVersionAction(dept.ledger, "pay_salary", `vars {
  account $employee
  monetary $amount
}
send $amount (
  source = @payroll:pool
  destination = $employee
)`, "2.0.0"),
		)

		// Verify both versions exist
		v1, err := testutil.GetNumscript(ctx, client, dept.ledger, "pay_salary", "1.0.0")
		require.NoError(t, err, "should find pay_salary v1.0.0")
		require.Equal(t, "1.0.0", v1.GetVersion())

		v2, err := testutil.GetNumscript(ctx, client, dept.ledger, "pay_salary", "2.0.0")
		require.NoError(t, err, "should find pay_salary v2.0.0")
		require.Equal(t, "2.0.0", v2.GetVersion())

		// Use v2 for one more payment
		scenariotest.ApplyActions(t, ctx, client,
			testutil.CreateScriptRefTransactionAction(dept.ledger, "fund_payroll", "1.0.0", map[string]string{
				"amount": "USD/2 50000",
			}, nil),
			testutil.CreateScriptRefTransactionAction(dept.ledger, "pay_salary", "2.0.0", map[string]string{
				"employee": "employee:1",
				"amount":   "USD/2 50000",
			}, map[string]string{"type": "v2-test"}),
		)
		balances[dept.ledger].employees[1].Add(balances[dept.ledger].employees[1], big.NewInt(50_000))

		// Duplicate semver should fail
		err = scenariotest.ApplyActionsExpectError(ctx, client,
			testutil.SaveNumscriptWithVersionAction(dept.ledger, "pay_salary", `send [USD/2 1] (source = @world destination = @world)`, "1.0.0"),
		)
		require.Error(t, err, "duplicate semver version should fail")
	})

	// --- Phase 7: Final Invariants ---
	t.Run("FinalInvariants", func(t *testing.T) {
		// Double-entry must hold in every ledger
		for _, dept := range departments {
			scenariotest.CheckDoubleEntryBalance(t, ctx, client, dept.ledger)
			scenariotest.CheckNoNegativeBalances(t, ctx, client, dept.ledger, []string{"world"})
		}
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, "clearing")
		scenariotest.CheckNoNegativeBalances(t, ctx, client, "clearing", []string{"world"})

		// Verify employee balances in each department
		for _, dept := range departments {
			for emp := 1; emp <= dept.employees; emp++ {
				expected := balances[dept.ledger].employees[emp]
				scenariotest.CheckAccountBalance(t, ctx, client, dept.ledger,
					fmt.Sprintf("employee:%d", emp), "USD/2", expected)
			}
		}

		// Verify payroll pools are zero (all paid out)
		for _, dept := range departments {
			scenariotest.CheckAccountBalance(t, ctx, client, dept.ledger, "payroll:pool", "USD/2", balances[dept.ledger].payrollPool)
		}

		// Stats per ledger
		for _, dept := range departments {
			stats, err := testutil.GetLedgerStats(ctx, client, dept.ledger)
			require.NoError(t, err)
			t.Logf("%s: %d accounts, %d transactions",
				dept.name, stats.GetAccountCount(), stats.GetTransactionCount())
		}
	})

	// --- Tail phases ---
	scenariotest.RunPostTestPhases(t, sc, func(t *testing.T, client servicepb.BucketServiceClient) {
		for _, dept := range departments {
			scenariotest.CheckDoubleEntryBalance(t, ctx, client, dept.ledger)
			scenariotest.CheckNoNegativeBalances(t, ctx, client, dept.ledger, []string{"world"})
		}
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, "clearing")
	})
}
