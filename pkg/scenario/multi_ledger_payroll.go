package scenario

import (
	"fmt"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/scenario/actions"
)

func init() { Register("multi-ledger-payroll", RunMultiLedgerPayroll) }

// RunMultiLedgerPayroll provisions a company payroll scenario across 4 ledgers:
// a central clearing ledger and 3 department ledgers (engineering, sales, operations).
// Runs 3 monthly cycles, engineering bonuses, and inter-department cost allocations.
func RunMultiLedgerPayroll(r *Runner) error {
	const (
		numMonths    = 3
		baseSalary   = 500_000
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

	// --- Setup All Ledgers ---
	{
		setupReqs := []*servicepb.Request{
			actions.CreateLedgerAction("clearing", nil),
			actions.AddAccountTypeAction("clearing", "company", "company:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			actions.AddAccountTypeAction("clearing", "dept", "dept:{name}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		}
		for _, dept := range departments {
			setupReqs = append(setupReqs,
				actions.CreateLedgerAction(dept.ledger, nil),
				actions.AddAccountTypeAction(dept.ledger, "payroll", "payroll:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				actions.AddAccountTypeAction(dept.ledger, "employee", "employee:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
				actions.AddAccountTypeAction(dept.ledger, "expense", "expense:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			)
		}
		if _, err := r.Step("Setup/Ledgers", setupReqs...); err != nil {
			return err
		}
	}

	// Save clearing-ledger numscripts
	if _, err := r.Step("Setup/ClearingNumscripts",
		actions.SaveNumscriptWithVersionAction("clearing", "fund_clearing", `vars {
  monetary $amount
}
send $amount (
  source = @world
  destination = @company:treasury
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction("clearing", "fund_dept", `vars {
  account $dept_account
  monetary $amount
}
send $amount (
  source = @company:treasury
  destination = $dept_account
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction("clearing", "cost_allocation", `vars {
  account $from_dept
  account $to_dept
  monetary $amount
}
send $amount (
  source = $from_dept
  destination = $to_dept
)`, "1.0.0"),
	); err != nil {
		return err
	}

	// Save department-scoped numscripts
	for _, dept := range departments {
		if _, err := r.Step("Setup/Numscripts/"+dept.name,
			actions.SaveNumscriptWithVersionAction(dept.ledger, "fund_payroll", `vars {
  monetary $amount
}
send $amount (
  source = @world
  destination = @payroll:pool
)`, "1.0.0"),
			actions.SaveNumscriptWithVersionAction(dept.ledger, "pay_salary", `vars {
  account $employee
  monetary $amount
}
send $amount (
  source = @payroll:pool
  destination = $employee
)`, "1.0.0"),
		); err != nil {
			return err
		}
	}

	// --- Monthly Payroll Cycles ---
	for month := 1; month <= numMonths; month++ {
		// Step 1: Fund clearing ledger
		totalNeeded := int64(0)
		for _, dept := range departments {
			totalNeeded += int64(dept.employees) * baseSalary
		}
		if _, err := r.Step(fmt.Sprintf("Month%d/FundClearing", month),
			actions.CreateScriptRefTransactionAction("clearing", "fund_clearing", "1.0.0", map[string]string{
				"amount": fmt.Sprintf("USD/2 %d", totalNeeded),
			}, map[string]string{"month": strconv.Itoa(month)}),
		); err != nil {
			return err
		}

		// Step 2: Distribute to department accounts in clearing ledger
		var deptReqs []*servicepb.Request
		for _, dept := range departments {
			amount := int64(dept.employees) * baseSalary
			deptReqs = append(deptReqs,
				actions.CreateScriptRefTransactionAction("clearing", "fund_dept", "1.0.0", map[string]string{
					"dept_account": "dept:" + dept.name,
					"amount":       fmt.Sprintf("USD/2 %d", amount),
				}, map[string]string{"month": strconv.Itoa(month)}),
			)
		}
		if _, err := r.Step(fmt.Sprintf("Month%d/DistributeToDepts", month), deptReqs...); err != nil {
			return err
		}

		// Step 3: Fund department payroll pools
		var payrollReqs []*servicepb.Request
		for _, dept := range departments {
			amount := int64(dept.employees) * baseSalary
			payrollReqs = append(payrollReqs,
				actions.CreateScriptRefTransactionAction(dept.ledger, "fund_payroll", "1.0.0", map[string]string{
					"amount": fmt.Sprintf("USD/2 %d", amount),
				}, map[string]string{"month": strconv.Itoa(month)}),
			)
		}
		if _, err := r.Step(fmt.Sprintf("Month%d/FundPayrollPools", month), payrollReqs...); err != nil {
			return err
		}

		// Step 4: Pay employees in each department
		for _, dept := range departments {
			var salaryReqs []*servicepb.Request
			for emp := 1; emp <= dept.employees; emp++ {
				salaryReqs = append(salaryReqs,
					actions.CreateScriptRefTransactionAction(dept.ledger, "pay_salary", "1.0.0", map[string]string{
						"employee": fmt.Sprintf("employee:%d", emp),
						"amount":   fmt.Sprintf("USD/2 %d", baseSalary),
					}, map[string]string{
						"month": strconv.Itoa(month),
						"type":  "salary",
					}),
				)
			}
			if _, err := r.Step(fmt.Sprintf("Month%d/PayEmployees/%s", month, dept.name), salaryReqs...); err != nil {
				return err
			}
		}
	}

	// --- Bonuses (engineering only) ---
	{
		dept := departments[0]
		bonusAmount := int64(baseSalary * bonusPercent / 100)
		totalBonus := bonusAmount * int64(dept.employees)

		if _, err := r.Step("Bonuses/FundPool",
			actions.CreateScriptRefTransactionAction(dept.ledger, "fund_payroll", "1.0.0", map[string]string{
				"amount": fmt.Sprintf("USD/2 %d", totalBonus),
			}, map[string]string{"type": "bonus-funding"}),
		); err != nil {
			return err
		}

		var bonusReqs []*servicepb.Request
		for emp := 1; emp <= dept.employees; emp++ {
			bonusReqs = append(bonusReqs,
				actions.CreateScriptRefTransactionAction(dept.ledger, "pay_salary", "1.0.0", map[string]string{
					"employee": fmt.Sprintf("employee:%d", emp),
					"amount":   fmt.Sprintf("USD/2 %d", bonusAmount),
				}, map[string]string{"type": "bonus"}),
			)
		}
		if _, err := r.Step("Bonuses/PayEngineering", bonusReqs...); err != nil {
			return err
		}
	}

	// --- Inter-department Cost Allocation ---
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

	{
		var allocReqs []*servicepb.Request
		for _, alloc := range allocations {
			allocReqs = append(allocReqs,
				actions.CreateScriptRefTransactionAction("clearing", "cost_allocation", "1.0.0", map[string]string{
					"from_dept": alloc.from,
					"to_dept":   alloc.to,
					"amount":    fmt.Sprintf("USD/2 %d", alloc.amount),
				}, map[string]string{"reason": alloc.reason}),
			)
		}
		if _, err := r.Step("CostAllocation", allocReqs...); err != nil {
			return err
		}
	}

	return nil
}
