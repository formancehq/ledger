package scenario

import (
	"fmt"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/scenario/actions"
)

func init() { Register("multi-ledger-payroll", RunMultiLedgerPayroll) }

// MultiLedgerPayrollDepartment describes a department in the payroll scenario.
type MultiLedgerPayrollDepartment struct {
	Name      string
	Ledger    string
	Employees int
}

// MultiLedgerPayrollDepartments returns the department definitions.
func MultiLedgerPayrollDepartments() []MultiLedgerPayrollDepartment {
	return []MultiLedgerPayrollDepartment{
		{Name: "engineering", Ledger: "dept-eng", Employees: 15},
		{Name: "sales", Ledger: "dept-sales", Employees: 10},
		{Name: "operations", Ledger: "dept-ops", Employees: 5},
	}
}

// MultiLedgerPayrollSetupActions returns all setup requests for the multi-ledger payroll scenario:
// clearing ledger + department ledgers, account types, and numscripts.
func MultiLedgerPayrollSetupActions() []*servicepb.Request {
	departments := MultiLedgerPayrollDepartments()

	reqs := []*servicepb.Request{
		actions.CreateLedgerAction("clearing", nil),
		actions.AddAccountTypeAction("clearing", "company", "company:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction("clearing", "dept", "dept:{name}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
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
	}

	for _, dept := range departments {
		reqs = append(reqs,
			actions.CreateLedgerAction(dept.Ledger, nil),
			actions.AddAccountTypeAction(dept.Ledger, "payroll", "payroll:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			actions.AddAccountTypeAction(dept.Ledger, "employee", "employee:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			actions.AddAccountTypeAction(dept.Ledger, "expense", "expense:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			actions.SaveNumscriptWithVersionAction(dept.Ledger, "fund_payroll", `vars {
  monetary $amount
}
send $amount (
  source = @world
  destination = @payroll:pool
)`, "1.0.0"),
			actions.SaveNumscriptWithVersionAction(dept.Ledger, "pay_salary", `vars {
  account $employee
  monetary $amount
}
send $amount (
  source = @payroll:pool
  destination = $employee
)`, "1.0.0"),
		)
	}

	return reqs
}

// RunMultiLedgerPayroll provisions a company payroll scenario across 4 ledgers:
// a central clearing ledger and 3 department ledgers (engineering, sales, operations).
// Runs 3 monthly cycles, engineering bonuses, and inter-department cost allocations.
func RunMultiLedgerPayroll(r *Runner) error {
	const (
		numMonths    = 3
		baseSalary   = 500_000
		bonusPercent = 10
	)

	departments := MultiLedgerPayrollDepartments()

	// --- Setup ---
	if _, err := r.Step("Setup", MultiLedgerPayrollSetupActions()...); err != nil {
		return err
	}

	// --- Monthly Payroll Cycles ---
	for month := 1; month <= numMonths; month++ {
		// Step 1: Fund clearing ledger
		totalNeeded := int64(0)
		for _, dept := range departments {
			totalNeeded += int64(dept.Employees) * baseSalary
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
			amount := int64(dept.Employees) * baseSalary
			deptReqs = append(deptReqs,
				actions.CreateScriptRefTransactionAction("clearing", "fund_dept", "1.0.0", map[string]string{
					"dept_account": "dept:" + dept.Name,
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
			amount := int64(dept.Employees) * baseSalary
			payrollReqs = append(payrollReqs,
				actions.CreateScriptRefTransactionAction(dept.Ledger, "fund_payroll", "1.0.0", map[string]string{
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
			for emp := 1; emp <= dept.Employees; emp++ {
				salaryReqs = append(salaryReqs,
					actions.CreateScriptRefTransactionAction(dept.Ledger, "pay_salary", "1.0.0", map[string]string{
						"employee": fmt.Sprintf("employee:%d", emp),
						"amount":   fmt.Sprintf("USD/2 %d", baseSalary),
					}, map[string]string{
						"month": strconv.Itoa(month),
						"type":  "salary",
					}),
				)
			}
			if _, err := r.Step(fmt.Sprintf("Month%d/PayEmployees/%s", month, dept.Name), salaryReqs...); err != nil {
				return err
			}
		}
	}

	// --- Bonuses (engineering only) ---
	{
		dept := departments[0]
		bonusAmount := int64(baseSalary * bonusPercent / 100)
		totalBonus := bonusAmount * int64(dept.Employees)

		if _, err := r.Step("Bonuses/FundPool",
			actions.CreateScriptRefTransactionAction(dept.Ledger, "fund_payroll", "1.0.0", map[string]string{
				"amount": fmt.Sprintf("USD/2 %d", totalBonus),
			}, map[string]string{"type": "bonus-funding"}),
		); err != nil {
			return err
		}

		var bonusReqs []*servicepb.Request
		for emp := 1; emp <= dept.Employees; emp++ {
			bonusReqs = append(bonusReqs,
				actions.CreateScriptRefTransactionAction(dept.Ledger, "pay_salary", "1.0.0", map[string]string{
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
