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

func init() { Register("multi-ledger-payroll", RunMultiLedgerPayroll) }

const MultiLedgerPayrollBaseSalary = 500_000

// MultiLedgerPayrollBlocks returns the atomic blocks for the multi-ledger payroll scenario.
func MultiLedgerPayrollBlocks() *BlockGroup {
	return &BlockGroup{
		Setup: MultiLedgerPayrollSetupActions,
		Blocks: []*Block{
			{Name: "payroll/fund_clearing", Run: payrollFundClearing},
			{Name: "payroll/distribute", Run: payrollDistribute},
			{Name: "payroll/pay_salary", Run: payrollPaySalary},
			{Name: "payroll/cost_alloc", Run: payrollCostAlloc},
		},
	}
}

func payrollFundClearing(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	departments := MultiLedgerPayrollDepartments()
	var totalNeeded int64
	for _, dept := range departments {
		totalNeeded += int64(dept.Employees) * MultiLedgerPayrollBaseSalary
	}

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction("clearing", "fund_clearing", "1.0.0", map[string]string{
			"amount": fmt.Sprintf("USD/2 %d", totalNeeded),
		}, nil),
	)
}

func payrollDistribute(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	departments := MultiLedgerPayrollDepartments()
	dept := departments[RandIntN(r, len(departments))]
	amount := int64(dept.Employees) * MultiLedgerPayrollBaseSalary

	bal, ok := GetAccountBalance(ctx, client, "clearing", "company:treasury", "USD/2")
	if !ok || bal.Cmp(big.NewInt(amount)) < 0 {
		return nil, ErrSkip
	}

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction("clearing", "fund_dept", "1.0.0", map[string]string{
			"dept_account": "dept:" + dept.Name,
			"amount":       fmt.Sprintf("USD/2 %d", amount),
		}, nil),
	)
}

func payrollPaySalary(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	departments := MultiLedgerPayrollDepartments()
	dept := departments[RandIntN(r, len(departments))]
	empID := 1 + RandIntN(r, dept.Employees)

	// Fund pool and pay in the same batch so payroll:pool (transient) nets to zero.
	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(dept.Ledger, "fund_payroll", "1.0.0", map[string]string{
			"amount": fmt.Sprintf("USD/2 %d", MultiLedgerPayrollBaseSalary),
		}, map[string]string{"type": "salary-funding"}),
		actions.CreateScriptRefTransactionAction(dept.Ledger, "pay_salary", "1.0.0", map[string]string{
			"employee": fmt.Sprintf("employee:%d", empID),
			"amount":   fmt.Sprintf("USD/2 %d", MultiLedgerPayrollBaseSalary),
		}, map[string]string{"type": "salary"}),
	)
}

func payrollCostAlloc(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	type allocation struct {
		from   string
		to     string
		amount int64
		reason string
	}
	allocations := []allocation{
		{"dept:operations", "dept:engineering", 50_000, "cloud-hosting"},
		{"dept:engineering", "dept:sales", 30_000, "lead-generation"},
		{"dept:sales", "dept:operations", 20_000, "office-supplies"},
	}

	alloc := allocations[RandIntN(r, len(allocations))]

	bal, ok := GetAccountBalance(ctx, client, "clearing", alloc.from, "USD/2")
	if !ok || bal.Cmp(big.NewInt(alloc.amount)) < 0 {
		return nil, ErrSkip
	}

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction("clearing", "cost_allocation", "1.0.0", map[string]string{
			"from_dept": alloc.from,
			"to_dept":   alloc.to,
			"amount":    fmt.Sprintf("USD/2 %d", alloc.amount),
		}, map[string]string{"reason": alloc.reason}),
	)
}

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
		actions.AddAccountTypeAction("clearing", "company", "company:{type}"),
		actions.AddAccountTypeAction("clearing", "dept", "dept:{name}"),
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
			actions.AddAccountTypeWithPersistenceAction(dept.Ledger, "payroll", "payroll:{type}", commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT),
			actions.AddAccountTypeAction(dept.Ledger, "employee", "employee:{id}"),
			actions.AddAccountTypeAction(dept.Ledger, "expense", "expense:{type}"),
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
	numMonths := r.Iterations(3)

	const (
		baseSalary   = 500_000
		bonusPercent = 10
	)

	departments := MultiLedgerPayrollDepartments()

	// --- Setup ---
	if err := r.Setup(MultiLedgerPayrollSetupActions()...); err != nil {
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

		// Step 3: Fund payroll pools and pay employees (same batch so payroll:pool nets to zero).
		for _, dept := range departments {
			amount := int64(dept.Employees) * baseSalary
			var payrollReqs []*servicepb.Request
			payrollReqs = append(payrollReqs,
				actions.CreateScriptRefTransactionAction(dept.Ledger, "fund_payroll", "1.0.0", map[string]string{
					"amount": fmt.Sprintf("USD/2 %d", amount),
				}, map[string]string{"month": strconv.Itoa(month)}),
			)
			for emp := 1; emp <= dept.Employees; emp++ {
				payrollReqs = append(payrollReqs,
					actions.CreateScriptRefTransactionAction(dept.Ledger, "pay_salary", "1.0.0", map[string]string{
						"employee": fmt.Sprintf("employee:%d", emp),
						"amount":   fmt.Sprintf("USD/2 %d", baseSalary),
					}, map[string]string{
						"month": strconv.Itoa(month),
						"type":  "salary",
					}),
				)
			}
			if _, err := r.Step(fmt.Sprintf("Month%d/Payroll/%s", month, dept.Name), payrollReqs...); err != nil {
				return err
			}
		}
	}

	// --- Bonuses (engineering only) ---
	{
		dept := departments[0]
		bonusAmount := int64(baseSalary * bonusPercent / 100)
		totalBonus := bonusAmount * int64(dept.Employees)

		bonusReqs := []*servicepb.Request{
			actions.CreateScriptRefTransactionAction(dept.Ledger, "fund_payroll", "1.0.0", map[string]string{
				"amount": fmt.Sprintf("USD/2 %d", totalBonus),
			}, map[string]string{"type": "bonus-funding"}),
		}
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
