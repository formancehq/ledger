package payroll

import (
	"github.com/formancehq/ledger/v3/pkg/scenario"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/block"
)

func init() {
	block.Register(scenario.MultiLedgerPayrollBlocks())
}
