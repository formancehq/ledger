package scenario

import (
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
)

// OperationsLifecycleLedger is the ledger name used by the operations lifecycle scenario.
const OperationsLifecycleLedger = "ops-test"

// OperationsLifecycleSetupActions returns the Apply requests that create the ledger,
// account types, and numscript library for the operations lifecycle scenario.
func OperationsLifecycleSetupActions() []*servicepb.Request {
	return []*servicepb.Request{
		actions.CreateLedgerAction(OperationsLifecycleLedger, nil),
		actions.AddAccountTypeAction(OperationsLifecycleLedger, "ops-account", "ops:{id}"),
		actions.SaveNumscriptWithVersionAction(OperationsLifecycleLedger, "deposit", `vars {
  account $account
  monetary $amount
}
send $amount (
  source = @world
  destination = $account
)`, "1.0.0"),
	}
}
