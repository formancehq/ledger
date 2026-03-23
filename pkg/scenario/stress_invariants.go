package scenario

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
)

// StressInvariantsLedger is the ledger name used by the stress invariants scenario.
const StressInvariantsLedger = "stress"

// StressInvariantsSetupActions returns the Apply requests that create the ledger,
// account types, and numscript library for the stress invariants scenario.
func StressInvariantsSetupActions() []*servicepb.Request {
	return []*servicepb.Request{
		actions.CreateLedgerAction(StressInvariantsLedger, nil),
		actions.AddAccountTypeAction(StressInvariantsLedger, "trader", "trader:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(StressInvariantsLedger, "exchange-fees", "exchange:fees", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(StressInvariantsLedger, "exchange-withdrawals", "exchange:withdrawals", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.SaveNumscriptWithVersionAction(StressInvariantsLedger, "deposit", `vars {
  account $account
  monetary $amount
}
send $amount (
  source = @world
  destination = $account
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(StressInvariantsLedger, "trade", `vars {
  account $buyer
  account $seller
  monetary $amount
}
send $amount (
  source = $buyer
  destination = {
    1/100 to @exchange:fees
    remaining to $seller
  }
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(StressInvariantsLedger, "withdraw", `vars {
  account $account
  monetary $amount
}
send $amount (
  source = $account
  destination = @exchange:withdrawals
)`, "1.0.0"),
	}
}
