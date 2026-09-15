package main

import (
	"github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// generateEnforcementMode switches the persisted ledger default through both
// supported request forms. Subsequent orders, including those in this bulk,
// observe the selected mode; there is no transaction-local mode override.
func generateEnforcementMode(ledger string) *servicepb.Request {
	mode := random.RandomChoice([]commonpb.ChartEnforcementMode{
		commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
		commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
	})
	return enforcementModeRequest(ledger, mode, random.RandomChoice([]bool{false, true}))
}

func enforcementModeRequest(ledger string, mode commonpb.ChartEnforcementMode, nested bool) *servicepb.Request {
	if nested {
		return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: ledger, Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeRequest{EnforcementMode: mode}}}}}}
	}
	return &servicepb.Request{Type: &servicepb.Request_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{Ledger: ledger, EnforcementMode: mode}}}
}
