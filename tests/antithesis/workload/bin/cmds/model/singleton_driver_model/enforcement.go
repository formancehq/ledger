package main

import (
	"github.com/antithesishq/antithesis-sdk-go/random"
	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// generateEnforcementMode switches the persisted ledger default through both
// supported request forms. Subsequent orders, including those in this bulk,
// observe the selected mode; there is no transaction-local mode override.
func generateEnforcementMode(ledger string) *commonpb.Request {
	mode := random.RandomChoice([]commonpb.ChartEnforcementMode{
		commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
		commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
	})

	return enforcementModeRequest(ledger, mode, random.RandomChoice([]bool{false, true}))
}

func enforcementModeRequest(ledger string, mode commonpb.ChartEnforcementMode, nested bool) *commonpb.Request {
	if nested {
		return &commonpb.Request{Type: &commonpb.Request_Apply{Apply: &commonpb.LedgerApplyRequest{Ledger: ledger, Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &commonpb.SetDefaultEnforcementModeRequest{EnforcementMode: mode}}}}}}
	}
	return &commonpb.Request{Type: &commonpb.Request_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &commonpb.SetDefaultEnforcementModeLedgerRequest{Ledger: ledger, EnforcementMode: mode}}}
}
