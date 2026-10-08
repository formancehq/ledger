package main

import (
	"github.com/antithesishq/antithesis-sdk-go/random"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// generateEnforcementMode switches the persisted ledger default through both
// supported request forms. Subsequent orders, including those in this bulk,
// observe the selected mode; there is no transaction-local mode override.
func generateEnforcementMode(ledger string) *ledgerpb.Request {
	mode := random.RandomChoice([]ledgerpb.ChartEnforcementMode{
		ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT,
		ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT,
	})

	return enforcementModeRequest(ledger, mode, random.RandomChoice([]bool{false, true}))
}

func enforcementModeRequest(ledger string, mode ledgerpb.ChartEnforcementMode, nested bool) *ledgerpb.Request {
	if nested {
		return &ledgerpb.Request{Type: &ledgerpb.Request_Apply{Apply: &ledgerpb.LedgerApplyRequest{Ledger: ledger, Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &ledgerpb.SetDefaultEnforcementModeRequest{EnforcementMode: mode}}}}}}
	}

	return &ledgerpb.Request{Type: &ledgerpb.Request_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &ledgerpb.SetDefaultEnforcementModeLedgerRequest{Ledger: ledger, EnforcementMode: mode}}}
}
