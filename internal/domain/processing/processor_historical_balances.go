package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processConfigureHistoricalBalances records client intent in the audit-bound
// ledger log. The FSM deliberately persists no additional projection: each
// replica's secondary builder derives configuration and data from the audit.
func processConfigureHistoricalBalances(order *raftcmdpb.ConfigureHistoricalBalancesOrder) (*commonpb.LedgerLogPayload, domain.Describable) {
	if order == nil {
		return nil, &domain.ErrInvalidApplyType{TypeName: "nil ConfigureHistoricalBalancesOrder"}
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_ConfiguredHistoricalBalances{
			ConfiguredHistoricalBalances: &commonpb.ConfiguredHistoricalBalancesLog{Enabled: order.GetEnabled()},
		},
	}, nil
}
