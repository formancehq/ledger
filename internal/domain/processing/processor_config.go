package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processSetMaintenanceMode(order *raftcmdpb.SetMaintenanceModeOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	s.SetMaintenanceMode(order.GetEnabled())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetMaintenanceMode{
			SetMaintenanceMode: &commonpb.SetMaintenanceModeLog{
				Enabled: order.GetEnabled(),
			},
		},
	}, nil
}
