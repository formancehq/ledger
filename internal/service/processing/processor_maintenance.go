package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processSetMaintenanceMode(order *raftcmdpb.SetMaintenanceModeOrder, s Store) (*commonpb.LogPayload, error) {
	s.SetMaintenanceMode(order.Enabled)
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetMaintenanceMode{
			SetMaintenanceMode: &commonpb.SetMaintenanceModeLog{
				Enabled: order.Enabled,
			},
		},
	}, nil
}
