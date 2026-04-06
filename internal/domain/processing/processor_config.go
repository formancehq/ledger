package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processSetAuditConfig(order *raftcmdpb.SetAuditConfigOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	s.SetAuditEnabled(order.GetEnabled())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetAuditConfig{
			SetAuditConfig: &commonpb.SetAuditConfigLog{
				Enabled: order.GetEnabled(),
			},
		},
	}, nil
}

func (p *RequestProcessor) processSetMaintenanceMode(order *raftcmdpb.SetMaintenanceModeOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	s.SetMaintenanceMode(order.GetEnabled())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetMaintenanceMode{
			SetMaintenanceMode: &commonpb.SetMaintenanceModeLog{
				Enabled: order.GetEnabled(),
			},
		},
	}, nil
}
