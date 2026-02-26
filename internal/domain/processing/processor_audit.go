package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processSetAuditConfig(order *raftcmdpb.SetAuditConfigOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	s.SetAuditEnabled(order.Enabled)
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetAuditConfig{
			SetAuditConfig: &commonpb.SetAuditConfigLog{
				Enabled: order.Enabled,
			},
		},
	}, nil
}
