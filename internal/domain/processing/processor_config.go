package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func processSetMaintenanceMode(order *raftcmdpb.SetMaintenanceModeOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	ctx.Scope.SetMaintenanceMode(order.GetEnabled())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SetMaintenanceMode{
			SetMaintenanceMode: &commonpb.SetMaintenanceModeLog{
				Enabled: order.GetEnabled(),
			},
		},
	}, nil
}
