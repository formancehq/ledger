package processing

import (
	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func processSetMaintenanceMode(order *raftcmdpb.SetMaintenanceModeOrder, ctx *Context) (*ledgerpb.LogPayload, domain.SerializableError) {
	ctx.Scope.SetMaintenanceMode(order.GetEnabled())

	return &ledgerpb.LogPayload{
		Type: &ledgerpb.LogPayload_SetMaintenanceMode{
			SetMaintenanceMode: &ledgerpb.SetMaintenanceModeLog{
				Enabled: order.GetEnabled(),
			},
		},
	}, nil
}
