package processing

import (
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func processAddEventsSink(order *raftcmdpb.AddEventsSinkOrder, ctx *Context) (*ledgerpb.LogPayload, domain.SerializableError) {
	cfg := order.GetConfig()

	if cfg.GetBatchSize() > domain.MaxSinkBatchSize {
		return nil, &domain.ErrSinkBatchSizeTooLarge{
			Name:      cfg.GetName(),
			BatchSize: cfg.GetBatchSize(),
			Max:       domain.MaxSinkBatchSize,
		}
	}

	existing, err := ctx.Scope.GetSinkConfig(cfg.GetName())
	if err != nil {
		return nil, domain.StoreFailure("checking existing sink "+cfg.GetName(), err)
	}

	if existing != nil {
		return nil, &domain.ErrSinkAlreadyExists{Name: cfg.GetName()}
	}

	return &ledgerpb.LogPayload{
		Type: &ledgerpb.LogPayload_AddedEventsSink{
			AddedEventsSink: &ledgerpb.AddedEventsSinkLog{
				Config: cfg,
			},
		},
	}, nil
}

func processRemoveEventsSink(order *raftcmdpb.RemoveEventsSinkOrder, ctx *Context) (*ledgerpb.LogPayload, domain.SerializableError) {
	existing, err := ctx.Scope.GetSinkConfig(order.GetName())
	if err != nil {
		return nil, domain.StoreFailure("checking existing sink "+order.GetName(), err)
	}

	if existing == nil {
		return nil, &domain.ErrSinkNotFound{Name: order.GetName()}
	}
	if order.GetControllerId() != "" && existing.GetControllerId() != order.GetControllerId() {
		return nil, &domain.ErrSinkControllerMismatch{
			Name: order.GetName(), ControllerID: order.GetControllerId(),
		}
	}

	return &ledgerpb.LogPayload{
		Type: &ledgerpb.LogPayload_RemovedEventsSink{
			RemovedEventsSink: &ledgerpb.RemovedEventsSinkLog{
				Name: order.GetName(),
			},
		},
	}, nil
}
