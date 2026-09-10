package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/connectionconfig"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

var errInvalidSinkConnection = domain.NewValidationSentinel("invalid events sink connection configuration")

func processAddEventsSink(order *raftcmdpb.AddEventsSinkOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
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

	// Normalize into owned operational state without changing the accepted audit intent.
	normalized, err := connectionconfig.Sink(cfg)
	if err != nil {
		return nil, errInvalidSinkConnection
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_AddedEventsSink{
			AddedEventsSink: &commonpb.AddedEventsSinkLog{
				Config: normalized,
			},
		},
	}, nil
}

func processRemoveEventsSink(order *raftcmdpb.RemoveEventsSinkOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	existing, err := ctx.Scope.GetSinkConfig(order.GetName())
	if err != nil {
		return nil, domain.StoreFailure("checking existing sink "+order.GetName(), err)
	}

	if existing == nil {
		return nil, &domain.ErrSinkNotFound{Name: order.GetName()}
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_RemovedEventsSink{
			RemovedEventsSink: &commonpb.RemovedEventsSinkLog{
				Name: order.GetName(),
			},
		},
	}, nil
}
