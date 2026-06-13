package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processAddEventsSink(order *raftcmdpb.AddEventsSinkOrder, s InMemoryStore) (*commonpb.LogPayload, domain.Describable) {
	cfg := order.GetConfig()

	if cfg.GetBatchSize() > domain.MaxSinkBatchSize {
		return nil, &domain.ErrSinkBatchSizeTooLarge{
			Name:      cfg.GetName(),
			BatchSize: cfg.GetBatchSize(),
			Max:       domain.MaxSinkBatchSize,
		}
	}

	existing, err := s.GetSinkConfig(cfg.GetName())
	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "checking existing sink " + cfg.GetName(), Cause: err}
	}

	if existing != nil {
		return nil, &domain.ErrSinkAlreadyExists{Name: cfg.GetName()}
	}

	s.AddSinkConfig(cfg)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_AddedEventsSink{
			AddedEventsSink: &commonpb.AddedEventsSinkLog{
				Config: cfg,
			},
		},
	}, nil
}

func (p *RequestProcessor) processRemoveEventsSink(order *raftcmdpb.RemoveEventsSinkOrder, s InMemoryStore) (*commonpb.LogPayload, domain.Describable) {
	existing, err := s.GetSinkConfig(order.GetName())
	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "checking existing sink " + order.GetName(), Cause: err}
	}

	if existing == nil {
		return nil, &domain.ErrSinkNotFound{Name: order.GetName()}
	}

	s.RemoveSinkConfig(order.GetName())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_RemovedEventsSink{
			RemovedEventsSink: &commonpb.RemovedEventsSinkLog{
				Name: order.GetName(),
			},
		},
	}, nil
}
