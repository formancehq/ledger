package processing

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processAddEventsSink(order *raftcmdpb.AddEventsSinkOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
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
		return nil, fmt.Errorf("checking existing sink %q: %w", cfg.GetName(), err)
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

func (p *RequestProcessor) processRemoveEventsSink(order *raftcmdpb.RemoveEventsSinkOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	existing, err := s.GetSinkConfig(order.GetName())
	if err != nil {
		return nil, fmt.Errorf("checking existing sink %q: %w", order.GetName(), err)
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
