package processing

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processAddEventsSink(order *raftcmdpb.AddEventsSinkOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	existing, err := s.GetSinkConfig(order.GetConfig().GetName())
	if err != nil {
		return nil, fmt.Errorf("checking existing sink %q: %w", order.GetConfig().GetName(), err)
	}

	if existing != nil {
		return nil, &domain.ErrSinkAlreadyExists{Name: order.GetConfig().GetName()}
	}

	s.AddSinkConfig(order.GetConfig())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_AddedEventsSink{
			AddedEventsSink: &commonpb.AddedEventsSinkLog{
				Config: order.GetConfig(),
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
