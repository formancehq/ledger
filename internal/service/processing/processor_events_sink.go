package processing

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processAddEventsSink(order *raftcmdpb.AddEventsSinkOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	existing, err := s.GetSinkConfig(order.Config.Name)
	if err != nil {
		return nil, fmt.Errorf("checking existing sink %q: %w", order.Config.Name, err)
	}
	if existing != nil {
		return nil, &ErrSinkAlreadyExists{Name: order.Config.Name}
	}

	s.AddSinkConfig(order.Config)
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_AddedEventsSink{
			AddedEventsSink: &commonpb.AddedEventsSinkLog{
				Config: order.Config,
			},
		},
	}, nil
}

func (p *RequestProcessor) processRemoveEventsSink(order *raftcmdpb.RemoveEventsSinkOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	existing, err := s.GetSinkConfig(order.Name)
	if err != nil {
		return nil, fmt.Errorf("checking existing sink %q: %w", order.Name, err)
	}
	if existing == nil {
		return nil, &ErrSinkNotFound{Name: order.Name}
	}

	s.RemoveSinkConfig(order.Name)
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_RemovedEventsSink{
			RemovedEventsSink: &commonpb.RemovedEventsSinkLog{
				Name: order.Name,
			},
		},
	}, nil
}
