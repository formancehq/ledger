package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processCreatePreparedQuery(order *raftcmdpb.CreatePreparedQueryOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	existing, err := s.GetPreparedQuery(order.Query.Ledger, order.Query.Name)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, &domain.ErrPreparedQueryAlreadyExists{
			Ledger: order.Query.Ledger,
			Name:   order.Query.Name,
		}
	}

	s.PutPreparedQuery(order.Query)
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreatedPreparedQuery{
			CreatedPreparedQuery: &commonpb.CreatedPreparedQueryLog{
				Query: order.Query,
			},
		},
	}, nil
}

func (p *RequestProcessor) processUpdatePreparedQuery(order *raftcmdpb.UpdatePreparedQueryOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	existing, err := s.GetPreparedQuery(order.Ledger, order.Name)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, &domain.ErrPreparedQueryNotFound{
			Ledger: order.Ledger,
			Name:   order.Name,
		}
	}

	previousFilter := existing.Filter
	updated := existing.CloneVT()
	updated.Filter = order.Filter
	s.PutPreparedQuery(updated)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_UpdatedPreparedQuery{
			UpdatedPreparedQuery: &commonpb.UpdatedPreparedQueryLog{
				Ledger:         order.Ledger,
				Name:           order.Name,
				PreviousFilter: previousFilter,
				NewFilter:      order.Filter,
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeletePreparedQuery(order *raftcmdpb.DeletePreparedQueryOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	existing, err := s.GetPreparedQuery(order.Ledger, order.Name)
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, &domain.ErrPreparedQueryNotFound{
			Ledger: order.Ledger,
			Name:   order.Name,
		}
	}

	s.DeletePreparedQuery(order.Ledger, order.Name)
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletedPreparedQuery{
			DeletedPreparedQuery: &commonpb.DeletedPreparedQueryLog{
				Ledger: order.Ledger,
				Name:   order.Name,
			},
		},
	}, nil
}
