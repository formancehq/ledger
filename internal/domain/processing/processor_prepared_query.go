package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processCreatePreparedQuery(order *raftcmdpb.CreatePreparedQueryOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	existing, err := s.GetPreparedQuery(order.GetQuery().GetLedger(), order.GetQuery().GetName())
	if err != nil {
		return nil, err
	}

	if existing != nil {
		return nil, &domain.ErrPreparedQueryAlreadyExists{
			Ledger: order.GetQuery().GetLedger(),
			Name:   order.GetQuery().GetName(),
		}
	}

	s.PutPreparedQuery(order.GetQuery())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreatedPreparedQuery{
			CreatedPreparedQuery: &commonpb.CreatedPreparedQueryLog{
				Query: order.GetQuery(),
			},
		},
	}, nil
}

func (p *RequestProcessor) processUpdatePreparedQuery(order *raftcmdpb.UpdatePreparedQueryOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	existing, err := s.GetPreparedQuery(order.GetLedger(), order.GetName())
	if err != nil {
		return nil, err
	}

	if existing == nil {
		return nil, &domain.ErrPreparedQueryNotFound{
			Ledger: order.GetLedger(),
			Name:   order.GetName(),
		}
	}

	previousFilter := existing.GetFilter()
	updated := existing.CloneVT()
	updated.Filter = order.GetFilter()
	s.PutPreparedQuery(updated)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_UpdatedPreparedQuery{
			UpdatedPreparedQuery: &commonpb.UpdatedPreparedQueryLog{
				Ledger:         order.GetLedger(),
				Name:           order.GetName(),
				PreviousFilter: previousFilter,
				NewFilter:      order.GetFilter(),
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeletePreparedQuery(order *raftcmdpb.DeletePreparedQueryOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	existing, err := s.GetPreparedQuery(order.GetLedger(), order.GetName())
	if err != nil {
		return nil, err
	}

	if existing == nil {
		return nil, &domain.ErrPreparedQueryNotFound{
			Ledger: order.GetLedger(),
			Name:   order.GetName(),
		}
	}

	s.DeletePreparedQuery(order.GetLedger(), order.GetName())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletedPreparedQuery{
			DeletedPreparedQuery: &commonpb.DeletedPreparedQueryLog{
				Ledger: order.GetLedger(),
				Name:   order.GetName(),
			},
		},
	}, nil
}
