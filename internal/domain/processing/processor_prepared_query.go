package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processCreatePreparedQuery(order *raftcmdpb.CreatePreparedQueryOrder, s InMemoryStore) (*commonpb.LogPayload, domain.Describable) {
	info, ok := s.GetLedger(order.GetQuery().GetLedger())
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: order.GetQuery().GetLedger()}
	}

	ledgerName := info.GetName()

	existing, err := s.GetPreparedQuery(ledgerName, order.GetQuery().GetName())
	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "getting prepared query", Cause: err}
	}

	if existing != nil {
		return nil, &domain.ErrPreparedQueryAlreadyExists{
			Ledger: order.GetQuery().GetLedger(),
			Name:   order.GetQuery().GetName(),
		}
	}

	s.PutPreparedQuery(ledgerName, order.GetQuery())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreatedPreparedQuery{
			CreatedPreparedQuery: &commonpb.CreatedPreparedQueryLog{
				Query: order.GetQuery(),
			},
		},
	}, nil
}

func (p *RequestProcessor) processUpdatePreparedQuery(order *raftcmdpb.UpdatePreparedQueryOrder, s InMemoryStore) (*commonpb.LogPayload, domain.Describable) {
	info, ok := s.GetLedger(order.GetLedger())
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: order.GetLedger()}
	}

	ledgerName := info.GetName()

	existing, err := s.GetPreparedQuery(ledgerName, order.GetName())
	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "getting prepared query", Cause: err}
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
	s.PutPreparedQuery(ledgerName, updated)

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

func (p *RequestProcessor) processDeletePreparedQuery(order *raftcmdpb.DeletePreparedQueryOrder, s InMemoryStore) (*commonpb.LogPayload, domain.Describable) {
	info, ok := s.GetLedger(order.GetLedger())
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: order.GetLedger()}
	}

	ledgerName := info.GetName()

	existing, err := s.GetPreparedQuery(ledgerName, order.GetName())
	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "getting prepared query", Cause: err}
	}

	if existing == nil {
		return nil, &domain.ErrPreparedQueryNotFound{
			Ledger: order.GetLedger(),
			Name:   order.GetName(),
		}
	}

	s.DeletePreparedQuery(ledgerName, order.GetName())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletedPreparedQuery{
			DeletedPreparedQuery: &commonpb.DeletedPreparedQueryLog{
				Ledger: order.GetLedger(),
				Name:   order.GetName(),
			},
		},
	}, nil
}
