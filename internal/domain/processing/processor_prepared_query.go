package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processCreatePreparedQuery(ledger string, order *raftcmdpb.CreatePreparedQueryOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	// Validate payload BEFORE loading the ledger. After moving `ledger` off
	// `PreparedQuery` onto the surrounding request (PR #522), a malformed
	// request with a valid top-level ledger but a nil/empty `query` would
	// otherwise reach PutPreparedQuery with an empty name and persist a
	// nameless entry. Validate at both layers: admission rejects for UX,
	// FSM rejects for audit trail / wire-replay determinism.
	q := order.GetQuery()
	if q == nil {
		return nil, domain.ErrPreparedQueryRequired
	}

	if err := domain.ValidatePreparedQueryName(q.GetName()); err != nil {
		return nil, err
	}

	info, loadErr := loadLedger(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	ledgerName := info.GetName()

	existing, err := s.GetPreparedQuery(ledgerName, q.GetName())
	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "getting prepared query", Cause: err}
	}

	if existing != nil {
		return nil, &domain.ErrPreparedQueryAlreadyExists{
			Ledger: ledger,
			Name:   q.GetName(),
		}
	}

	s.PutPreparedQuery(ledgerName, q)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreatedPreparedQuery{
			CreatedPreparedQuery: &commonpb.CreatedPreparedQueryLog{
				Ledger: ledger,
				Query:  q,
			},
		},
	}, nil
}

func (p *RequestProcessor) processUpdatePreparedQuery(ledger string, order *raftcmdpb.UpdatePreparedQueryOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	if err := domain.ValidatePreparedQueryName(order.GetName()); err != nil {
		return nil, err
	}

	info, loadErr := loadLedger(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	ledgerName := info.GetName()

	existing, err := s.GetPreparedQuery(ledgerName, order.GetName())
	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "getting prepared query", Cause: err}
	}

	if existing == nil {
		return nil, &domain.ErrPreparedQueryNotFound{
			Ledger: ledger,
			Name:   order.GetName(),
		}
	}

	updated := existing.Mutate()
	previousFilter := updated.GetFilter().CloneVT()
	updated.Filter = order.GetFilter()
	s.PutPreparedQuery(ledgerName, updated)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_UpdatedPreparedQuery{
			UpdatedPreparedQuery: &commonpb.UpdatedPreparedQueryLog{
				Ledger:         ledger,
				Name:           order.GetName(),
				PreviousFilter: previousFilter,
				NewFilter:      order.GetFilter(),
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeletePreparedQuery(ledger string, order *raftcmdpb.DeletePreparedQueryOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	if err := domain.ValidatePreparedQueryName(order.GetName()); err != nil {
		return nil, err
	}

	info, loadErr := loadLedger(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	ledgerName := info.GetName()

	existing, err := s.GetPreparedQuery(ledgerName, order.GetName())
	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "getting prepared query", Cause: err}
	}

	if existing == nil {
		return nil, &domain.ErrPreparedQueryNotFound{
			Ledger: ledger,
			Name:   order.GetName(),
		}
	}

	s.DeletePreparedQuery(ledgerName, order.GetName())

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletedPreparedQuery{
			DeletedPreparedQuery: &commonpb.DeletedPreparedQueryLog{
				Ledger: ledger,
				Name:   order.GetName(),
			},
		},
	}, nil
}
