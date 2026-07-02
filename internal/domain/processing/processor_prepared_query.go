package processing

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// lookupPreparedQuery centralises the (nil, ErrNotFound) vs (nil, otherErr)
// discrimination on the Accessor contract: a tombstone / absent-key returns
// (nil, nil) so callers see "doesn't exist"; any other error wraps into
// ErrStorageOperation. Mirrors the loadLedger pattern.
func lookupPreparedQuery(s Scope, ledger, name string) (commonpb.PreparedQueryReader, domain.Describable) {
	pq, err := s.PreparedQueries().Get(domain.PreparedQueryKey{LedgerName: ledger, Name: name})
	if errors.Is(err, domain.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "getting prepared query", Cause: err}
	}

	return pq, nil
}

func processCreatePreparedQuery(ledger string, order *raftcmdpb.CreatePreparedQueryOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope
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

	if _, loadErr := loadLedger(s, ledger); loadErr != nil {
		return nil, loadErr
	}

	existing, lookupErr := lookupPreparedQuery(s, ledger, q.GetName())
	if lookupErr != nil {
		return nil, lookupErr
	}

	if existing != nil {
		return nil, &domain.ErrPreparedQueryAlreadyExists{
			Ledger: ledger,
			Name:   q.GetName(),
		}
	}

	s.PreparedQueries().Put(domain.PreparedQueryKey{LedgerName: ledger, Name: q.GetName()}, q)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreatedPreparedQuery{
			CreatedPreparedQuery: &commonpb.CreatedPreparedQueryLog{
				Ledger: ledger,
				Query:  q,
			},
		},
	}, nil
}

func processUpdatePreparedQuery(ledger string, order *raftcmdpb.UpdatePreparedQueryOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope
	if err := domain.ValidatePreparedQueryName(order.GetName()); err != nil {
		return nil, err
	}

	if _, loadErr := loadLedger(s, ledger); loadErr != nil {
		return nil, loadErr
	}

	existing, lookupErr := lookupPreparedQuery(s, ledger, order.GetName())
	if lookupErr != nil {
		return nil, lookupErr
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
	s.PreparedQueries().Put(domain.PreparedQueryKey{LedgerName: ledger, Name: updated.GetName()}, updated)

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

func processDeletePreparedQuery(ledger string, order *raftcmdpb.DeletePreparedQueryOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope
	if err := domain.ValidatePreparedQueryName(order.GetName()); err != nil {
		return nil, err
	}

	if _, loadErr := loadLedger(s, ledger); loadErr != nil {
		return nil, loadErr
	}

	existing, lookupErr := lookupPreparedQuery(s, ledger, order.GetName())
	if lookupErr != nil {
		return nil, lookupErr
	}

	if existing == nil {
		return nil, &domain.ErrPreparedQueryNotFound{
			Ledger: ledger,
			Name:   order.GetName(),
		}
	}

	if err := s.PreparedQueries().Delete(domain.PreparedQueryKey{LedgerName: ledger, Name: order.GetName()}); err != nil {
		return nil, &domain.ErrStorageOperation{Operation: "deleting prepared query", Cause: err}
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletedPreparedQuery{
			DeletedPreparedQuery: &commonpb.DeletedPreparedQueryLog{
				Ledger: ledger,
				Name:   order.GetName(),
			},
		},
	}, nil
}
