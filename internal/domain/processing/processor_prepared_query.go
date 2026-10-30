package processing

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// lookupPreparedQuery centralises the (nil, ErrNotFound) vs (nil, otherErr)
// discrimination on the Accessor contract: a tombstone / absent-key returns
// (nil, nil) so callers see "doesn't exist"; an admission-contract violation
// (notably *state.ErrCoverageMiss) propagates verbatim so the FSM rejects it
// without auditing a storage fault (EN-1379); any other
// error wraps into ErrStorageOperation. Mirrors the loadLedger pattern.
func lookupPreparedQuery(s Scope, ledger, name string) (commonpb.PreparedQueryReader, domain.SerializableError) {
	pq, err := s.PreparedQueries().Get(domain.PreparedQueryKey{LedgerName: ledger, Name: name})
	if errors.Is(err, domain.ErrNotFound) {
		return nil, nil
	}

	if err != nil {
		return nil, domain.StoreFailure("getting prepared query", err)
	}

	return pq, nil
}

func processCreatePreparedQuery(ledger string, order *raftcmdpb.CreatePreparedQueryOrder, ctx *Context) (*commonpb.LogPayload, domain.SerializableError) {
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

	// Mirror the admission gates on the FSM side (per the dual-layer rule
	// above): a non-executable target or a condition invalid on the target must
	// never be persisted, so a stored prepared query is always executable and
	// the audit trail stays deterministic on wire-replay.
	if !domain.IsPreparedQueryExecutableTarget(q.GetTarget()) {
		return nil, domain.ErrPreparedQueryTargetUnsupported
	}

	if err := domain.ValidateFilterForTarget(q.GetFilter(), q.GetTarget()); err != nil {
		return nil, err
	}

	if _, loadErr := loadLiveLedgerReader(s, ledger); loadErr != nil {
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

func processUpdatePreparedQuery(ledger string, order *raftcmdpb.UpdatePreparedQueryOrder, ctx *Context) (*commonpb.LogPayload, domain.SerializableError) {
	s := ctx.Scope
	if err := domain.ValidatePreparedQueryName(order.GetName()); err != nil {
		return nil, err
	}

	if _, loadErr := loadLiveLedgerReader(s, ledger); loadErr != nil {
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
	previousTarget := updated.GetTarget()
	target := previousTarget
	if order.Target != nil {
		target = order.GetTarget()
	}
	if !domain.IsPreparedQueryExecutableTarget(target) {
		return nil, domain.ErrPreparedQueryTargetUnsupported
	}

	// The target is an explicit order input when it changes. Historical orders
	// omit it and continue to validate against the cached stored target.
	if err := domain.ValidateFilterForTarget(order.GetFilter(), target); err != nil {
		return nil, err
	}

	previousFilter := updated.GetFilter().CloneVT()
	updated.Filter = order.GetFilter()
	updated.Target = target
	s.PreparedQueries().Put(domain.PreparedQueryKey{LedgerName: ledger, Name: updated.GetName()}, updated)
	log := &commonpb.UpdatedPreparedQueryLog{
		Ledger:         ledger,
		Name:           order.GetName(),
		PreviousFilter: previousFilter,
		NewFilter:      order.GetFilter(),
	}
	if order.Target != nil {
		log.PreviousTarget = previousTarget.Enum()
		log.NewTarget = target.Enum()
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_UpdatedPreparedQuery{
			UpdatedPreparedQuery: log,
		},
	}, nil
}

func processDeletePreparedQuery(ledger string, order *raftcmdpb.DeletePreparedQueryOrder, ctx *Context) (*commonpb.LogPayload, domain.SerializableError) {
	s := ctx.Scope
	if err := domain.ValidatePreparedQueryName(order.GetName()); err != nil {
		return nil, err
	}

	if _, loadErr := loadLiveLedgerReader(s, ledger); loadErr != nil {
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
		return nil, domain.StoreFailure("deleting prepared query", err)
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
