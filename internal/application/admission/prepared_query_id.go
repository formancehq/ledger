package admission

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// resolvePreparedQueryIDOrder resolves public id before an update is committed.
// Every voter therefore receives the same target-specific condition, including
// during a rolling upgrade. The internal read uses the already-authorized write
// request and does not require a client query-read scope.
func (a *Admission) resolvePreparedQueryIDOrder(ctx context.Context, order *raftcmdpb.Order, overlay *bulkOverlay) error {
	ledgerOrder := order.GetLedgerScoped()
	if ledgerOrder == nil {
		return nil
	}
	ledger := ledgerOrder.GetLedger()
	switch payload := ledgerOrder.GetPayload().(type) {
	case *raftcmdpb.LedgerScopedOrder_CreatePreparedQuery:
		q := payload.CreatePreparedQuery.GetQuery()
		if q != nil {
			overlay.preparedTargets.Put(domain.PreparedQueryKey{LedgerName: ledger, Name: q.GetName()}, q.GetTarget())
		}
	case *raftcmdpb.LedgerScopedOrder_DeletePreparedQuery:
		overlay.preparedTargets.Delete(domain.PreparedQueryKey{LedgerName: ledger, Name: payload.DeletePreparedQuery.GetName()})
	case *raftcmdpb.LedgerScopedOrder_UpdatePreparedQuery:
		update := payload.UpdatePreparedQuery
		if !hasTransactionIDCondition(update.GetFilter()) {
			return nil
		}
		key := domain.PreparedQueryKey{LedgerName: ledger, Name: update.GetName()}
		if overlay.preparedTargets.IsDeleted(key) {
			return nil
		}
		target, ok := overlay.preparedTargets.Get(key)
		if !ok {
			handle, err := a.store.NewReadHandle()
			if err != nil {
				return fmt.Errorf("opening read handle for prepared query target: %w", err)
			}
			defer func() { _ = handle.Close() }()
			stored, err := query.ReadPreparedQuery(ctx, a.attrs.PreparedQuery, handle, ledger, update.GetName())
			if errors.Is(err, domain.ErrNotFound) {
				return nil // The FSM reports the missing query on apply.
			}
			if err != nil {
				return fmt.Errorf("reading prepared query target: %w", err)
			}
			if stored == nil {
				return nil
			}
			target = stored.GetTarget()
		}
		if target == commonpb.QueryTarget_QUERY_TARGET_LOGS {
			update.Filter = update.GetFilter().CloneVT()
			resolveLogIDFilter(update.GetFilter())
		}
	}

	return nil
}

func hasTransactionIDCondition(filter *commonpb.QueryFilter) bool {
	if filter == nil {
		return false
	}
	switch arm := filter.GetFilter().(type) {
	case *commonpb.QueryFilter_BuiltinUint:
		return arm.BuiltinUint.GetField() == commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID
	case *commonpb.QueryFilter_And:
		if slices.ContainsFunc(arm.And.GetFilters(), hasTransactionIDCondition) {
			return true
		}
	case *commonpb.QueryFilter_Or:
		if slices.ContainsFunc(arm.Or.GetFilters(), hasTransactionIDCondition) {
			return true
		}
	case *commonpb.QueryFilter_Not:
		return hasTransactionIDCondition(arm.Not.GetFilter())
	}

	return false
}

func resolveLogIDFilter(filter *commonpb.QueryFilter) {
	if filter == nil {
		return
	}
	switch arm := filter.GetFilter().(type) {
	case *commonpb.QueryFilter_BuiltinUint:
		if arm.BuiltinUint.GetField() == commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID {
			filter.Filter = &commonpb.QueryFilter_LogId{LogId: &commonpb.LogIdCondition{Cond: arm.BuiltinUint.GetCond()}}
		}
	case *commonpb.QueryFilter_And:
		for _, child := range arm.And.GetFilters() {
			resolveLogIDFilter(child)
		}
	case *commonpb.QueryFilter_Or:
		for _, child := range arm.Or.GetFilters() {
			resolveLogIDFilter(child)
		}
	case *commonpb.QueryFilter_Not:
		resolveLogIDFilter(arm.Not.GetFilter())
	}
}
