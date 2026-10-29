package admission

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/state"
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

// restorePreparedQueryIDRetry keeps the committed business intent stable when
// a prepared query was deleted or recreated between an idempotent request and
// its retry. The prior audit items are the durable source of the original
// target; using the current query alone would turn a replay into a conflict.
// Only equivalent filters with a different id arm are adjusted, and the
// complete batch must still match the frozen hash before any change is used.
func (a *Admission) restorePreparedQueryIDRetry(ctx context.Context, key string, orders []*raftcmdpb.Order) error {
	if key == "" || !slices.ContainsFunc(orders, hasPreparedQueryIDUpdate) {
		return nil
	}

	handle, err := a.store.NewReadHandle()
	if err != nil {
		return fmt.Errorf("opening retry read handle: %w", err)
	}
	defer func() { _ = handle.Close() }()

	stored, err := state.LoadIdempotencyKey(handle, key)
	if err != nil || stored == nil {
		return err
	}
	if bytes.Equal(processing.HashOrders(orders), stored.GetHash()) {
		return nil
	}
	lastApplied, err := query.ReadLastAppliedTimestamp(handle)
	if err != nil {
		return fmt.Errorf("reading retry HLC: %w", err)
	}
	// The FSM compares expiry with max(proposal date, last HLC + 1). An
	// already-expired outcome is a fresh request against the current target.
	now := uint64(time.Now().UnixMicro())
	if stored.GetExpiresAt() != 0 &&
		(stored.GetExpiresAt() <= now || stored.GetExpiresAt() <= lastApplied) {
		return nil
	}

	// Audit timestamps are monotonic. Locate the frozen timestamp by sequence
	// instead of scanning every later proposal for a long-lived key.
	last, err := query.ReadLastAuditSequence(handle)
	if err != nil || last == 0 {
		return err
	}
	lo, hi := uint64(1), last
	for lo < hi {
		mid := lo + (hi-lo)/2
		entry, err := query.ReadAuditEntry(ctx, handle, mid)
		if err != nil {
			return fmt.Errorf("locating retry audit entry: %w", err)
		}
		if entry.GetTimestamp().GetData() < stored.GetCreatedAt() {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	for seq := lo; ; seq++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		entry, err := query.ReadAuditEntry(ctx, handle, seq)
		if err != nil {
			return fmt.Errorf("reading retry audit entry: %w", err)
		}
		if entry.GetTimestamp().GetData() > stored.GetCreatedAt() {
			return nil
		}
		if entry.GetTimestamp().GetData() == stored.GetCreatedAt() && entry.GetIdempotency().GetKey() == key {
			items, readErr := query.ReadAuditItems(ctx, handle, entry.GetSequence())
			if readErr != nil {
				return fmt.Errorf("reading retry audit orders: %w", readErr)
			}
			original := make([]*raftcmdpb.Order, len(items))
			for i, item := range items {
				original[i] = &raftcmdpb.Order{}
				if err := original[i].UnmarshalVT(item.GetSerializedOrder()); err != nil {
					return fmt.Errorf("decoding retry audit order: %w", err)
				}
			}
			if bytes.Equal(processing.HashOrders(original), stored.GetHash()) {
				restoreEquivalentIDFilters(orders, original, stored.GetHash())

				return nil
			}
		}
		if seq == last {
			return nil
		}
	}
}

func hasPreparedQueryIDUpdate(order *raftcmdpb.Order) bool {
	filter := order.GetLedgerScoped().GetUpdatePreparedQuery().GetFilter()

	return hasTransactionIDCondition(filter) || hasLogIDCondition(filter)
}

func hasLogIDCondition(filter *commonpb.QueryFilter) bool {
	if filter == nil {
		return false
	}
	switch arm := filter.GetFilter().(type) {
	case *commonpb.QueryFilter_LogId:
		return true
	case *commonpb.QueryFilter_And:
		return slices.ContainsFunc(arm.And.GetFilters(), hasLogIDCondition)
	case *commonpb.QueryFilter_Or:
		return slices.ContainsFunc(arm.Or.GetFilters(), hasLogIDCondition)
	case *commonpb.QueryFilter_Not:
		return hasLogIDCondition(arm.Not.GetFilter())
	}

	return false
}

func restoreEquivalentIDFilters(orders, original []*raftcmdpb.Order, hash []byte) {
	if len(orders) != len(original) {
		return
	}
	previous := make([]*commonpb.QueryFilter, len(orders))
	for i, order := range orders {
		update := order.GetLedgerScoped().GetUpdatePreparedQuery()
		prior := original[i].GetLedgerScoped().GetUpdatePreparedQuery()
		if update == nil || prior == nil || update.GetFilter() == nil || prior.GetFilter() == nil {
			continue
		}
		candidate := update.GetFilter().CloneVT()
		resolveLogIDFilter(candidate)
		if candidate.EqualVT(prior.GetFilter()) {
			previous[i] = update.GetFilter()
			update.Filter = candidate

			continue
		}
		candidate = prior.GetFilter().CloneVT()
		resolveLogIDFilter(candidate)
		if candidate.EqualVT(update.GetFilter()) {
			previous[i] = update.GetFilter()
			update.Filter = prior.GetFilter().CloneVT()
		}
	}
	if !bytes.Equal(processing.HashOrders(orders), hash) {
		for i, filter := range previous {
			if filter != nil {
				orders[i].GetLedgerScoped().GetUpdatePreparedQuery().Filter = filter
			}
		}
	}
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
