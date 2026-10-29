package admission

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
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
			update.IdResolvedToLogPaths = resolveLogIDFilter(update.GetFilter())
		}
	}

	return nil
}

// replayPreparedQueryIDRetry returns a frozen outcome directly when a prepared
// query's target changed after the original request. The audit orders prove
// that the caller's current business intent is identical despite the target
// specific ID arm. The consistent read snapshot is the replay's decision
// point. No historical order is submitted to Raft: if the key expires after
// this read, a later fresh request uses the current target.
func (a *Admission) replayPreparedQueryIDRetry(ctx context.Context, key string, orders []*raftcmdpb.Order) (*domain.ApplyResult, error) {
	if key == "" || !slices.ContainsFunc(orders, hasPreparedQueryIDUpdate) {
		return nil, nil
	}

	handle, err := a.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("opening retry read handle: %w", err)
	}
	defer func() {
		if handle != nil {
			_ = handle.Close()
		}
	}()

	stored, err := state.LoadIdempotencyKey(handle, key)
	if err != nil || stored == nil {
		return nil, err
	}
	if bytes.Equal(processing.HashOrders(orders), stored.GetHash()) {
		return nil, nil
	}
	if a.readBarrier == nil {
		return nil, errors.New("linearizable idempotency replay barrier unavailable")
	}
	// Release the candidate snapshot before waiting: the barrier may need an
	// exclusive store lifecycle lock (for example during checkpoint restore).
	if err := handle.Close(); err != nil {
		handle = nil

		return nil, fmt.Errorf("closing retry read handle: %w", err)
	}
	handle = nil
	if err := a.readBarrier(ctx); err != nil {
		return nil, fmt.Errorf("waiting for idempotency replay barrier: %w", err)
	}
	// The initial lookup is only a cheap candidate check. Reopen the snapshot
	// after the barrier so an overwritten or evicted key cannot be replayed.
	handle, err = a.store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("opening replay snapshot: %w", err)
	}
	stored, err = state.LoadIdempotencyKey(handle, key)
	if err != nil || stored == nil {
		return nil, err
	}
	if bytes.Equal(processing.HashOrders(orders), stored.GetHash()) {
		return nil, nil
	}
	lastApplied, err := query.ReadLastAppliedTimestamp(handle)
	if err != nil {
		return nil, fmt.Errorf("reading retry HLC: %w", err)
	}
	if preparedQueryRetryExpired(stored, lastApplied) {
		return nil, nil
	}

	// The FSM HLC strictly increases for every committed audit entry. Locate
	// the frozen timestamp in logarithmic reads, independent of key age.
	last, err := query.ReadLastAuditSequence(handle)
	if err != nil || last == 0 {
		return nil, err
	}
	entry, err := locateRetryAuditEntry(ctx, last, stored.GetCreatedAt(), func(seq uint64) (*auditpb.AuditEntry, error) {
		return query.ReadAuditEntry(ctx, handle, seq)
	})
	if err != nil {
		return nil, fmt.Errorf("locating retry audit entry: %w", err)
	}
	if entry != nil && entry.GetIdempotency().GetKey() == key {
		items, readErr := query.ReadAuditItems(ctx, handle, entry.GetSequence())
		if readErr != nil {
			return nil, fmt.Errorf("reading retry audit orders: %w", readErr)
		}
		original := make([]*raftcmdpb.Order, len(items))
		for i, item := range items {
			original[i] = &raftcmdpb.Order{}
			if err := original[i].UnmarshalVT(item.GetSerializedOrder()); err != nil {
				return nil, fmt.Errorf("decoding retry audit order: %w", err)
			}
		}
		if bytes.Equal(processing.HashOrders(original), stored.GetHash()) {
			candidate := make([]*raftcmdpb.Order, len(orders))
			for i, order := range orders {
				candidate[i] = order.CloneVT()
			}
			if restoreEquivalentIDFilters(candidate, original, stored.GetHash()) &&
				!preparedQueryRetryExpired(stored, lastApplied) {
				return replayPreparedQueryOutcome(ctx, handle, stored)
			}

			return nil, nil
		}
	}

	return nil, nil
}

func locateRetryAuditEntry(ctx context.Context, last, createdAt uint64, read func(uint64) (*auditpb.AuditEntry, error)) (*auditpb.AuditEntry, error) {
	lo, hi := uint64(1), last
	for lo < hi {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		mid := lo + (hi-lo)/2
		entry, err := read(mid)
		if err != nil {
			return nil, err
		}
		if entry.GetTimestamp().GetData() < createdAt {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entry, err := read(lo)
	if err != nil || entry.GetTimestamp().GetData() != createdAt {
		return nil, err
	}

	return entry, nil
}

func preparedQueryRetryExpired(stored *commonpb.IdempotencyKeyValue, lastApplied uint64) bool {
	expiresAt := stored.GetExpiresAt()

	return expiresAt != 0 && (expiresAt <= uint64(time.Now().UnixMicro()) || lastApplied >= expiresAt-1)
}

func replayPreparedQueryOutcome(ctx context.Context, handle *dal.ReadHandle, stored *commonpb.IdempotencyKeyValue) (*domain.ApplyResult, error) {
	if failure := stored.GetFailure(); failure != nil {
		facts, err := domain.FailureFactsFromDetails(failure.GetDetails())
		if err != nil {
			return nil, fmt.Errorf("decoding frozen prepared query failure: %w", err)
		}
		message, _ := domain.RenderFailureFacts(facts)

		return nil, &domain.BusinessError{Err: &domain.ReplayedFailure{
			ErrReason: domain.ReasonString(failure.GetReason()), Msg: message, Details: failure.GetDetails(),
		}}
	}
	logs := make([]*commonpb.Log, stored.GetLogCount())
	for i := range logs {
		log, err := query.ReadLogBySequence(ctx, handle, stored.GetFirstLogSequence()+uint64(i))
		if err != nil {
			return nil, fmt.Errorf("reading frozen prepared query log: %w", err)
		}
		logs[i] = log
	}

	return &domain.ApplyResult{Logs: logs, Replayed: true}, nil
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

func restoreEquivalentIDFilters(orders, original []*raftcmdpb.Order, hash []byte) bool {
	if len(orders) != len(original) {
		return false
	}
	previous := make([]*commonpb.QueryFilter, len(orders))
	previousPaths := make([][]string, len(orders))
	for i, order := range orders {
		update := order.GetLedgerScoped().GetUpdatePreparedQuery()
		prior := original[i].GetLedgerScoped().GetUpdatePreparedQuery()
		if update == nil || prior == nil || update.GetFilter() == nil || prior.GetFilter() == nil {
			continue
		}
		if len(prior.GetIdResolvedToLogPaths()) > 0 && len(update.GetIdResolvedToLogPaths()) == 0 && hasTransactionIDCondition(update.GetFilter()) {
			candidate := update.GetFilter().CloneVT()
			paths := resolveLogIDFilter(candidate)
			if !slices.Equal(paths, prior.GetIdResolvedToLogPaths()) || !candidate.EqualVT(prior.GetFilter()) {
				continue
			}
			previous[i] = update.GetFilter()
			previousPaths[i] = update.GetIdResolvedToLogPaths()
			update.Filter = candidate
			update.IdResolvedToLogPaths = paths

			continue
		}
		if len(prior.GetIdResolvedToLogPaths()) == 0 && len(update.GetIdResolvedToLogPaths()) > 0 && hasTransactionIDCondition(prior.GetFilter()) {
			candidate := prior.GetFilter().CloneVT()
			paths := resolveLogIDFilter(candidate)
			if !slices.Equal(paths, update.GetIdResolvedToLogPaths()) || !candidate.EqualVT(update.GetFilter()) {
				continue
			}
			previous[i] = update.GetFilter()
			previousPaths[i] = update.GetIdResolvedToLogPaths()
			update.Filter = prior.GetFilter().CloneVT()
			update.IdResolvedToLogPaths = nil
		}
	}
	if !bytes.Equal(processing.HashOrders(orders), hash) {
		for i, filter := range previous {
			if filter != nil {
				update := orders[i].GetLedgerScoped().GetUpdatePreparedQuery()
				update.Filter = filter
				update.IdResolvedToLogPaths = previousPaths[i]
			}
		}

		return false
	}

	return true
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

func resolveLogIDFilter(filter *commonpb.QueryFilter) []string {
	var paths []string
	resolveLogIDFilterAtPath(filter, "$", &paths)

	return paths
}

func resolveLogIDFilterAtPath(filter *commonpb.QueryFilter, path string, paths *[]string) {
	if filter == nil {
		return
	}
	switch arm := filter.GetFilter().(type) {
	case *commonpb.QueryFilter_BuiltinUint:
		if arm.BuiltinUint.GetField() == commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID {
			filter.Filter = &commonpb.QueryFilter_LogId{LogId: &commonpb.LogIdCondition{Cond: arm.BuiltinUint.GetCond()}}
			*paths = append(*paths, path)
		}
	case *commonpb.QueryFilter_And:
		for i, child := range arm.And.GetFilters() {
			resolveLogIDFilterAtPath(child, path+"/"+strconv.Itoa(i), paths)
		}
	case *commonpb.QueryFilter_Or:
		for i, child := range arm.Or.GetFilters() {
			resolveLogIDFilterAtPath(child, path+"/"+strconv.Itoa(i), paths)
		}
	case *commonpb.QueryFilter_Not:
		resolveLogIDFilterAtPath(arm.Not.GetFilter(), path+"/not", paths)
	}
}
