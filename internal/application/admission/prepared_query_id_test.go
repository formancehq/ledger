package admission

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestRequestToOrder_ResolvesPreparedQueryIDBeforeCommit(t *testing.T) {
	t.Parallel()

	for _, target := range []commonpb.QueryTarget{
		commonpb.QueryTarget_QUERY_TARGET_LOGS,
		commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
	} {
		ctx := context.Background()
		admission := &Admission{}
		overlay := newBulkOverlay()
		_, err := admission.requestToOrder(ctx, &servicepb.Request{Type: &servicepb.Request_CreatePreparedQuery{
			CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{Ledger: "ledger1", Query: &commonpb.PreparedQuery{Name: "q1", Target: target}},
		}}, nil, overlay)
		require.NoError(t, err)

		filter := &commonpb.QueryFilter{}
		require.NoError(t, json.Unmarshal([]byte(`{"$or":[{"$match":{"id":"7"}},{"$match":{"id":"8"}}]}`), filter))
		order, err := admission.requestToOrder(ctx, &servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{
			UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{Ledger: "ledger1", Name: "q1", Filter: filter},
		}}, nil, overlay)
		require.NoError(t, err)
		require.NotNil(t, filter.GetOr().GetFilters()[0].GetBuiltinUint(), "wire request must not be mutated")
		for _, child := range order.GetLedgerScoped().GetUpdatePreparedQuery().GetFilter().GetOr().GetFilters() {
			if target == commonpb.QueryTarget_QUERY_TARGET_LOGS {
				require.NotNil(t, child.GetLogId())
			} else {
				require.NotNil(t, child.GetBuiltinUint())
			}
		}
	}
}

func TestRestorePreparedQueryIDRetryAfterDeletion(t *testing.T) {
	store := createTestStoreWithoutPolicy(t)
	adm := &Admission{store: store, attrs: attributes.New(), readBarrier: func(context.Context) error { return nil }}
	ctx := context.Background()
	const key = "prepared-query-retry"

	request := func(name, id string) *servicepb.Request {
		filter := &commonpb.QueryFilter{}
		require.NoError(t, json.Unmarshal([]byte(`{"$match":{"id":"`+id+`"}}`), filter))

		return &servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{
			UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{Ledger: testLedgerName, Name: name, Filter: filter},
		}}
	}
	requests := []*servicepb.Request{request("logs", "7"), request("transactions", "8")}
	originalOverlay := newBulkOverlay()
	originalOverlay.preparedTargets.Put(domain.PreparedQueryKey{LedgerName: testLedgerName, Name: "logs"}, commonpb.QueryTarget_QUERY_TARGET_LOGS)
	originalOverlay.preparedTargets.Put(domain.PreparedQueryKey{LedgerName: testLedgerName, Name: "transactions"}, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
	original := make([]*raftcmdpb.Order, len(requests))
	for i, req := range requests {
		var err error
		original[i], err = adm.requestToOrder(ctx, req, nil, originalOverlay)
		require.NoError(t, err)
	}
	hash := processing.HashOrders(original)

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveIdempotencyKey(batch, key, &commonpb.IdempotencyKeyValue{Hash: hash, CreatedAt: 100, FirstLogSequence: 1, LogCount: 2}))
	require.NoError(t, state.AppendLogs(batch, []*commonpb.Log{{Sequence: 1}, {Sequence: 2}}))
	entry := &auditpb.AuditEntry{Sequence: 1, Timestamp: &commonpb.Timestamp{Data: 100}, Idempotency: &commonpb.Idempotency{Key: key}}
	batch.KeyBuilder.PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(1)
	require.NoError(t, batch.SetProto(batch.KeyBuilder.Consume(), entry))
	for i, order := range original {
		item := &auditpb.AuditItem{OrderIndex: uint32(i), SerializedOrder: processing.MarshalOrderBusinessIntent(order, nil)}
		batch.KeyBuilder.PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAuditItem).PutUint64(1).PutUint32(uint32(i))
		require.NoError(t, batch.SetProto(batch.KeyBuilder.Consume(), item))
	}
	// A later conflicting use of the same key must not hide its frozen entry.
	batch.KeyBuilder.PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(2)
	require.NoError(t, batch.SetProto(batch.KeyBuilder.Consume(), &auditpb.AuditEntry{
		Sequence: 2, Timestamp: &commonpb.Timestamp{Data: 101}, Idempotency: &commonpb.Idempotency{Key: key},
	}))
	require.NoError(t, batch.Commit())

	// Both queries have disappeared. The first filter matched a LOGS target
	// while the second was a transaction id, even in one batch.
	retry := make([]*raftcmdpb.Order, len(requests))
	for i, req := range requests {
		var err error
		retry[i], err = adm.requestToOrder(ctx, req, nil, newBulkOverlay())
		require.NoError(t, err)
	}
	require.False(t, bytes.Equal(hash, processing.HashOrders(retry)))
	replay, err := adm.replayPreparedQueryIDRetry(ctx, key, retry)
	require.NoError(t, err)
	require.True(t, replay.Replayed)
	require.Len(t, replay.Logs, 2)
	require.False(t, bytes.Equal(hash, processing.HashOrders(retry)), "replay must not rewrite a future fresh order")
	require.NotNil(t, retry[0].GetLedgerScoped().GetUpdatePreparedQuery().GetFilter().GetBuiltinUint())
	require.NotNil(t, retry[1].GetLedgerScoped().GetUpdatePreparedQuery().GetFilter().GetBuiltinUint())
	candidate := []*raftcmdpb.Order{retry[0].CloneVT(), retry[1].CloneVT()}
	require.True(t, restoreEquivalentIDFilters(candidate, original, hash))
	require.Equal(t, hash, processing.HashOrders(candidate))
	readyAdmission, _ := createTestAdmission(t, store)
	readyAdmission.readBarrier = func(context.Context) error { return nil }
	plan := &admissionPlan{key: key, buildOrders: func(context.Context) ([]*raftcmdpb.Order, *bulkOverlay, error) {
		return retry, newBulkOverlay(), nil
	}}
	proposed, _, _, err := readyAdmission.prepareOrders(ctx, plan)
	require.NoError(t, err)
	require.Nil(t, proposed, "historical filter must not be proposed to Raft")
	require.True(t, plan.replay.Replayed)

	// A different body with the same key must remain different for the FSM's
	// normal IDEMPOTENCY_KEY_CONFLICT check.
	changed, err := adm.requestToOrder(ctx, request("logs", "9"), nil, newBulkOverlay())
	require.NoError(t, err)
	other := []*raftcmdpb.Order{changed, retry[1].CloneVT()}
	replay, err = adm.replayPreparedQueryIDRetry(ctx, key, other)
	require.NoError(t, err)
	require.Nil(t, replay)
	require.NotEqual(t, hash, processing.HashOrders(other))

	// An expired record is no longer a replay candidate. The FSM advances the
	// HLC by one before expiry comparison, so the E-1 boundary must be skipped.
	expiresAt := uint64(time.Now().Add(time.Hour).UnixMicro())
	batch = store.OpenWriteSession()
	require.NoError(t, state.SaveIdempotencyKey(batch, key, &commonpb.IdempotencyKeyValue{
		Hash: hash, CreatedAt: 100, ExpiresAt: expiresAt, FirstLogSequence: 1, LogCount: 2,
	}))
	require.NoError(t, state.StoreLastAppliedTimestamp(batch, expiresAt-1))
	_, err = adm.attrs.PreparedQuery.Set(batch, domain.PreparedQueryKey{LedgerName: testLedgerName, Name: "logs"}.Bytes(),
		&commonpb.PreparedQuery{Name: "logs", Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
	fresh, err := adm.requestToOrder(ctx, requests[0], nil, newBulkOverlay())
	require.NoError(t, err)
	current := []*raftcmdpb.Order{fresh, retry[1].CloneVT()}
	replay, err = adm.replayPreparedQueryIDRetry(ctx, key, current)
	require.NoError(t, err)
	require.Nil(t, replay)
	require.NotEqual(t, hash, processing.HashOrders(current))
	require.NotNil(t, current[0].GetLedgerScoped().GetUpdatePreparedQuery().GetFilter().GetBuiltinUint())

	// The first lookup can see a live key while a later committed write makes
	// it expired. The barrier forces a fresh snapshot before replaying it.
	batch = store.OpenWriteSession()
	require.NoError(t, state.SaveIdempotencyKey(batch, key, &commonpb.IdempotencyKeyValue{
		Hash: hash, CreatedAt: 100, FirstLogSequence: 1, LogCount: 2,
	}))
	require.NoError(t, state.StoreLastAppliedTimestamp(batch, 101))
	require.NoError(t, batch.Commit())
	barrierCalls := 0
	adm.readBarrier = func(context.Context) error {
		barrierCalls++
		batch := store.OpenWriteSession()
		require.NoError(t, state.SaveIdempotencyKey(batch, key, &commonpb.IdempotencyKeyValue{
			Hash: hash, CreatedAt: 100, ExpiresAt: expiresAt, FirstLogSequence: 1, LogCount: 2,
		}))
		require.NoError(t, state.StoreLastAppliedTimestamp(batch, expiresAt))

		return batch.Commit()
	}
	replay, err = adm.replayPreparedQueryIDRetry(ctx, key, current)
	require.NoError(t, err)
	require.Nil(t, replay)
	require.Equal(t, 1, barrierCalls)

	// Store closure needs the exclusive DB lock. It must complete while the
	// barrier runs, and a failed snapshot reopen must return without panicking.
	batch = store.OpenWriteSession()
	require.NoError(t, state.SaveIdempotencyKey(batch, key, &commonpb.IdempotencyKeyValue{
		Hash: hash, CreatedAt: 100, FirstLogSequence: 1, LogCount: 2,
	}))
	require.NoError(t, state.StoreLastAppliedTimestamp(batch, 101))
	require.NoError(t, batch.Commit())
	adm.readBarrier = func(context.Context) error {
		closed := make(chan error, 1)
		go func() { closed <- store.Close() }()
		select {
		case err := <-closed:
			return err
		case <-time.After(2 * time.Second):
			return context.DeadlineExceeded
		}
	}
	replay, err = adm.replayPreparedQueryIDRetry(ctx, key, current)
	require.Nil(t, replay)
	require.ErrorIs(t, err, dal.ErrStoreClosed)
}

func TestRequestToOrder_ResolvesStoredLogQueryIDBeforeCommit(t *testing.T) {
	store := createTestStoreWithoutPolicy(t)
	attrs := attributes.New()
	batch := store.OpenWriteSession()
	_, err := attrs.PreparedQuery.Set(batch, domain.PreparedQueryKey{LedgerName: testLedgerName, Name: "q1"}.Bytes(),
		&commonpb.PreparedQuery{Name: "q1", Target: commonpb.QueryTarget_QUERY_TARGET_LOGS})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	filter := &commonpb.QueryFilter{}
	require.NoError(t, json.Unmarshal([]byte(`{"$match":{"id":"7"}}`), filter))
	admission := &Admission{store: store, attrs: attrs}
	order, err := admission.requestToOrder(context.Background(), &servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{
		UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{Ledger: testLedgerName, Name: "q1", Filter: filter},
	}}, nil, newBulkOverlay())
	require.NoError(t, err)
	require.NotNil(t, order.GetLedgerScoped().GetUpdatePreparedQuery().GetFilter().GetLogId())
	require.NotNil(t, filter.GetBuiltinUint(), "wire request must not be mutated")
}
