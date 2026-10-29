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
	adm := &Admission{store: store, attrs: attributes.New()}
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

	// Both queries have disappeared. The first filter must recover its LOGS
	// target while the second stays a transaction id, even in one batch.
	retry := make([]*raftcmdpb.Order, len(requests))
	for i, req := range requests {
		var err error
		retry[i], err = adm.requestToOrder(ctx, req, nil, newBulkOverlay())
		require.NoError(t, err)
	}
	require.False(t, bytes.Equal(hash, processing.HashOrders(retry)))
	require.NoError(t, adm.restorePreparedQueryIDRetry(ctx, key, retry))
	require.Equal(t, hash, processing.HashOrders(retry))
	require.NotNil(t, retry[0].GetLedgerScoped().GetUpdatePreparedQuery().GetFilter().GetLogId())
	require.NotNil(t, retry[1].GetLedgerScoped().GetUpdatePreparedQuery().GetFilter().GetBuiltinUint())

	// A different body with the same key must remain different for the FSM's
	// normal IDEMPOTENCY_KEY_CONFLICT check.
	changed, err := adm.requestToOrder(ctx, request("logs", "9"), nil, newBulkOverlay())
	require.NoError(t, err)
	other := []*raftcmdpb.Order{changed, retry[1].CloneVT()}
	require.NoError(t, adm.restorePreparedQueryIDRetry(ctx, key, other))
	require.NotEqual(t, hash, processing.HashOrders(other))

	// An expired record is no longer a replay candidate. Even if the HLC is
	// ahead of wall time, the fresh update must keep the current target arm.
	expiresAt := uint64(time.Now().Add(time.Hour).UnixMicro())
	batch = store.OpenWriteSession()
	require.NoError(t, state.SaveIdempotencyKey(batch, key, &commonpb.IdempotencyKeyValue{
		Hash: hash, CreatedAt: 100, ExpiresAt: expiresAt, FirstLogSequence: 1, LogCount: 2,
	}))
	require.NoError(t, state.StoreLastAppliedTimestamp(batch, expiresAt))
	_, err = adm.attrs.PreparedQuery.Set(batch, domain.PreparedQueryKey{LedgerName: testLedgerName, Name: "logs"}.Bytes(),
		&commonpb.PreparedQuery{Name: "logs", Target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
	fresh, err := adm.requestToOrder(ctx, requests[0], nil, newBulkOverlay())
	require.NoError(t, err)
	current := []*raftcmdpb.Order{fresh, retry[1].CloneVT()}
	require.NoError(t, adm.restorePreparedQueryIDRetry(ctx, key, current))
	require.NotEqual(t, hash, processing.HashOrders(current))
	require.NotNil(t, current[0].GetLedgerScoped().GetUpdatePreparedQuery().GetFilter().GetBuiltinUint())
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
