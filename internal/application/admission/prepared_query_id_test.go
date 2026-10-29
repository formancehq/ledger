package admission

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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
