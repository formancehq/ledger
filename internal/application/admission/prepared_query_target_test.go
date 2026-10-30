package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestRequestToOrderPreparedQueryTargetIsExplicit(t *testing.T) {
	t.Parallel()
	filter, err := commonpb.DecodeQueryFilterJSON([]byte(`{"$match":{"id":7}}`), commonpb.QueryTarget_QUERY_TARGET_LOGS)
	require.NoError(t, err)
	target := commonpb.QueryTarget_QUERY_TARGET_LOGS
	request := &servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{
		UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{
			Ledger: "ledger1", Name: "q1", Filter: filter, Target: target.Enum(),
		},
	}}
	admission := &Admission{}
	first, err := admission.requestToOrder(context.Background(), request, nil, newBulkOverlay())
	require.NoError(t, err)
	update := first.GetLedgerScoped().GetUpdatePreparedQuery()
	require.NotNil(t, update.Target)
	require.Equal(t, target, update.GetTarget())
	require.NotNil(t, update.GetFilter().GetLogId())

	// The order no longer reads a stored target: recreating the query with
	// another target cannot change the hash of the same explicit request.
	second, err := admission.requestToOrder(context.Background(), request, nil, newBulkOverlay())
	require.NoError(t, err)
	require.Equal(t, processing.HashOrders([]*raftcmdpb.Order{first}), processing.HashOrders([]*raftcmdpb.Order{second}))
}

func TestRequestToOrderPreparedQueryRejectsDeepFilter(t *testing.T) {
	t.Parallel()
	filter, err := commonpb.DecodeQueryFilterJSON([]byte(`{"$match":{"id":7}}`), commonpb.QueryTarget_QUERY_TARGET_LOGS)
	require.NoError(t, err)
	for range domain.MaxFilterDepth {
		filter = &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: filter}}}
	}
	target := commonpb.QueryTarget_QUERY_TARGET_LOGS
	request := &servicepb.Request{Type: &servicepb.Request_UpdatePreparedQuery{
		UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{
			Ledger: "ledger1", Name: "q1", Filter: filter, Target: target.Enum(),
		},
	}}
	_, err = (&Admission{}).requestToOrder(context.Background(), request, nil, newBulkOverlay())
	require.ErrorIs(t, err, domain.ErrFilterTooDeep)
}
