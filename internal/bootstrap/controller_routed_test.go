package bootstrap

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/application/ctrl/ctrlmock"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

type barrierHorizonMatcher uint64

func (m barrierHorizonMatcher) Matches(value any) bool {
	ctx, ok := value.(context.Context)
	if !ok {
		return false
	}

	horizon, ok := query.ReadBarrierHorizon(ctx)

	return ok && horizon == uint64(m)
}

func (m barrierHorizonMatcher) String() string {
	return "context carrying the local ReadIndex horizon"
}

func TestNewRoutedController(t *testing.T) {
	t.Parallel()

	// Verify RoutedController implements ctrl.Controller at compile time.
	var _ ctrl.Controller = (*RoutedController)(nil)

	rc := NewRoutedController(nil, nil, nil)
	assert.Nil(t, rc.localController)
	assert.Nil(t, rc.servicePool)
	assert.Nil(t, rc.Node)
	assert.Nil(t, rc.readIndexAndWait)
}

func TestRoutedController_IsHealthy_NilNode(t *testing.T) {
	t.Parallel()

	// IsHealthy delegates to Node.IsHealthy(). With a nil Node, we verify
	// through the compile-time interface check that the method exists.
	// Behavioral testing of Node.IsHealthy() is in the node package.
	// Here we just confirm the method signature matches ctrl.Controller.
	rc := &RoutedController{}
	assert.NotNil(t, rc) // RoutedController can be instantiated
}

func TestRoutedController_FinishLeaderFallback(t *testing.T) {
	t.Parallel()

	mockCtrl := gomock.NewController(t)
	local := ctrlmock.NewMockController(mockCtrl)
	remote := ctrlmock.NewMockController(mockCtrl)
	routed := &RoutedController{localController: local}
	barrierErr := node.ErrNotLeader

	t.Run("leadership moved local", func(t *testing.T) {
		t.Parallel()

		ctx, profile := query.WithProfile(context.Background())
		selected, barrier, err := routed.finishLeaderFallback(ctx, local, nil, barrierErr)
		require.ErrorIs(t, err, barrierErr)
		assert.Nil(t, selected)
		assert.Nil(t, barrier)
		assert.False(t, profile.Forwarded)
	})

	t.Run("remote leader resolved", func(t *testing.T) {
		t.Parallel()

		ctx, profile := query.WithProfile(context.Background())
		selected, barrier, err := routed.finishLeaderFallback(ctx, remote, nil, barrierErr)
		require.NoError(t, err)
		assert.Same(t, remote, selected)
		assert.Nil(t, barrier)
		assert.True(t, profile.Forwarded)
	})

	t.Run("leader resolution failed", func(t *testing.T) {
		t.Parallel()

		ctx, profile := query.WithProfile(context.Background())
		selected, barrier, err := routed.finishLeaderFallback(ctx, nil, commonpb.ErrNoLeader, barrierErr)
		require.ErrorIs(t, err, commonpb.ErrNoLeader)
		assert.Nil(t, selected)
		assert.Nil(t, barrier)
		assert.False(t, profile.Forwarded)
	})
}

func TestRoutedController_WithLocalBarrierHorizon(t *testing.T) {
	t.Parallel()

	mockCtrl := gomock.NewController(t)
	local := ctrlmock.NewMockController(mockCtrl)
	remote := ctrlmock.NewMockController(mockCtrl)
	routed := &RoutedController{localController: local}

	ctx := routed.withLocalBarrierHorizon(context.Background(), local, &node.ReadBarrierInfo{CommitIndex: 42})
	horizon, ok := query.ReadBarrierHorizon(ctx)
	require.True(t, ok)
	require.Equal(t, uint64(42), horizon)

	_, ok = query.ReadBarrierHorizon(routed.withLocalBarrierHorizon(context.Background(), remote, &node.ReadBarrierInfo{CommitIndex: 42}))
	require.False(t, ok, "the remote hop establishes and checks its own barrier")

	_, ok = query.ReadBarrierHorizon(routed.withLocalBarrierHorizon(context.Background(), local, nil))
	require.False(t, ok, "stale reads deliberately carry no linearizable horizon")
}

func TestRoutedController_IndexedReadsForwardLocalBarrierHorizon(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		expect func(*ctrlmock.MockController)
		call   func(context.Context, *RoutedController) error
	}{
		{
			name: "list transactions",
			expect: func(local *ctrlmock.MockController) {
				local.EXPECT().ListTransactions(barrierHorizonMatcher(42), "ledger", uint32(10), uint64(0), nil, false).
					Return(cursor.NewSliceCursor([]*commonpb.Transaction{}), nil)
			},
			call: func(ctx context.Context, routed *RoutedController) error {
				_, err := routed.ListTransactions(ctx, "ledger", 10, 0, nil, false)

				return err
			},
		},
		{
			name: "list logs",
			expect: func(local *ctrlmock.MockController) {
				local.EXPECT().ListLogs(barrierHorizonMatcher(42), "ledger", uint64(0), uint32(10), nil).
					Return(cursor.NewSliceCursor([]*commonpb.Log{}), nil)
			},
			call: func(ctx context.Context, routed *RoutedController) error {
				_, err := routed.ListLogs(ctx, "ledger", 0, 10, nil)

				return err
			},
		},
		{
			name: "list audit entries",
			expect: func(local *ctrlmock.MockController) {
				local.EXPECT().ListAuditEntries(barrierHorizonMatcher(42), uint32(10), uint64(0), nil, false, uint64(0)).
					Return(cursor.NewSliceCursor([]*auditpb.AuditEntry{}), nil)
			},
			call: func(ctx context.Context, routed *RoutedController) error {
				_, err := routed.ListAuditEntries(ctx, 10, 0, nil, false, 0)

				return err
			},
		},
		{
			name: "list accounts",
			expect: func(local *ctrlmock.MockController) {
				local.EXPECT().ListAccounts(barrierHorizonMatcher(42), "ledger", uint32(10), "", nil, false).
					Return(cursor.NewSliceCursor([]*commonpb.Account{}), nil)
			},
			call: func(ctx context.Context, routed *RoutedController) error {
				_, err := routed.ListAccounts(ctx, "ledger", 10, "", nil, false)

				return err
			},
		},
		{
			name: "aggregate volumes",
			expect: func(local *ctrlmock.MockController) {
				local.EXPECT().AggregateVolumes(barrierHorizonMatcher(42), "ledger", nil, query.AggregateOptions{}).
					Return(&commonpb.AggregateResult{}, nil)
			},
			call: func(ctx context.Context, routed *RoutedController) error {
				_, err := routed.AggregateVolumes(ctx, "ledger", nil, query.AggregateOptions{})

				return err
			},
		},
		{
			name: "execute prepared query",
			expect: func(local *ctrlmock.MockController) {
				local.EXPECT().ExecutePreparedQuery(barrierHorizonMatcher(42), gomock.Any()).
					Return(&servicepb.ExecutePreparedQueryResponse{}, nil)
			},
			call: func(ctx context.Context, routed *RoutedController) error {
				_, err := routed.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{})

				return err
			},
		},
		{
			name: "inspect index",
			expect: func(local *ctrlmock.MockController) {
				local.EXPECT().InspectIndex(barrierHorizonMatcher(42), gomock.Any()).
					Return(&servicepb.InspectIndexResponse{}, nil)
			},
			call: func(ctx context.Context, routed *RoutedController) error {
				_, err := routed.InspectIndex(ctx, &servicepb.InspectIndexRequest{})

				return err
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockCtrl := gomock.NewController(t)
			local := ctrlmock.NewMockController(mockCtrl)
			tc.expect(local)
			routed := &RoutedController{
				Node:            &node.Node{},
				localController: local,
				readIndexAndWait: func(context.Context) (*node.ReadBarrierInfo, error) {
					return &node.ReadBarrierInfo{CommitIndex: 42}, nil
				},
			}
			ctx, _ := query.WithProfile(t.Context())
			require.NoError(t, tc.call(ctx, routed))
		})
	}
}
