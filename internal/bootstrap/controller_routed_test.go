package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	grpcadp "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/application/ctrl/ctrlmock"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestNewRoutedController(t *testing.T) {
	t.Parallel()

	// Verify RoutedController implements ctrl.Controller at compile time.
	var _ ctrl.Controller = (*RoutedController)(nil)

	rc := NewRoutedController(nil, nil, nil)
	assert.Nil(t, rc.localController)
	assert.Nil(t, rc.servicePool)
	assert.Nil(t, rc.Node)
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

func TestRoutedController_MarkForwardedOnlyForRemoteController(t *testing.T) {
	t.Parallel()

	mockCtrl := gomock.NewController(t)
	local := ctrlmock.NewMockController(mockCtrl)
	remote := ctrlmock.NewMockController(mockCtrl)
	routed := &RoutedController{localController: local}

	localCtx, localProfile := query.WithProfile(context.Background())
	routed.markForwardedIfRemote(localCtx, local)
	assert.False(t, localProfile.Forwarded)

	remoteCtx, remoteProfile := query.WithProfile(context.Background())
	routed.markForwardedIfRemote(remoteCtx, remote)
	assert.True(t, remoteProfile.Forwarded)
}

func TestRoutedController_LeaderRoutingErrorIsNotForwarded(t *testing.T) {
	t.Parallel()

	routed := &RoutedController{Node: &node.Node{}}
	ctx, profile := query.WithProfile(context.Background())
	ctx = grpcadp.WithConsistency(ctx, grpcadp.ConsistencyLeader)

	_, _, err := routed.readCtrl(ctx)
	require.ErrorIs(t, err, commonpb.ErrNoLeader)
	assert.False(t, profile.Forwarded)
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

// TestShouldForwardIndexBuilding pins the forward decision: a follower's own
// local INDEX_BUILDING refusal forwards to the leader — a rewound read store
// rebuilding through an index's retype chain refuses stale bindings as
// building, and a converged replica can answer now — while every other error,
// the leader itself, explicitly-stale reads (which ask for THIS node's view),
// and reads readCtrl already routed to a remote leader do not forward.
func TestShouldForwardIndexBuilding(t *testing.T) {
	t.Parallel()

	building := &domain.BusinessError{Err: &domain.ErrIndexBuilding{Index: "metadata[\"tier\"]"}}
	notFound := &domain.BusinessError{Err: &domain.ErrIndexNotFound{Index: "metadata[\"tier\"]"}}

	assert.True(t, shouldForwardIndexBuilding(building, true, false, grpcadp.ConsistencyLinearizable))
	assert.False(t, shouldForwardIndexBuilding(building, false, false, grpcadp.ConsistencyLinearizable), "a read already routed to a remote leader is not re-sent")
	assert.False(t, shouldForwardIndexBuilding(building, true, true, grpcadp.ConsistencyLinearizable), "the leader never forwards")
	assert.False(t, shouldForwardIndexBuilding(building, true, false, grpcadp.ConsistencyStale), "a stale read asks for this node's view")
	assert.False(t, shouldForwardIndexBuilding(notFound, true, false, grpcadp.ConsistencyLinearizable), "only the building class forwards")
	assert.False(t, shouldForwardIndexBuilding(nil, true, false, grpcadp.ConsistencyLinearizable))
	assert.False(t, shouldForwardIndexBuilding(errors.New("boom"), true, false, grpcadp.ConsistencyLinearizable))
}

// TestRetryOnStaleBinding executes the forwarding retry itself: a follower's
// local INDEX_BUILDING refusal re-runs on the resolved leader and the profile
// records the forward, while resolution failures, leadership moving local, and
// refusals that already ran remotely all surface the original refusal without
// a retry.
func TestRetryOnStaleBinding(t *testing.T) {
	t.Parallel()

	building := &domain.BusinessError{Err: &domain.ErrIndexBuilding{Index: `metadata["tier"]`}}

	mockCtrl := gomock.NewController(t)
	local := ctrlmock.NewMockController(mockCtrl)
	remote := ctrlmock.NewMockController(mockCtrl)

	newRouted := func(resolve func() (ctrl.Controller, error)) *RoutedController {
		return &RoutedController{Node: &node.Node{}, localController: local, leaderResolver: resolve}
	}
	failResolve := func() (ctrl.Controller, error) {
		t.Error("leader resolution must not run on a non-forwarding path")

		return nil, commonpb.ErrNoLeader
	}
	failRun := func(ctrl.Controller) (string, error) {
		t.Error("the read must not be re-sent on a non-forwarding path")

		return "", nil
	}

	t.Run("forwards a local refusal and serves the leader's result", func(t *testing.T) {
		t.Parallel()

		routed := newRouted(func() (ctrl.Controller, error) { return remote, nil })
		ctx, profile := query.WithProfile(context.Background())

		got, err := retryOnStaleBinding(routed, ctx, local, "", building, func(leader ctrl.Controller) (string, error) {
			assert.Same(t, remote, leader)

			return "leader-page", nil
		})
		require.NoError(t, err)
		assert.Equal(t, "leader-page", got)
		assert.True(t, profile.Forwarded)
	})

	t.Run("surfaces the refusal when leader resolution fails", func(t *testing.T) {
		t.Parallel()

		routed := newRouted(func() (ctrl.Controller, error) { return nil, commonpb.ErrNoLeader })
		ctx, profile := query.WithProfile(context.Background())

		_, err := retryOnStaleBinding(routed, ctx, local, "", building, failRun)
		require.ErrorIs(t, err, error(building))
		assert.False(t, profile.Forwarded)
	})

	t.Run("surfaces the refusal when leadership moved to this node", func(t *testing.T) {
		t.Parallel()

		routed := newRouted(func() (ctrl.Controller, error) { return local, nil })
		ctx, profile := query.WithProfile(context.Background())

		_, err := retryOnStaleBinding(routed, ctx, local, "", building, failRun)
		require.ErrorIs(t, err, error(building))
		assert.False(t, profile.Forwarded)
	})

	t.Run("a refusal that already ran on the leader is not re-sent", func(t *testing.T) {
		t.Parallel()

		routed := newRouted(failResolve)
		ctx, profile := query.WithProfile(context.Background())

		_, err := retryOnStaleBinding(routed, ctx, remote, "", building, failRun)
		require.ErrorIs(t, err, error(building))
		assert.False(t, profile.Forwarded)
	})

	t.Run("an explicitly-stale read keeps this node's refusal", func(t *testing.T) {
		t.Parallel()

		routed := newRouted(failResolve)
		ctx, profile := query.WithProfile(context.Background())
		ctx = grpcadp.WithConsistency(ctx, grpcadp.ConsistencyStale)

		_, err := retryOnStaleBinding(routed, ctx, local, "", building, failRun)
		require.ErrorIs(t, err, error(building))
		assert.False(t, profile.Forwarded)
	})
}

// TestRoutedController_ListAccounts_ForwardsLocalBuildingRefusal drives a real
// wrapped read method end to end: the local controller refuses exactly once
// with INDEX_BUILDING, the leader serves, and the caller gets the leader's
// page with the forward profiled. gomock's Times(1) pins the attempt counts —
// one local attempt, one leader attempt, argument-exact.
func TestRoutedController_ListAccounts_ForwardsLocalBuildingRefusal(t *testing.T) {
	t.Parallel()

	building := &domain.BusinessError{Err: &domain.ErrIndexBuilding{Index: `metadata["tier"]`}}
	filter := &commonpb.QueryFilter{}

	mockCtrl := gomock.NewController(t)
	local := ctrlmock.NewMockController(mockCtrl)
	remote := ctrlmock.NewMockController(mockCtrl)
	leaderPage := cursor.NewSliceCursor([]*commonpb.Account{{Address: "acc:leader"}})

	local.EXPECT().
		ListAccounts(gomock.Any(), "ledger", uint32(10), "after", filter, true).
		Return(nil, building).
		Times(1)
	remote.EXPECT().
		ListAccounts(gomock.Any(), "ledger", uint32(10), "after", filter, true).
		Return(leaderPage, nil).
		Times(1)

	routed := &RoutedController{
		Node:            &node.Node{},
		localController: local,
		leaderResolver:  func() (ctrl.Controller, error) { return remote, nil },
		readBarrier:     func(context.Context) (*node.ReadBarrierInfo, error) { return nil, nil },
	}

	ctx, profile := query.WithProfile(context.Background())

	got, err := routed.ListAccounts(ctx, "ledger", 10, "after", filter, true)
	require.NoError(t, err)

	accounts, err := cursor.Collect(got)
	require.NoError(t, err)
	require.Len(t, accounts, 1)
	assert.Equal(t, "acc:leader", accounts[0].GetAddress())
	assert.True(t, profile.Forwarded)
}

// TestRoutedController_InspectIndex_ForwardsLocalBuildingRefusal pins the same
// contract on InspectIndex — the only non-compile producer of INDEX_BUILDING —
// which routes through retryOnStaleBinding like the list reads.
func TestRoutedController_InspectIndex_ForwardsLocalBuildingRefusal(t *testing.T) {
	t.Parallel()

	building := &domain.BusinessError{Err: &domain.ErrIndexBuilding{Index: `metadata["tier"]`}}
	req := &servicepb.InspectIndexRequest{Ledger: "ledger", MetadataKey: "tier"}
	leaderResp := &servicepb.InspectIndexResponse{}

	mockCtrl := gomock.NewController(t)
	local := ctrlmock.NewMockController(mockCtrl)
	remote := ctrlmock.NewMockController(mockCtrl)

	local.EXPECT().InspectIndex(gomock.Any(), req).Return(nil, building).Times(1)
	remote.EXPECT().InspectIndex(gomock.Any(), req).Return(leaderResp, nil).Times(1)

	routed := &RoutedController{
		Node:            &node.Node{},
		localController: local,
		leaderResolver:  func() (ctrl.Controller, error) { return remote, nil },
		readBarrier:     func(context.Context) (*node.ReadBarrierInfo, error) { return nil, nil },
	}

	ctx, profile := query.WithProfile(context.Background())

	got, err := routed.InspectIndex(ctx, req)
	require.NoError(t, err)
	assert.Same(t, leaderResp, got)
	assert.True(t, profile.Forwarded)
}

// TestRoutedController_InspectIndex_StaleRefusalNotForwarded: under
// explicitly-stale consistency InspectIndex keeps this node's refusal.
func TestRoutedController_InspectIndex_StaleRefusalNotForwarded(t *testing.T) {
	t.Parallel()

	building := &domain.BusinessError{Err: &domain.ErrIndexBuilding{Index: `metadata["tier"]`}}
	req := &servicepb.InspectIndexRequest{Ledger: "ledger", MetadataKey: "tier"}

	mockCtrl := gomock.NewController(t)
	local := ctrlmock.NewMockController(mockCtrl)
	local.EXPECT().InspectIndex(gomock.Any(), req).Return(nil, building).Times(1)

	routed := &RoutedController{Node: &node.Node{}, localController: local, leaderResolver: func() (ctrl.Controller, error) {
		t.Error("a stale read must not resolve a forward target")

		return nil, commonpb.ErrNoLeader
	}}

	ctx, profile := query.WithProfile(context.Background())
	ctx = grpcadp.WithConsistency(ctx, grpcadp.ConsistencyStale)

	_, err := routed.InspectIndex(ctx, req)
	require.ErrorIs(t, err, error(building))
	assert.False(t, profile.Forwarded)
}

// TestRoutedController_ListAccounts_StaleRefusalNotForwarded drives a real
// wrapped read method end to end: under explicitly-stale consistency the local
// INDEX_BUILDING refusal reaches the caller unretried.
func TestRoutedController_ListAccounts_StaleRefusalNotForwarded(t *testing.T) {
	t.Parallel()

	building := &domain.BusinessError{Err: &domain.ErrIndexBuilding{Index: `metadata["tier"]`}}

	mockCtrl := gomock.NewController(t)
	local := ctrlmock.NewMockController(mockCtrl)
	local.EXPECT().
		ListAccounts(gomock.Any(), "ledger", uint32(10), "", nil, false).
		Return(nil, building)

	routed := &RoutedController{Node: &node.Node{}, localController: local, leaderResolver: func() (ctrl.Controller, error) {
		t.Error("a stale read must not resolve a forward target")

		return nil, commonpb.ErrNoLeader
	}}

	ctx, profile := query.WithProfile(context.Background())
	ctx = grpcadp.WithConsistency(ctx, grpcadp.ConsistencyStale)

	_, err := routed.ListAccounts(ctx, "ledger", 10, "", nil, false)
	require.ErrorIs(t, err, error(building))
	assert.False(t, profile.Forwarded)
}
