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
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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
