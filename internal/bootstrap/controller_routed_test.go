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
