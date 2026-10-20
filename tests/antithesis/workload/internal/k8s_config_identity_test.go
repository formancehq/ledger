package internal

import (
	"context"
	"math"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Exercise the generated client while emulating legacy and full-width peers.
type configIdentityConn struct {
	grpc.ClientConnInterface

	t      *testing.T
	target uint64
	state  *clusterpb.ClusterState
}

func (c configIdentityConn) Invoke(_ context.Context, method string, request, response any, _ ...grpc.CallOption) error {
	require.Equal(c.t, clusterpb.ClusterService_GetClusterState_FullMethodName, method)
	require.True(c.t, proto.Equal(clusterpb.NewStateRequest(c.target), request.(*clusterpb.GetClusterStateRequest)))
	proto.Merge(response.(*clusterpb.ClusterState), c.state)

	return nil
}

func TestClusterConfigValidatesTargetBeforeConvergence(t *testing.T) {
	t.Parallel()
	const highID = uint64(math.MaxUint32) + 2
	for _, tc := range []struct {
		name      string
		target    uint64
		local     uint64
		fullWidth bool
		wantReady bool
	}{
		{name: "legacy leader cannot qualify high target", target: highID, local: 1},
		{name: "wrong full-width node cannot qualify high target", target: highID, local: 1, fullWidth: true},
		{name: "exact full-width node qualifies", target: highID, local: highID, fullWidth: true, wantReady: true},
		{name: "legacy small node qualifies", target: 1, local: 1, wantReady: true},
		{name: "legacy leader routing qualifies", target: 0, local: 1, wantReady: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				state := &clusterpb.ClusterState{
					LocalNode:     clusterpb.LegacyNodeID(tc.local),
					ClusterConfig: &commonpb.ClusterConfig{},
				}
				if tc.fullWidth {
					state.LocalNode64 = new(tc.local)
				}
				client := clusterpb.NewClusterServiceClient(configIdentityConn{t: t, target: tc.target, state: state})
				cfg, err := GetClusterConfig(t.Context(), client, tc.target)
				if tc.wantReady {
					require.NoError(t, err)
					require.True(t, proto.Equal(state.GetClusterConfig(), cfg))
				} else {
					require.Equal(t, codes.FailedPrecondition, status.Code(err))
					require.Nil(t, cfg, "an unverified node's config must not escape")
				}
				predicateCalled := false
				ready := WaitForClusterConfigOnNode(t.Context(), client, tc.target, func(*commonpb.ClusterConfig) bool {
					predicateCalled = true

					return true
				}, 2*time.Second)
				require.Equal(t, tc.wantReady, ready)
				require.Equal(t, tc.wantReady, predicateCalled, "an unverified response must not reach the convergence predicate")
			})
		})
	}
}
