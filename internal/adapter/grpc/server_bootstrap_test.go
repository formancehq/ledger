package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/clusterbootstrappb"
)

// Cluster-id gate is the only piece of behaviour exercised here.
// Request-validation and membership invariants live in the application
// membership package and are covered by membership_test.go.
// End-to-end behaviour (peer listing, leader forwarding, ConfChange)
// lives in tests/e2e/cluster.

func TestClusterBootstrapServiceServer_RejectsWrongClusterID(t *testing.T) {
	t.Parallel()

	impl := &ClusterBootstrapServiceServerImpl{
		clusterID: "expected-cluster",
		logger:    noopLogger{},
	}

	ctx := metadata.NewIncomingContext(context.Background(),
		metadata.Pairs(node.MetadataKeyClusterID, "wrong-cluster"))

	t.Run("GetPeers", func(t *testing.T) {
		t.Parallel()
		_, err := impl.GetPeers(ctx, &clusterbootstrappb.GetPeersRequest{})
		require.Error(t, err)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	})

	t.Run("JoinAsLearner", func(t *testing.T) {
		t.Parallel()
		_, err := impl.JoinAsLearner(ctx, &clusterbootstrappb.JoinAsLearnerRequest{
			NodeId:         2,
			RaftAddress:    "r:1",
			ServiceAddress: "s:1",
		})
		require.Error(t, err)
		assert.Equal(t, codes.PermissionDenied, status.Code(err))
	})
}

func TestClusterBootstrapServiceServer_RejectsMissingClusterID(t *testing.T) {
	t.Parallel()

	impl := &ClusterBootstrapServiceServerImpl{
		clusterID: "expected-cluster",
		logger:    noopLogger{},
	}

	// Empty incoming metadata — same outcome as a wrong value: rejected.
	_, err := impl.GetPeers(context.Background(), &clusterbootstrappb.GetPeersRequest{})
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}

// Statuses produced by the leader must reach the joining node unchanged:
// its fatal classes (stale progress, removed member, auth) depend on it.
func TestForwardedCallError_PassesLeaderStatusesThrough(t *testing.T) {
	t.Parallel()

	stale, err := status.New(codes.FailedPrecondition, "stale").WithDetails(&errdetails.ErrorInfo{
		Reason: node.StaleRaftProgressReason,
		Domain: "ledger",
	})
	require.NoError(t, err)

	for _, leaderErr := range []error{
		stale.Err(),
		status.Error(codes.FailedPrecondition, "node 3 was previously removed from this cluster"),
		status.Error(codes.AlreadyExists, "node already in cluster"),
		status.Error(codes.Unauthenticated, "invalid cluster credentials on Raft RPC"),
		status.Error(codes.Unavailable, "no leader"),
	} {
		require.Same(t, leaderErr, forwardedCallError(leaderErr))
	}
}

func TestForwardedCallError_ForwardingHopFailureIsUnavailable(t *testing.T) {
	t.Parallel()

	for _, hopErr := range []error{
		status.Error(codes.Canceled, "grpc: the client connection is closing"),
		status.Error(codes.DeadlineExceeded, "context deadline exceeded"),
	} {
		assert.Equal(t, codes.Unavailable, status.Code(forwardedCallError(hopErr)))
	}
}
