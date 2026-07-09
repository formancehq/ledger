package server

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/bootstrap"
	"github.com/formancehq/ledger/v3/internal/proto/clusterbootstrappb"
)

// unauthClusterBootstrapServer is a ClusterBootstrapService whose every RPC
// rejects the caller with codes.Unauthenticated, mirroring what the real
// RaftServer's cluster-secret interceptor does when a joining node presents a
// missing or wrong secret.
type unauthClusterBootstrapServer struct {
	clusterbootstrappb.UnimplementedClusterBootstrapServiceServer
}

func (unauthClusterBootstrapServer) GetPeers(context.Context, *clusterbootstrappb.GetPeersRequest) (*clusterbootstrappb.GetPeersResponse, error) {
	return nil, status.Error(codes.Unauthenticated, "missing authorization metadata on Raft RPC")
}

// TestDiscoverPeers_FailsFastOnUnauthenticated pins EN-1080: peer discovery
// must NOT retry a cluster-secret mismatch until the context deadline (which
// would surface an opaque "context deadline exceeded"). It must abort
// immediately with a typed, actionable JoinAuthError.
func TestDiscoverPeers_FailsFastOnUnauthenticated(t *testing.T) {
	t.Parallel()

	// Real plaintext loopback server rejecting every RPC with Unauthenticated.
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	clusterbootstrappb.RegisterClusterBootstrapServiceServer(srv, unauthClusterBootstrapServer{})

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	// Generous deadline: if the fail-fast breaks and the loop retries, the
	// call would run until this deadline; a passing test returns almost
	// immediately, far under it.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	start := time.Now()
	_, err = discoverPeersFromClusterWithRetry(
		ctx,
		lis.Addr().String(),
		bootstrap.TLSConfig{Mode: bootstrap.TLSModeDisabled},
		"test-cluster",
		"", // no cluster-secret on this joining node
	)
	elapsed := time.Since(start)

	require.Error(t, err)

	var joinErr *bootstrap.JoinAuthError
	require.True(t, errors.As(err, &joinErr),
		"Unauthenticated during discovery must surface as *bootstrap.JoinAuthError, got %T: %v", err, err)
	require.False(t, joinErr.HasSecret)
	require.Contains(t, err.Error(), "inter-node authentication failed")
	require.Contains(t, err.Error(), "set --cluster-secret")

	// Detail must carry the clean, unwrapped status message — not the
	// wrapped chain. Wrapping GetPeers' error before status.FromError would
	// leak "getting peers from <addr>: rpc error: code = Unauthenticated
	// desc = …" here, duplicating the address and re-exposing the gRPC noise
	// this fail-fast exists to hide (EN-1080 review finding).
	require.Equal(t, "missing authorization metadata on Raft RPC", joinErr.Detail)
	require.NotContains(t, joinErr.Detail, "rpc error:")
	require.NotContains(t, joinErr.Detail, "getting peers from")
	require.NotContains(t, joinErr.Detail, lis.Addr().String())

	// Must fail fast, not retry until the deadline.
	require.Less(t, elapsed, 5*time.Second,
		"discovery must abort immediately on Unauthenticated, not retry until the deadline")
}
