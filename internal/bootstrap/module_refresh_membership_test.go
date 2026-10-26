package bootstrap

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/clusterbootstrappb"
)

func TestIsRetryableMembershipRefreshError(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		code codes.Code
		want bool
	}{
		{name: "unavailable", code: codes.Unavailable, want: true},
		{name: "old peer missing rpc", code: codes.Unimplemented, want: true},
		{name: "invalid request", code: codes.InvalidArgument, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, isRetryableMembershipRefreshError(status.Error(tc.code, "refresh failed")))
		})
	}
}

type retryingRefreshServer struct {
	clusterbootstrappb.UnimplementedClusterBootstrapServiceServer
	attempts atomic.Int32
	firstErr error
}

func (s *retryingRefreshServer) RefreshMembership(context.Context, *clusterbootstrappb.RefreshMembershipRequest) (*clusterbootstrappb.RefreshMembershipResponse, error) {
	if s.attempts.Add(1) == 1 {
		return nil, s.firstErr
	}

	return &clusterbootstrappb.RefreshMembershipResponse{}, nil
}

func serveRefreshMembership(t *testing.T, server clusterbootstrappb.ClusterBootstrapServiceServer) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	clusterbootstrappb.RegisterClusterBootstrapServiceServer(srv, server)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String()
}

func refreshTestConfig(t *testing.T, peerAddress string) Config {
	t.Helper()

	return Config{
		ClusterID: "test-cluster",
		TLSConfig: TLSConfig{Mode: TLSModeDisabled},
		RaftConfig: node.NodeConfig{
			NodeID:        2,
			WalDir:        t.TempDir(),
			AdvertiseAddr: "127.0.0.1:7777",
			Peers:         []node.Peer{{ID: 1, Address: peerAddress}},
		},
	}
}

func TestTryRefreshMembershipRetriesUnimplementedUntilPeerSupportsRPC(t *testing.T) {
	t.Parallel()

	peer := &retryingRefreshServer{firstErr: status.Error(codes.Unimplemented, "old peer")}
	cfg := refreshTestConfig(t, serveRefreshMembership(t, peer))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, tryRefreshMembership(ctx, cfg, cfg.TLSConfig, logging.Testing()))
	require.Equal(t, int32(2), peer.attempts.Load())
}

func TestTryRefreshMembershipStopsRetryingWhenContextIsCanceled(t *testing.T) {
	t.Parallel()

	peer := &retryingRefreshServer{firstErr: status.Error(codes.Unavailable, "leader unavailable")}
	cfg := refreshTestConfig(t, serveRefreshMembership(t, peer))

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- tryRefreshMembership(ctx, cfg, cfg.TLSConfig, logging.Testing()) }()
	require.Eventually(t, func() bool { return peer.attempts.Load() > 0 }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
}
