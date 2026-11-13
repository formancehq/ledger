package bootstrap

import (
	"context"
	"errors"
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
		{name: "remote deadline", code: codes.DeadlineExceeded, want: true},
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
	failures int32
}

type blockingRefreshServer struct {
	clusterbootstrappb.UnimplementedClusterBootstrapServiceServer
}

type capturingRefreshServer struct {
	clusterbootstrappb.UnimplementedClusterBootstrapServiceServer

	requests chan *clusterbootstrappb.RefreshMembershipRequest
}

func (s *capturingRefreshServer) RefreshMembership(_ context.Context, req *clusterbootstrappb.RefreshMembershipRequest) (*clusterbootstrappb.RefreshMembershipResponse, error) {
	s.requests <- req

	return &clusterbootstrappb.RefreshMembershipResponse{}, nil
}

func (blockingRefreshServer) RefreshMembership(ctx context.Context, _ *clusterbootstrappb.RefreshMembershipRequest) (*clusterbootstrappb.RefreshMembershipResponse, error) {
	<-ctx.Done()

	return nil, ctx.Err()
}

func (s *retryingRefreshServer) RefreshMembership(context.Context, *clusterbootstrappb.RefreshMembershipRequest) (*clusterbootstrappb.RefreshMembershipResponse, error) {
	if s.attempts.Add(1) <= max(1, s.failures) {
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

func TestTryRefreshMembershipAdvertisesPeerServicePort(t *testing.T) {
	t.Parallel()

	peer := &capturingRefreshServer{requests: make(chan *clusterbootstrappb.RefreshMembershipRequest, 1)}
	cfg := refreshTestConfig(t, serveRefreshMembership(t, peer))
	cfg.GRPCPort = 8888
	cfg.GRPCPeerPort = 9999

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, tryRefreshMembershipOnce(ctx, cfg, cfg.TLSConfig, logging.Testing()))
	require.Equal(t, "127.0.0.1:9999", (<-peer.requests).GetServiceAddress())
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

func TestTryRefreshMembershipRetriesRemoteDeadlinesUntilSuccess(t *testing.T) {
	t.Parallel()

	peer := &retryingRefreshServer{firstErr: status.Error(codes.DeadlineExceeded, "leader proposal timed out"), failures: 2}
	cfg := refreshTestConfig(t, serveRefreshMembership(t, peer))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	require.NoError(t, tryRefreshMembership(ctx, cfg, cfg.TLSConfig, logging.Testing()))
	require.Equal(t, int32(3), peer.attempts.Load())
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

func TestTryRefreshMembershipPropagatesPermanentRejection(t *testing.T) {
	t.Parallel()

	peer := &retryingRefreshServer{firstErr: status.Error(codes.FailedPrecondition, "removed incarnation")}
	cfg := refreshTestConfig(t, serveRefreshMembership(t, peer))

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	t.Cleanup(cancel)
	err := tryRefreshMembership(ctx, cfg, cfg.TLSConfig, logging.Testing())
	require.ErrorContains(t, err, "removed incarnation")
	require.Equal(t, int32(1), peer.attempts.Load(), "permanent rejection must not be retried")
}

func TestTryRefreshMembershipForStartupTreatsBoundedAttemptAsTransient(t *testing.T) {
	t.Parallel()

	cfg := refreshTestConfig(t, serveRefreshMembership(t, blockingRefreshServer{}))
	err := tryRefreshMembershipForStartupWithTimeout(
		context.Background(),
		cfg,
		cfg.TLSConfig,
		logging.Testing(),
		10*time.Millisecond,
	)

	require.ErrorIs(t, err, errMembershipRefreshTransient)
	require.True(t, errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded)
}

func TestTryRefreshMembershipForStartupTreatsEarlyRPCDeadlineAsTransient(t *testing.T) {
	t.Parallel()

	peer := &retryingRefreshServer{firstErr: status.Error(codes.DeadlineExceeded, "leader proposal timed out")}
	cfg := refreshTestConfig(t, serveRefreshMembership(t, peer))
	err := tryRefreshMembershipForStartupWithTimeout(
		context.Background(), cfg, cfg.TLSConfig, logging.Testing(), time.Second,
	)

	require.ErrorIs(t, err, errMembershipRefreshTransient)
	require.Equal(t, int32(1), peer.attempts.Load())
}
