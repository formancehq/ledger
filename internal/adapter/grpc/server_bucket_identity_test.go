package grpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type bucketMetricsIdentityServer struct {
	servicepb.UnimplementedBucketServiceServer

	id        uint64
	primary   chan uint64
	secondary chan uint64
}

func (s *bucketMetricsIdentityServer) GetPrimaryMetrics(_ context.Context, req *servicepb.GetPrimaryMetricsRequest) (*servicepb.GetPrimaryMetricsResponse, error) {
	s.primary <- req.GetNodeId()

	return &servicepb.GetPrimaryMetricsResponse{Available: true, Metrics: &servicepb.PebbleMetrics{DiskSpaceUsage: s.id}}, nil
}

func (s *bucketMetricsIdentityServer) GetSecondaryMetrics(_ context.Context, req *servicepb.GetSecondaryMetricsRequest) (*servicepb.GetSecondaryMetricsResponse, error) {
	s.secondary <- req.GetNodeId()

	return &servicepb.GetSecondaryMetricsResponse{Available: true, Metrics: &servicepb.PebbleMetrics{DiskSpaceUsage: s.id}}, nil
}

func TestStoreMetricsRouteFullNodeID(t *testing.T) {
	t.Parallel()
	pool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	peers := make([]*bucketMetricsIdentityServer, 0, 3)
	for _, id := range []uint64{1, 1<<32 + 1, ^uint64(0)} {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		require.NoError(t, err)
		server := ggrpc.NewServer()
		peer := &bucketMetricsIdentityServer{id: id, primary: make(chan uint64, 1), secondary: make(chan uint64, 1)}
		servicepb.RegisterBucketServiceServer(server, peer)
		done := make(chan error, 1)
		go func() { done <- server.Serve(listener) }()
		t.Cleanup(func() { server.Stop(); require.NoError(t, <-done) })
		require.NoError(t, pool.AddPeer(id, listener.Addr().String()))
		peers = append(peers, peer)
	}
	impl := &BucketServiceServerImpl{forwarder: nodeForwarder{node: &node.Node{}, servicePool: pool}}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server := ggrpc.NewServer()
	servicepb.RegisterBucketServiceServer(server, impl)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); require.NoError(t, <-done) })
	conn, err := ggrpc.NewClient(listener.Addr().String(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := servicepb.NewBucketServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, peer := range peers {
		primary, err := client.GetPrimaryMetrics(ctx, &servicepb.GetPrimaryMetricsRequest{NodeId: peer.id})
		require.NoError(t, err)
		require.True(t, primary.GetAvailable())
		require.Equal(t, peer.id, primary.GetMetrics().GetDiskSpaceUsage())
		require.Len(t, peer.primary, 1)
		require.Equal(t, peer.id, <-peer.primary)
		secondary, err := client.GetSecondaryMetrics(ctx, &servicepb.GetSecondaryMetricsRequest{NodeId: peer.id})
		require.NoError(t, err)
		require.True(t, secondary.GetAvailable())
		require.Equal(t, peer.id, secondary.GetMetrics().GetDiskSpaceUsage())
		require.Len(t, peer.secondary, 1)
		require.Equal(t, peer.id, <-peer.secondary)
	}
	// This unknown high ID collides with the configured low-ID peer if narrowed.
	_, err = impl.GetPrimaryMetrics(ctx, &servicepb.GetPrimaryMetricsRequest{NodeId: 2<<32 + 1})
	require.ErrorIs(t, err, ErrNodeNotReachable)
	_, err = impl.GetSecondaryMetrics(ctx, &servicepb.GetSecondaryMetricsRequest{NodeId: 2<<32 + 1})
	require.ErrorIs(t, err, ErrNodeNotReachable)
	// Zero still selects the local secondary store (unavailable in this fixture).
	local, err := client.GetSecondaryMetrics(ctx, &servicepb.GetSecondaryMetricsRequest{})
	require.NoError(t, err)
	require.False(t, local.GetAvailable())
	for _, peer := range peers {
		require.Empty(t, peer.primary)
		require.Empty(t, peer.secondary)
	}
}
