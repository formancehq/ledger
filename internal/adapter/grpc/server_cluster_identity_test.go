package grpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
)

type clusterIdentityServer struct {
	clusterpb.UnimplementedClusterServiceServer

	requests chan uint64
	state    *clusterpb.ClusterState
}

func (s *clusterIdentityServer) GetClusterState(_ context.Context, req *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	s.requests <- req.GetNodeId()

	return s.state, nil
}

func startClusterIdentityServer(t *testing.T, pool *transport.ConnectionPool, id uint64, state *clusterpb.ClusterState) *clusterIdentityServer {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server := ggrpc.NewServer()
	impl := &clusterIdentityServer{requests: make(chan uint64, 2), state: state}
	clusterpb.RegisterClusterServiceServer(server, impl)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-done)
	})
	require.NoError(t, pool.AddPeer(id, listener.Addr().String()))

	return impl
}

func TestFetchPeerStatePreservesFullNodeID(t *testing.T) {
	t.Parallel()
	pool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	const highID uint64 = 1<<32 + 1
	lowState := &clusterpb.ClusterState{NodeVersion: "low", SyncProgress: &clusterpb.SyncProgress{BytesReceived: 111}, IndexProgress: &clusterpb.IndexProgress{LastIndexedSequence: 11}}
	highState := &clusterpb.ClusterState{NodeVersion: "high", SyncProgress: &clusterpb.SyncProgress{BytesReceived: 222}, IndexProgress: &clusterpb.IndexProgress{LastIndexedSequence: 22}}
	low := startClusterIdentityServer(t, pool, 1, lowState)
	high := startClusterIdentityServer(t, pool, highID, highState)
	impl := &ClusterServiceServerImpl{servicePool: pool}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	require.Equal(t, "low", impl.fetchPeerState(ctx, 1).GetNodeVersion())
	require.Len(t, low.requests, 1)
	require.Equal(t, uint64(1), <-low.requests)
	response := impl.fetchPeerState(ctx, highID)
	require.NotNil(t, response)
	require.Len(t, high.requests, 1)
	require.Equal(t, highID, <-high.requests, "the peer must receive its exact identity, not the colliding low bits")
	require.Equal(t, "high", response.GetNodeVersion())
	require.Equal(t, uint64(222), response.GetSyncProgress().GetBytesReceived())
	require.Equal(t, uint64(22), response.GetIndexProgress().GetLastIndexedSequence())
	require.Empty(t, low.requests, "the low-ID peer must not receive the high-ID request")
}

func TestGetClusterStateWithoutLeader(t *testing.T) {
	t.Parallel()
	impl := &ClusterServiceServerImpl{node: &node.Node{}, logger: testLogger()}
	response, err := impl.GetClusterState(context.Background(), &clusterpb.GetClusterStateRequest{})
	require.Nil(t, response)
	require.ErrorIs(t, err, ledgerpb.ErrNoLeader)
}

func TestGetClusterStateRoutesFullNodeID(t *testing.T) {
	t.Parallel()
	pool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	const highID uint64 = 1<<32 + 1
	low := startClusterIdentityServer(t, pool, 1, &clusterpb.ClusterState{NodeVersion: "low"})
	high := startClusterIdentityServer(t, pool, highID, &clusterpb.ClusterState{NodeVersion: "high"})
	// Explicit target requests only need the node's identity to distinguish local
	// handling from forwarding; the Raft loop is not involved in this branch.
	localNode := &node.Node{}
	impl := &ClusterServiceServerImpl{forwarder: nodeForwarder{node: localNode, servicePool: pool}}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server := ggrpc.NewServer()
	clusterpb.RegisterClusterServiceServer(server, impl)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-done)
	})
	conn, err := ggrpc.NewClient(listener.Addr().String(), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := clusterpb.NewClusterServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tt := range []struct {
		id      uint64
		version string
		target  *clusterIdentityServer
	}{
		{1, "low", low}, {highID, "high", high},
	} {
		response, err := client.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{NodeId: tt.id})
		require.NoError(t, err)
		require.Equal(t, tt.version, response.GetNodeVersion())
		require.Len(t, tt.target.requests, 1)
		require.Equal(t, tt.id, <-tt.target.requests)
	}
	require.Empty(t, low.requests)
	require.Empty(t, high.requests)
}

func TestTransferLeadershipRejectsZeroAndUnknownLeader(t *testing.T) {
	t.Parallel()
	impl := &ClusterServiceServerImpl{node: &node.Node{}}
	response, err := impl.TransferLeadership(context.Background(), &clusterpb.TransferLeadershipRequest{})
	require.Nil(t, response)
	require.EqualError(t, err, "transferee node ID must be non-zero")
	response, err = impl.TransferLeadership(context.Background(), &clusterpb.TransferLeadershipRequest{Transferee: 1<<32 + 1})
	require.Nil(t, response)
	require.ErrorIs(t, err, ledgerpb.ErrNoLeader)
}
