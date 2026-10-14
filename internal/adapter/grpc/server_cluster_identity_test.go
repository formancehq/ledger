package grpc

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/application/indexbuilder"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/pkg/signal"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/spool"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

type identityPeerServer struct {
	clusterpb.UnimplementedClusterServiceServer

	id       uint64
	requests chan uint64
}

func (s *identityPeerServer) GetClusterState(_ context.Context, req *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	s.requests <- req.GetNodeId()

	return &clusterpb.ClusterState{NodeVersion: fmt.Sprintf("node-%d", s.id),
		SyncProgress:  &clusterpb.SyncProgress{Status: fmt.Sprintf("sync-%d", s.id)},
		IndexProgress: &clusterpb.IndexProgress{LastIndexedSequence: s.id},
	}, nil
}

func serveIdentityRPC(t *testing.T, handler clusterpb.ClusterServiceServer) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server := ggrpc.NewServer()
	clusterpb.RegisterClusterServiceServer(server, handler)
	go func() { _ = server.Serve(listener) }() // Stop closes the owned listener.
	t.Cleanup(server.Stop)

	return listener.Addr().String()
}

func identityRPCClient(t *testing.T, handler clusterpb.ClusterServiceServer) clusterpb.ClusterServiceClient {
	t.Helper()
	conn, err := ggrpc.NewClient(serveIdentityRPC(t, handler), ggrpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return clusterpb.NewClusterServiceClient(conn)
}

func TestGetClusterStateRoutesDistinctUint64Identities(t *testing.T) {
	t.Parallel()
	pool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	peers := make(map[uint64]*identityPeerServer)
	for _, id := range []uint64{1, 4294967297, 4294967296, ^uint64(0)} {
		peer := &identityPeerServer{id: id, requests: make(chan uint64, 1)}
		peers[id] = peer
		require.NoError(t, pool.AddPeer(id, serveIdentityRPC(t, peer)))
	}
	local := &node.Node{}
	client := identityRPCClient(t, &ClusterServiceServerImpl{
		node: local, servicePool: pool, logger: testLogger(),
		forwarder: nodeForwarder{node: local, servicePool: pool},
	})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := client.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
	require.ErrorContains(t, err, "no leader")
	for _, id := range []uint64{1, 4294967297, 4294967296, ^uint64(0)} {
		t.Run(strconv.FormatUint(id, 10), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			// Decode the full-width wire value so this regression also detects an
			// accidentally narrowed generated request contract.
			wire := protowire.AppendTag(nil, 1, protowire.VarintType)
			wire = protowire.AppendVarint(wire, id)
			request := &clusterpb.GetClusterStateRequest{}
			require.NoError(t, proto.Unmarshal(wire, request))
			response, err := client.GetClusterState(ctx, request)
			require.NoError(t, err)
			require.Equal(t, fmt.Sprintf("node-%d", id), response.GetNodeVersion())
			select {
			case captured := <-peers[id].requests:
				require.Equal(t, id, captured)
			case <-ctx.Done():
				t.Fatal("request did not reach the exact target identity")
			}
		})
	}
}

// A single real voter can report learner topology without requiring remote
// replicas to acknowledge entries. The peer services remain independent RPC
// endpoints, so address/progress attribution crosses the production boundary.
func newIdentityClusterServer(t *testing.T, localID uint64, peers map[uint64]*identityPeerServer) *ClusterServiceServerImpl {
	t.Helper()
	logger := testLogger()
	meters := noop.NewMeterProvider()
	meter := meters.Meter("cluster-identity-test")
	store, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	w, err := wal.New(t.TempDir(), logger, meter)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	learners := make([]uint64, 0, len(peers))
	for id := range peers {
		learners = append(learners, id)
	}
	require.NoError(t, w.CreateSnapshot(0, &raftpb.ConfState{Voters: []uint64{localID}, Learners: learners}, nil))
	sp, err := spool.NewDefault(spool.DefaultSpoolConfig{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sp.Close()) })
	pool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	raftPool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, raftPool.Close()) })
	raftTransport := node.NewTransport(logger, raftPool, meters, localID, node.TransportConfig{
		Reception: []int{16, 16, 16}, Send: []int{16, 16, 16},
	}, "cluster-identity-test", 1024, "self:7777", "self:8888")
	transportDone := make(chan struct{})
	go func() { defer close(transportDone); raftTransport.Start(context.Background()) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, raftTransport.Stop(ctx))
		select {
		case <-transportDone:
		case <-ctx.Done():
			t.Error("identity transport did not stop")
		}
	})
	member, err := membership.NewMembership(membership.NewPeerStore(store), raftTransport, pool,
		localID, "self:7777", "self:8888", []byte("self-instance-id"), logger)
	require.NoError(t, err)
	for id, peer := range peers {
		address := serveIdentityRPC(t, peer)
		require.NoError(t, member.Register(id, address, address, []byte("peer-instance-id")))
	}
	c, err := cache.New(1000, meter)
	require.NoError(t, err)
	registry := state.NewStateRegistry(c, attributes.New())
	shared := state.NewSharedState()
	machine, err := state.NewMachine(logger, registry, state.NewCacheSnapshotter(logger, registry, nil),
		store, dal.NewSentinelFactory(store, false), meters, nil, shared, signal.NewNotifications(), nil,
		"cluster-identity-test", 0, member.WriteConfChange)
	require.NoError(t, err)
	t.Cleanup(machine.Close)
	recovery := state.NewRecovery(machine, store)
	require.NoError(t, recovery.RecoverState())
	synchronizer := state.NewSynchronizer(machine, recovery, dal.NewIncomingRestoreFactory(store))
	responses := node.NewLocalResponses()
	applier, err := node.NewApplier(machine, recovery, synchronizer, sp, store, w, logger, meter,
		0, 1000, nil, member.OnSnapshotInstalled, responses)
	require.NoError(t, err)
	n, err := node.NewNode(node.NodeConfig{
		NodeID: localID, AdvertiseAddr: "self:7777", ServiceAdvertiseAddr: "self:8888",
		InstanceID: []byte("self-instance-id"), TickInterval: 100 * time.Millisecond,
		ElectionTick: 15, HeartbeatTick: 10,
		ProcessingTickInterval: time.Millisecond, MaintenanceInterval: time.Hour,
	}, raftTransport, applier, logger, meter, w, machine, recovery, synchronizer, member, responses)
	require.NoError(t, err)
	member.Start()
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- n.Run(context.Background(), ready) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, n.Stop(ctx))
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-ctx.Done():
			t.Error("identity node did not stop before its stores closed")
		}
	})
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("identity node did not become ready")
	}
	require.Eventually(t, n.IsLeader, 5*time.Second, time.Millisecond)
	builder := indexbuilder.NewBuilder(store, nil, attributes.New(), logger, meter, 0)

	return NewClusterServiceServer(n, raftTransport, pool, nil, store, c, shared, builder,
		nil, nil, nil, logger, "self:7777", "self:8888", "cluster-identity-test", version.Info{Version: "local-version"}).(*ClusterServiceServerImpl)
}

func TestGetClusterStateEnrichesExactUint64PeerIdentities(t *testing.T) {
	t.Parallel()
	peers := map[uint64]*identityPeerServer{
		4294967297: {id: 4294967297, requests: make(chan uint64, 1)},
		4294967296: {id: 4294967296, requests: make(chan uint64, 1)},
	}
	impl := newIdentityClusterServer(t, 1, peers)
	client := identityRPCClient(t, impl)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	response, err := client.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{NodeId: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), response.GetLocalNode())
	require.Equal(t, uint64(1), response.GetLeader())
	nodes := make(map[uint64]*clusterpb.NodeInfo)
	for _, info := range response.GetNodes() {
		nodes[info.GetId()] = info
	}
	require.Len(t, nodes, 3)
	local := nodes[1]
	require.NotNil(t, local)
	require.Equal(t, "self:7777", local.GetRaftAddress())
	require.Equal(t, "self:8888", local.GetServiceAddress())
	require.Equal(t, "local-version", local.GetVersion())
	require.Equal(t, response.GetSyncProgress(), local.GetSyncProgress())
	require.Equal(t, response.GetIndexProgress(), local.GetIndexProgress())
	for id, peer := range peers {
		info := nodes[id]
		require.NotNil(t, info)
		require.Equal(t, impl.servicePool.GetPeerAddress(id), info.GetServiceAddress())
		require.Equal(t, impl.raftTransport.GetPeerAddress(id), info.GetRaftAddress())
		require.NotEqual(t, local.GetServiceAddress(), info.GetServiceAddress())
		require.Equal(t, fmt.Sprintf("node-%d", id), info.GetVersion())
		require.Equal(t, fmt.Sprintf("sync-%d", id), info.GetSyncProgress().GetStatus())
		require.Equal(t, id, info.GetIndexProgress().GetLastIndexedSequence())
		require.True(t, info.GetProgress().GetIsLearner())
		select {
		case captured := <-peer.requests:
			require.Equal(t, id, captured)
		case <-ctx.Done():
			t.Fatal("peer enrichment did not query the exact identity")
		}
	}
}

func TestTransferLeadershipPreservesHighLocalIdentity(t *testing.T) {
	t.Parallel()
	impl := newIdentityClusterServer(t, 4294967296, nil)
	client := identityRPCClient(t, impl)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err := client.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{})
	require.ErrorContains(t, err, "transferee node ID must be non-zero")
	response, err := client.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{Transferee: 4294967296})
	require.NoError(t, err)
	require.Equal(t, uint64(4294967296), response.GetNewLeader())
	local, err := client.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{NodeId: 4294967296})
	require.NoError(t, err)
	require.Equal(t, uint64(4294967296), local.GetLocalNode())
	require.Equal(t, "local-version", local.GetNodeVersion())
	leader, err := client.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
	require.NoError(t, err)
	require.Equal(t, uint64(4294967296), leader.GetLeader())
	require.Equal(t, uint64(4294967296), leader.GetLocalNode())
}
