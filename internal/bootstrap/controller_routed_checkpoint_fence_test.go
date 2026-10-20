package bootstrap

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	grpcadp "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
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
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/rafttransportpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/spool"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

// This complements the harness regression with the real routing and metadata
// handlers. The stores and barrier failures are deterministic fixtures, not a
// replay of the Antithesis timeline or a fault-driven replication experiment.
func TestRoutedController_GetLedgerFallbackDoesNotFenceCheckpointMetadata(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	logger := logging.Testing()
	meters := noop.NewMeterProvider()
	attrs := attributes.New()
	leaderStore, followerStore := newTestStore(t), newTestStore(t)
	seedRoutedCheckpointMetadata(t, leaderStore, 521, 7, "@every 960000h", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT)
	seedRoutedCheckpointMetadata(t, followerStore, 155, 4, "@every 950000h", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT)
	leaderController := ctrl.NewDefaultController(nil, leaderStore, logger, attrs, nil, nil, meters.Meter("test"))
	followerController := ctrl.NewDefaultController(nil, followerStore, logger, attrs, nil, nil, meters.Meter("test"))
	var leaderReads atomic.Int32
	leaderServer := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if info.FullMethod == servicepb.BucketService_GetLedger_FullMethodName {
			leaderReads.Add(1)
		}

		return handler(ctx, req)
	}))
	servicepb.RegisterBucketServiceServer(leaderServer, grpcadp.NewBucketServiceServer(logger, leaderController, leaderController, leaderStore, nil, attrs, nil, nil, 0, "", meters, nil, nil, version.Info{}))
	clusterpb.RegisterClusterServiceServer(leaderServer, checkpointMetadataHandler(leaderStore))
	leaderAddr := serveCheckpointFenceRPC(t, leaderServer)
	pool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	require.NoError(t, pool.AddPeer(1, leaderAddr))
	follower := startCheckpointFenceFollower(t, ctx, followerStore, pool)
	require.Equal(t, uint64(2), follower.GetNodeID())
	require.Equal(t, uint64(1), follower.GetLeader())
	require.False(t, follower.IsLeader())
	followerState, err := follower.GetClusterState(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(2), followerState.GetLocalNode())
	require.Equal(t, uint64(155), followerState.GetRaftStatus().GetLastPersistedIndex())
	require.Empty(t, followerState.GetNodes(), "followers report their local cursor without leader topology")
	routed := NewRoutedController(followerController, follower, pool)
	followerServer := grpc.NewServer()
	servicepb.RegisterBucketServiceServer(followerServer, grpcadp.NewBucketServiceServer(logger, routed, followerController, followerStore, nil, attrs, nil, nil, 0, "", meters, follower, pool, version.Info{}))
	clusterpb.RegisterClusterServiceServer(followerServer, checkpointMetadataHandler(followerStore))
	followerConn := dialCheckpointFenceRPC(t, serveCheckpointFenceRPC(t, followerServer))
	bucket := servicepb.NewBucketServiceClient(followerConn)
	localMetadata := clusterpb.NewClusterServiceClient(followerConn)
	leaderMetadata := clusterpb.NewClusterServiceClient(pool.GetConnection(1))

	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "syncing follower", err: node.ErrNodeSyncing},
		{name: "leadership changed during local barrier", err: node.ErrNotLeader},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var barrierCalls atomic.Int32
			routed.readIndexAndWait = func(context.Context) (*node.ReadBarrierInfo, error) {
				barrierCalls.Add(1)

				return nil, tc.err
			}
			before := leaderReads.Load()
			ledger, err := bucket.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: "L"})
			require.NoError(t, err)
			require.Equal(t, int32(1), barrierCalls.Load(), "the actual routed read must attempt its local barrier")
			require.Equal(t, before+1, leaderReads.Load(), "fallback must cross the real leader gRPC connection")
			require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, ledger.GetDefaultEnforcementMode(), "the response must come from the leader's updated store")
			require.Equal(t, uint64(155), follower.LastPersistedIndex(), "forwarding must not imply local catch-up")

			listed, err := localMetadata.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
			require.NoError(t, err)
			require.Len(t, listed.GetCheckpoints(), 1)
			require.Equal(t, uint64(4), listed.GetCheckpoints()[0].GetCheckpointId(), "metadata still reads the follower's local Pebble")
			schedule, err := localMetadata.GetQueryCheckpointSchedule(ctx, &clusterpb.GetQueryCheckpointScheduleRequest{})
			require.NoError(t, err)
			require.Equal(t, "@every 950000h", schedule.GetCron())
			leaderList, err := leaderMetadata.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
			require.NoError(t, err)
			require.Len(t, leaderList.GetCheckpoints(), 1)
			require.Equal(t, uint64(7), leaderList.GetCheckpoints()[0].GetCheckpointId())
			leaderSchedule, err := leaderMetadata.GetQueryCheckpointSchedule(ctx, &clusterpb.GetQueryCheckpointScheduleRequest{})
			require.NoError(t, err)
			require.Equal(t, "@every 960000h", leaderSchedule.GetCron())
			persisted, err := query.ReadLastAppliedIndex(followerStore)
			require.NoError(t, err)
			require.Equal(t, uint64(155), persisted)
		})
	}
}

// Exercise the actual ClusterService address enrichment and routing, rather than
// supplying a synthetic ClusterState. Seeded stores keep a known progress gap;
// the sole voter elects itself, while learner replication is deliberately not
// wired. This tests the identity contract, not fault-driven Raft catch-up.
func TestClusterService_CheckpointFenceIdentityAndLocalProgress(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	leaderStore, followerStore := newTestStore(t), newTestStore(t)
	seedRoutedCheckpointMetadata(t, leaderStore, 521, 7, "@every 960000h", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT)
	seedRoutedCheckpointMetadata(t, followerStore, 155, 4, "@every 950000h", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT)
	leaderListener, followerListener := checkpointFenceListener(t), checkpointFenceListener(t)
	leaderAddr, followerAddr := leaderListener.Addr().String(), followerListener.Addr().String()
	leaderPool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	followerPool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, leaderPool.Close()) })
	t.Cleanup(func() { require.NoError(t, followerPool.Close()) })
	require.NoError(t, leaderPool.AddPeer(2, followerAddr))
	require.NoError(t, followerPool.AddPeer(1, leaderAddr))
	confState := &raftpb.ConfState{Voters: []uint64{1}, Learners: []uint64{2}}
	leader := startCheckpointFenceNode(t, ctx, leaderStore, leaderPool, 1, 521, leaderAddr, 10*time.Millisecond, confState)
	follower := startCheckpointFenceNode(t, ctx, followerStore, followerPool, 2, 155, followerAddr, time.Hour, confState)
	establishCheckpointFenceLeader(t, ctx, follower)
	require.Eventually(t, leader.node.IsLeader, 5*time.Second, time.Millisecond)

	leaderServer, followerServer := grpc.NewServer(), grpc.NewServer()
	clusterpb.RegisterClusterServiceServer(leaderServer, leader.clusterHandler(leaderStore, leaderPool, leaderAddr))
	clusterpb.RegisterClusterServiceServer(followerServer, follower.clusterHandler(followerStore, followerPool, followerAddr))
	serveCheckpointFenceRPCListener(t, leaderServer, leaderListener)
	serveCheckpointFenceRPCListener(t, followerServer, followerListener)
	pinned := clusterpb.NewClusterServiceClient(dialCheckpointFenceRPC(t, followerAddr))

	// The omitted ID routes from the pinned follower to the real leader. Its
	// topology must map the address we actually dialed back to node 2.
	topology, err := pinned.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
	require.NoError(t, err)
	require.Equal(t, "Leader", topology.GetState())
	require.Equal(t, uint32(1), topology.GetLocalNode())
	require.GreaterOrEqual(t, topology.GetRaftStatus().GetLastPersistedIndex(), uint64(521))
	require.Len(t, topology.GetNodes(), 2)
	var resolvedID uint32
	for _, peer := range topology.GetNodes() {
		switch peer.GetId() {
		case 1:
			require.Equal(t, leaderAddr, peer.GetServiceAddress(), "the local leader advertises its serving endpoint")
		case 2:
			require.Equal(t, followerAddr, peer.GetServiceAddress(), "remote addresses come from the real service pool")
			resolvedID = peer.GetId()
		}
	}
	require.Equal(t, uint32(2), resolvedID)

	// The explicit discovered ID selects the follower's own durable cursor.
	// Reusing the leader response, or requiring topology on this local response,
	// would confuse leader progress with the metadata replica's progress.
	local, err := pinned.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{NodeId: resolvedID})
	require.NoError(t, err)
	require.Equal(t, "Follower", local.GetState())
	require.Equal(t, resolvedID, local.GetLocalNode())
	require.Equal(t, uint64(155), local.GetRaftStatus().GetLastPersistedIndex())
	require.Equal(t, "normal", local.GetSyncProgress().GetStatus())
	require.Empty(t, local.GetNodes())
	require.Greater(t, topology.GetRaftStatus().GetLastPersistedIndex(), local.GetRaftStatus().GetLastPersistedIndex())
}

func checkpointMetadataHandler(store *dal.Store) clusterpb.ClusterServiceServer {
	return grpcadp.NewClusterServiceServer(nil, nil, nil, nil, store, nil, nil, nil, nil, nil, nil, logging.Testing(), "", "", "", version.Info{})
}

func seedRoutedCheckpointMetadata(t *testing.T, store *dal.Store, applied, checkpoint uint64, cron string, mode commonpb.ChartEnforcementMode) {
	t.Helper()
	batch := store.OpenWriteSession()
	t.Cleanup(func() { require.NoError(t, batch.Cancel()) })
	require.NoError(t, state.SaveLedger(batch, "L", &commonpb.LedgerInfo{Name: "L", Id: 1, CreatedAt: &commonpb.Timestamp{Data: 1}, DefaultEnforcementMode: mode}))
	require.NoError(t, state.SaveQueryCheckpoint(batch, &raftcmdpb.QueryCheckpointState{CheckpointId: checkpoint, MaxSequence: applied}))
	require.NoError(t, state.SaveQueryCheckpointSchedule(batch, cron))
	require.NoError(t, state.SetAppliedIndex(batch, applied))
	require.NoError(t, batch.Commit())
}

// Only the follower runs a Raft node in the routing regression. A real incoming
// heartbeat establishes its remote-leader identity; no replication messages
// advance the seeded local FSM.
func startCheckpointFenceFollower(t *testing.T, ctx context.Context, store *dal.Store, pool *transport.ConnectionPool) *node.Node {
	t.Helper()
	fixture := startCheckpointFenceNode(t, ctx, store, pool, 2, 155, "", time.Hour, &raftpb.ConfState{Voters: []uint64{1, 2}})
	establishCheckpointFenceLeader(t, ctx, fixture)

	return fixture.node
}

type checkpointFenceNode struct {
	node          *node.Node
	raftTransport *node.DefaultTransport
	cache         *cache.Cache
	shared        *state.SharedState
	raftAddr      string
}

func (fixture checkpointFenceNode) clusterHandler(store *dal.Store, pool *transport.ConnectionPool, serviceAddr string) clusterpb.ClusterServiceServer {
	// Indexing is not running in this fixture; the real builder exposes its
	// initial progress through the same methods used by the production handler.
	builder := indexbuilder.NewBuilder(store, nil, attributes.New(), logging.Testing(), noop.NewMeterProvider().Meter("checkpoint-fence"), 0)

	return grpcadp.NewClusterServiceServer(fixture.node, fixture.raftTransport, pool, nil, store, fixture.cache, fixture.shared, builder, nil, nil, nil, logging.Testing(), fixture.raftAddr, serviceAddr, "checkpoint-fence", version.Info{})
}

func startCheckpointFenceNode(t *testing.T, ctx context.Context, store *dal.Store, pool *transport.ConnectionPool, id, applied uint64, serviceAddr string, tickInterval time.Duration, confState *raftpb.ConfState) checkpointFenceNode {
	t.Helper()
	logger := logging.Testing()
	meters := noop.NewMeterProvider()
	meter := meters.Meter("checkpoint-fence")
	w, err := wal.New(t.TempDir(), logger, meter)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, w.Close()) })
	require.NoError(t, w.CreateSnapshot(applied, confState, nil))
	require.NoError(t, w.Append(&raftpb.HardState{Term: proto.Uint64(1), Commit: new(applied)}, nil))
	sp, err := spool.NewDefault(spool.DefaultSpoolConfig{Dir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sp.Close()) })
	cacheStore, err := cache.New(1000, meter)
	require.NoError(t, err)
	registry := state.NewStateRegistry(cacheStore, attributes.New())
	shared := state.NewSharedState()
	machine, err := state.NewMachine(logger, registry, state.NewCacheSnapshotter(logger, registry, nil), store, dal.NewSentinelFactory(store, false), meters, nil, shared, signal.NewNotifications(), nil, "checkpoint-fence", 0, func(*raftpb.Entry, *dal.WriteSession) error { return nil })
	require.NoError(t, err)
	t.Cleanup(machine.Close)
	recovery := state.NewRecovery(machine, store)
	require.NoError(t, recovery.RecoverState())
	synchronizer := state.NewSynchronizer(machine, recovery, dal.NewIncomingRestoreFactory(store))
	raftPool := transport.NewConnectionPool(transport.TLSPolicy{}, transport.PoolConfig{})
	t.Cleanup(func() { require.NoError(t, raftPool.Close()) })
	raftTransport := node.NewTransport(logger, raftPool, meters, id, node.TransportConfig{Reception: []int{8, 8, 8}, Send: []int{8, 8, 8}}, "checkpoint-fence", 1024, "", "")
	go raftTransport.Start(context.Background())
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, raftTransport.Stop(stopCtx))
	})
	raftServer := grpc.NewServer()
	raftTransport.RegisterRaftService(raftServer)
	raftAddr := serveCheckpointFenceRPC(t, raftServer)
	if serviceAddr == "" {
		serviceAddr = raftAddr
	}
	instanceID := []byte(fmt.Sprintf("%016d", id))
	member, err := membership.NewMembership(membership.NewPeerStore(store), raftTransport, pool, id, raftAddr, serviceAddr, instanceID, logger)
	require.NoError(t, err)
	responses := node.NewLocalResponses()
	applier, err := node.NewApplier(machine, recovery, synchronizer, sp, store, w, logger, meter, 1000, 1000, nil, member.OnSnapshotInstalled, responses)
	require.NoError(t, err)
	raftNode, err := node.NewNode(node.NodeConfig{NodeID: id, AdvertiseAddr: raftAddr, ServiceAdvertiseAddr: serviceAddr, InstanceID: instanceID, TickInterval: tickInterval, ProcessingTickInterval: time.Millisecond, MaintenanceInterval: time.Hour}, raftTransport, applier, logger, meter, w, machine, recovery, synchronizer, member, responses)
	require.NoError(t, err)
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- raftNode.Run(context.Background(), ready) }()
	t.Cleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		require.NoError(t, raftNode.Stop(stopCtx))
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-stopCtx.Done():
			t.Error("Raft node did not stop before its stores were closed")
		}
	})
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("Raft node exited before readiness: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	return checkpointFenceNode{node: raftNode, raftTransport: raftTransport, cache: cacheStore, shared: shared, raftAddr: raftAddr}
}

func establishCheckpointFenceLeader(t *testing.T, ctx context.Context, fixture checkpointFenceNode) {
	t.Helper()
	raftConn := dialCheckpointFenceRPC(t, fixture.raftAddr)
	streamCtx := metadata.NewOutgoingContext(ctx, metadata.Pairs(node.MetadataKeyNodeID, "1", node.MetadataKeyClusterID, "checkpoint-fence", node.MetadataKeyPriority, "high"))
	stream, err := rafttransportpb.NewRaftTransportServiceClient(raftConn).StreamMessages(streamCtx)
	require.NoError(t, err)
	heartbeat, err := proto.Marshal(&raftpb.Message{Type: new(raftpb.MsgHeartbeat), From: proto.Uint64(1), To: proto.Uint64(2), Term: proto.Uint64(1), Commit: proto.Uint64(155)})
	require.NoError(t, err)
	require.NoError(t, stream.Send(&rafttransportpb.SendMessageRequest{Message: &rafttransportpb.SendMessageRequest_Raft{Raft: &rafttransportpb.RaftRequestBatch{Messages: []*rafttransportpb.RaftRequestMessage{{Id: 1, Message: heartbeat}}}}}))
	response, err := stream.Recv()
	require.NoError(t, err)
	require.Len(t, response.GetRaft().GetMessages(), 1)
	require.True(t, response.GetRaft().GetMessages()[0].GetSuccess())
	require.Eventually(t, func() bool { return fixture.node.GetLeader() == 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, stream.CloseSend())
}

func serveCheckpointFenceRPC(t *testing.T, server *grpc.Server) string {
	t.Helper()

	return serveCheckpointFenceRPCListener(t, server, checkpointFenceListener(t))
}

func checkpointFenceListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	// Also cover fixture failures before Serve takes ownership. Server.Stop
	// closes this socket in the normal path, so a second close is intentional.
	t.Cleanup(func() { _ = listener.Close() })

	return listener
}

func serveCheckpointFenceRPCListener(t *testing.T, server *grpc.Server, listener net.Listener) string {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Error("checkpoint fixture gRPC server did not stop")
		}
	})

	return listener.Addr().String()
}

func dialCheckpointFenceRPC(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return conn
}
