package main

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// Reproduce the RPC contract that the model relies on: GetLedger arriving at
// a lagging follower is forwarded to a distinct leader, while ClusterService
// metadata remains local. Values are synthetic; this is not a replay of the
// Antithesis timeline. The real helper under test must prove LOCAL durability.
func TestCheckpointMetadataForwardedLedgerDoesNotFenceFollower(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"registry", "schedule"} {
		t.Run(kind, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			leader := &checkpointFenceLeader{}
			leaderBucket, _ := serveCheckpointMetadata(t, leader, leader)
			follower := &checkpointFenceFollower{leader: leaderBucket}
			bucket, cluster := serveCheckpointMetadata(t, follower, follower)

			// Control: successful leader forwarding leaves the follower stale.
			_, err := bucket.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: "L"})
			require.NoError(t, err)
			require.Equal(t, int32(1), follower.forwarded.Load())
			require.True(t, leader.fenced.Load())
			require.False(t, follower.fenced.Load())
			stale, err := cluster.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
			require.NoError(t, err)
			require.Empty(t, stale.GetCheckpoints())

			if kind == "registry" {
				response, err := readCheckpointRegistry(ctx, checkpointTestNode(bucket, cluster))
				require.NoError(t, err)
				require.Len(t, response.GetCheckpoints(), 1, "forwarded GetLedger must not authorize stale follower registry")
				require.Equal(t, uint64(7), response.Checkpoints[0].GetCheckpointId())
			} else {
				response, err := readCheckpointSchedule(ctx, checkpointTestNode(bucket, cluster))
				require.NoError(t, err)
				require.Equal(t, modelCheckpointCrons[0], response.GetCron(), "forwarded GetLedger must not authorize stale follower schedule")
			}
			require.Equal(t, int32(2), follower.localPolls.Load(), "wait for the receiving node's durable watermark, not Raft applied/commit or leader progress")
		})
	}
}

type checkpointFenceLeader struct {
	checkpointMetadataServer
}

func (s *checkpointFenceLeader) GetLedger(context.Context, *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	s.fenced.Store(true)
	return &commonpb.LedgerInfo{}, nil
}

func (*checkpointFenceLeader) Barrier(context.Context, *servicepb.BarrierRequest) (*servicepb.BarrierResponse, error) {
	return &servicepb.BarrierResponse{CommitIndex: 996}, nil
}

type checkpointFenceFollower struct {
	checkpointMetadataServer
	leader     servicepb.BucketServiceClient
	forwarded  atomic.Int32
	localPolls atomic.Int32
}

func (s *checkpointFenceFollower) GetLedger(ctx context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	s.forwarded.Add(1)
	return s.leader.GetLedger(ctx, req)
}

func (s *checkpointFenceFollower) Barrier(ctx context.Context, req *servicepb.BarrierRequest) (*servicepb.BarrierResponse, error) {
	return s.leader.Barrier(ctx, req)
}

func (s *checkpointFenceFollower) GetClusterState(_ context.Context, req *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	// A zero/default request routes to the leader, and cannot fence this node.
	if req.GetNodeId() != 2 {
		return &clusterpb.ClusterState{State: "Leader", LocalNode: 1, Nodes: []*clusterpb.NodeInfo{{Id: 2, ServiceAddress: "node"}}, RaftStatus: &clusterpb.RaftStatus{LastPersistedIndex: 1992}}, nil
	}
	persisted := uint64(995)
	if s.localPolls.Add(1) >= 2 {
		persisted = 996
		s.fenced.Store(true)
	}
	return &clusterpb.ClusterState{
		LocalNode:  2,
		RaftStatus: &clusterpb.RaftStatus{Applied: 1992, Commit: 1992, LastPersistedIndex: persisted},
	}, nil
}

func serveCheckpointMetadata(t *testing.T, bucket servicepb.BucketServiceServer, cluster clusterpb.ClusterServiceServer, opts ...grpc.ServerOption) (servicepb.BucketServiceClient, clusterpb.ClusterServiceClient) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(opts...)
	servicepb.RegisterBucketServiceServer(server, bucket)
	clusterpb.RegisterClusterServiceServer(server, cluster)
	go func() { _ = server.Serve(listener) /* Stop terminates Serve. */ }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///checkpoint-fence", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return servicepb.NewBucketServiceClient(conn), clusterpb.NewClusterServiceClient(conn)
}

func TestCheckpointMetadataRejectsUnprovenLocalProgress(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		mutate  func(*checkpointFenceProbe, *uint32)
		code    codes.Code
		want    string
		timeout bool
	}{
		{name: "unresolved identity", want: "unresolved identity", mutate: func(s *checkpointFenceProbe, id *uint32) { *id = 0; s.topology.Nodes = nil }},
		{name: "wrong requested node", want: "is not advertised as node 1", mutate: func(s *checkpointFenceProbe, id *uint32) { *id = 1; s.state.LocalNode = 1 }},
		{name: "wrong responding node", want: "expected state for node 2", mutate: func(s *checkpointFenceProbe, _ *uint32) { s.state.LocalNode = 1 }},
		{name: "leader demoted during discovery", mutate: func(s *checkpointFenceProbe, _ *uint32) { s.topology.State = "Follower"; s.topology.Nodes = nil }, code: codes.Unavailable},
		{name: "invalid discovery response", want: "invalid node state", mutate: func(s *checkpointFenceProbe, _ *uint32) { s.topology.State = "" }},
		{name: "missing durable status", want: "with durable progress", mutate: func(s *checkpointFenceProbe, _ *uint32) { s.state.RaftStatus = nil }},
		{name: "missing address mapping", want: "unresolved identity in leader topology", mutate: func(s *checkpointFenceProbe, _ *uint32) { s.topology.Nodes = nil }},
		{name: "zero barrier", want: "barrier returned zero index", mutate: func(s *checkpointFenceProbe, _ *uint32) { s.target = 0 }},
		{name: "barrier denied", mutate: func(s *checkpointFenceProbe, _ *uint32) {
			s.barrierErr = status.Error(codes.PermissionDenied, "denied")
		}, code: codes.PermissionDenied},
		{name: "state error", mutate: func(s *checkpointFenceProbe, _ *uint32) { s.stateErr = status.Error(codes.Unknown, "storage failure") }, code: codes.Unknown},
		{name: "raft progress without durable progress", mutate: func(s *checkpointFenceProbe, _ *uint32) { s.state.RaftStatus.LastPersistedIndex = 995 }, timeout: true},
		{name: "syncing despite durable cursor", mutate: func(s *checkpointFenceProbe, _ *uint32) {
			s.state.SyncProgress = &clusterpb.SyncProgress{Status: "syncing"}
		}, timeout: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			for _, schedule := range []bool{false, true} {
				probe := newCheckpointFenceProbe()
				id := uint32(2)
				tc.mutate(probe, &id)
				bucket, cluster := serveCheckpointMetadata(t, probe, probe)
				node := checkpointTestNode(bucket, cluster)
				node.NodeID = id
				ctx, cancel := context.WithTimeout(context.Background(), 75*time.Millisecond)
				var err error
				if schedule {
					_, err = readCheckpointSchedule(ctx, node)
				} else {
					_, err = readCheckpointRegistry(ctx, node)
				}
				cancel()
				require.Error(t, err)
				if tc.want != "" {
					require.ErrorContains(t, err, tc.want)
				}
				if tc.timeout {
					require.Equal(t, codes.DeadlineExceeded, status.Code(err))
					require.True(t, internal.IsTransient(err) || isShutdownError(err), "the read callers must treat ordinary catch-up timeout as inconclusive: %v", err)
				} else if tc.code != codes.OK {
					require.Equal(t, tc.code, status.Code(err))
				}
				require.Zero(t, probe.metadataReads.Load(), "unproven local progress must never authorize metadata")
				if id == 0 {
					require.Zero(t, probe.barriers.Load(), "unresolved identity must not issue a barrier or local metadata read")
				}
			}
		})
	}
}

func TestCheckpointMetadataAllowsProgressBeyondFixedBarrier(t *testing.T) {
	t.Parallel()
	probe := newCheckpointFenceProbe()
	probe.state.RaftStatus.LastPersistedIndex = 1000
	probe.fenced.Store(true)
	bucket, cluster := serveCheckpointMetadata(t, probe, probe)
	response, err := readCheckpointRegistry(context.Background(), checkpointTestNode(bucket, cluster))
	require.NoError(t, err)
	require.Len(t, response.GetCheckpoints(), 1)
	require.Equal(t, int32(1), probe.barriers.Load(), "one fixed watermark, not a moving target")
}

type checkpointFenceProbe struct {
	checkpointMetadataServer
	state      *clusterpb.ClusterState
	topology   *clusterpb.ClusterState
	target     uint64
	barrierErr error
	stateErr   error
	barriers   atomic.Int32
}

func newCheckpointFenceProbe() *checkpointFenceProbe {
	return &checkpointFenceProbe{
		target:   996,
		topology: &clusterpb.ClusterState{State: "Leader", LocalNode: 1, Nodes: []*clusterpb.NodeInfo{{Id: 2, ServiceAddress: "node"}}, RaftStatus: &clusterpb.RaftStatus{LastPersistedIndex: 1992}},
		state: &clusterpb.ClusterState{
			LocalNode:  2,
			RaftStatus: &clusterpb.RaftStatus{Applied: 1992, Commit: 1992, LastPersistedIndex: 996},
		},
	}
}

func (s *checkpointFenceProbe) Barrier(context.Context, *servicepb.BarrierRequest) (*servicepb.BarrierResponse, error) {
	s.barriers.Add(1)
	return &servicepb.BarrierResponse{CommitIndex: s.target}, s.barrierErr
}

func (s *checkpointFenceProbe) GetClusterState(_ context.Context, req *clusterpb.GetClusterStateRequest) (*clusterpb.ClusterState, error) {
	if req.GetNodeId() == 0 {
		return s.topology, nil
	}
	return s.state, s.stateErr
}
