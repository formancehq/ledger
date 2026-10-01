package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func TestCheckpointMetadataReadsFenceSameNode(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server, bucket, cluster := checkpointMetadataClients(t)
	stale, err := cluster.ListQueryCheckpoints(ctx, &commonpb.ListQueryCheckpointsRequest{})
	require.NoError(t, err)
	require.Empty(t, stale.GetCheckpoints())
	registry, err := readCheckpointRegistry(ctx, checkpointTestNode(bucket, cluster))
	require.NoError(t, err)
	require.Len(t, registry.GetCheckpoints(), 1)
	require.Equal(t, uint64(7), registry.GetCheckpoints()[0].GetCheckpointId())
	server.fenced.Store(false)
	staleSchedule, err := cluster.GetQueryCheckpointSchedule(ctx, &commonpb.GetQueryCheckpointScheduleRequest{})
	require.NoError(t, err)
	require.Empty(t, staleSchedule.GetCron())
	schedule, err := readCheckpointSchedule(ctx, checkpointTestNode(bucket, cluster))
	require.NoError(t, err)
	require.Equal(t, modelCheckpointCrons[0], schedule.GetCron())
}

func TestCheckpointMetadataFenceFailureStopsRead(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server, bucket, cluster := checkpointMetadataClients(t)
	server.failFence.Store(true)
	_, err := readCheckpointRegistry(ctx, checkpointTestNode(bucket, cluster))
	require.Equal(t, codes.Unavailable, status.Code(err))
	_, err = readCheckpointSchedule(ctx, checkpointTestNode(bucket, cluster))
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Zero(t, server.metadataReads.Load())
}

func checkpointMetadataClients(t *testing.T) (*checkpointMetadataServer, commonpb.BucketServiceClient, commonpb.ClusterServiceClient) {
	t.Helper()
	handler := &checkpointMetadataServer{addr: "node"}
	bucket, cluster := serveCheckpointMetadata(t, handler, handler)

	return handler, bucket, cluster
}

type checkpointMetadataServer struct {
	commonpb.UnimplementedBucketServiceServer
	commonpb.UnimplementedClusterServiceServer

	addr                   string
	denied                 bool
	failDiscovery          bool
	fenced                 atomic.Bool
	failFence              atomic.Bool
	remainingFenceFailures atomic.Int32
	remainingDemotions     atomic.Int32
	metadataReads          atomic.Int32
}

func (s *checkpointMetadataServer) Barrier(context.Context, *commonpb.BarrierRequest) (*commonpb.BarrierResponse, error) {
	for remaining := s.remainingFenceFailures.Load(); remaining > 0; remaining = s.remainingFenceFailures.Load() {
		if s.remainingFenceFailures.CompareAndSwap(remaining, remaining-1) {
			return nil, status.Error(codes.Unavailable, "fence temporarily unavailable")
		}
	}
	if s.failFence.Load() {
		return nil, status.Error(codes.Unavailable, "fence unavailable")
	}
	if s.denied {
		return nil, status.Error(codes.PermissionDenied, "fence denied")
	}

	return &commonpb.BarrierResponse{CommitIndex: 42}, nil
}

func (s *checkpointMetadataServer) GetClusterState(_ context.Context, req *commonpb.GetClusterStateRequest) (*commonpb.ClusterState, error) {
	if req.GetNodeId() == 0 {
		if s.remainingDemotions.CompareAndSwap(1, 0) {
			return &commonpb.ClusterState{State: "Follower", LocalNode: 1}, nil
		}
		if s.failDiscovery {
			return nil, status.Error(codes.Unavailable, "node unavailable during identity discovery")
		}

		return &commonpb.ClusterState{State: "Leader", LocalNode: 1, Nodes: []*commonpb.NodeInfo{{Id: 2, ServiceAddress: s.addr}}}, nil
	}
	if req.GetNodeId() != 2 {
		return nil, status.Error(codes.InvalidArgument, "expected pinned node ID")
	}
	s.fenced.Store(true)

	return &commonpb.ClusterState{LocalNode: 2, RaftStatus: &commonpb.RaftStatus{LastPersistedIndex: 42}}, nil
}

func checkpointTestNode(bucket commonpb.BucketServiceClient, cluster commonpb.ClusterServiceClient) *internal.PerNodeConn {
	return &internal.PerNodeConn{Addr: "node", NodeID: 2, Bucket: bucket, Cluster: cluster}
}

func (s *checkpointMetadataServer) ListQueryCheckpoints(context.Context, *commonpb.ListQueryCheckpointsRequest) (*commonpb.ListQueryCheckpointsResponse, error) {
	s.metadataReads.Add(1)
	response := &commonpb.ListQueryCheckpointsResponse{}
	if s.fenced.Load() {
		response.Checkpoints = []*commonpb.QueryCheckpointInfo{{CheckpointId: 7}}
	}

	return response, nil
}

func (s *checkpointMetadataServer) GetQueryCheckpointSchedule(context.Context, *commonpb.GetQueryCheckpointScheduleRequest) (*commonpb.GetQueryCheckpointScheduleResponse, error) {
	s.metadataReads.Add(1)
	response := &commonpb.GetQueryCheckpointScheduleResponse{}
	if s.fenced.Load() {
		response.Cron = modelCheckpointCrons[0]
	}

	return response, nil
}

func TestCheckpointSetupSelectsReachablePinnedNode(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstServer, firstBucket, firstCluster := checkpointMetadataClients(t)
	firstServer.failFence.Store(true)
	secondServer, secondBucket, secondCluster := checkpointMetadataClients(t)
	firstServer.addr, secondServer.addr = "first", "second"
	first := &internal.PerNodeConn{Addr: "first", NodeID: 2, Bucket: firstBucket, Cluster: firstCluster}
	second := &internal.PerNodeConn{Addr: "second", NodeID: 2, Bucket: secondBucket, Cluster: secondCluster}
	selected, err := selectCheckpointSetupNode(ctx, internal.PerNodeConns{first, second})
	require.NoError(t, err)
	require.Same(t, second, selected)
	_, err = selectCheckpointSetupNode(ctx, internal.PerNodeConns{first})
	require.ErrorContains(t, err, "first")
	require.ErrorContains(t, err, "fence unavailable")
	var failures checkpointSetupProbeFailures
	ok := errors.As(err, &failures)
	require.True(t, ok)
	require.True(t, failures.allTransient())
	require.Zero(t, firstServer.metadataReads.Load(), "selection must not issue metadata calls or setup mutations")
}

func TestCheckpointSetupProbeFailuresPreserveClassification(t *testing.T) {
	t.Parallel()
	transientFailures := checkpointSetupProbeFailures{
		{addr: "unavailable", err: status.Error(codes.Unavailable, "fence unavailable")},
		{addr: "timed-out", err: context.DeadlineExceeded},
	}
	require.True(t, transientFailures.allTransient())

	failures := checkpointSetupProbeFailures{
		{addr: "transient", err: status.Error(codes.Unavailable, "fence unavailable")},
		{addr: "definitive", err: status.Error(codes.PermissionDenied, "fence denied")},
	}
	require.False(t, failures.allTransient())
	require.Equal(t, codes.Unavailable, status.Code(failures[0].err))
	require.Equal(t, codes.PermissionDenied, status.Code(failures[1].err))
}

func TestCheckpointSetupRetriesTransientProbeFailures(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server, bucket, cluster := checkpointMetadataClients(t)
	server.remainingFenceFailures.Store(1)
	node := &internal.PerNodeConn{Addr: "node", NodeID: 2, Bucket: bucket, Cluster: cluster}

	selected, err := waitForCheckpointSetupNode(ctx, internal.PerNodeConns{node})
	require.NoError(t, err)
	require.Same(t, node, selected)
}

func TestCheckpointSetupReturnsDefinitiveProbeFailure(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server, bucket, cluster := checkpointMetadataClients(t)
	server.denied = true
	node := &internal.PerNodeConn{Addr: "node", NodeID: 2, Bucket: bucket, Cluster: cluster}

	_, err := waitForCheckpointSetupNode(ctx, internal.PerNodeConns{node})
	var failures checkpointSetupProbeFailures
	ok := errors.As(err, &failures)
	require.True(t, ok)
	require.Len(t, failures, 1)
	require.Equal(t, codes.PermissionDenied, status.Code(failures[0].err))
}

func TestCheckpointSetupResolvesIdentityAfterFailedDialDiscovery(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstServer, firstBucket, firstCluster := checkpointMetadataClients(t)
	firstServer.failDiscovery = true
	secondServer, secondBucket, secondCluster := checkpointMetadataClients(t)
	secondServer.addr = "second"
	// DialPerNode discovery is best effort. A down first address can consume
	// its shared deadline and leave even reachable connections unresolved.
	first := &internal.PerNodeConn{Addr: "first", Bucket: firstBucket, Cluster: firstCluster}
	second := &internal.PerNodeConn{Addr: "second", Bucket: secondBucket, Cluster: secondCluster}
	selected, err := waitForCheckpointSetupNode(ctx, internal.PerNodeConns{first, second})
	require.NoError(t, err)
	require.Same(t, second, selected)
	response, err := readCheckpointSchedule(ctx, selected)
	require.NoError(t, err)
	require.Equal(t, modelCheckpointCrons[0], response.GetCron())
	require.Zero(t, selected.NodeID, "resolution must not race by mutating a connection shared with workers")
	require.Zero(t, firstServer.metadataReads.Load())
}

func TestCheckpointSetupRetriesDiscoveryDuringLeadershipChange(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server, bucket, cluster := checkpointMetadataClients(t)
	server.remainingDemotions.Store(1)
	node := checkpointTestNode(bucket, cluster)
	selected, err := waitForCheckpointSetupNode(ctx, internal.PerNodeConns{node})
	require.NoError(t, err)
	require.Same(t, node, selected)
	require.Zero(t, server.remainingDemotions.Load(), "the first leader lookup must observe a demotion")
	require.True(t, server.fenced.Load(), "the retry must reach local durable progress")
	require.Zero(t, server.metadataReads.Load(), "discovery retries must not read metadata")
}
