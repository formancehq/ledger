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
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func TestCheckpointMetadataReadsFenceSameNode(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server, bucket, cluster := checkpointMetadataClients(t)
	stale, err := cluster.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
	require.NoError(t, err)
	require.Empty(t, stale.GetCheckpoints())
	registry, err := readCheckpointRegistry(ctx, bucket, cluster, "L")
	require.NoError(t, err)
	require.Len(t, registry.GetCheckpoints(), 1)
	require.Equal(t, uint64(7), registry.Checkpoints[0].GetCheckpointId())
	server.fenced.Store(false)
	staleSchedule, err := cluster.GetQueryCheckpointSchedule(ctx, &clusterpb.GetQueryCheckpointScheduleRequest{})
	require.NoError(t, err)
	require.Empty(t, staleSchedule.GetCron())
	schedule, err := readCheckpointSchedule(ctx, bucket, cluster, "L")
	require.NoError(t, err)
	require.Equal(t, modelCheckpointCrons[0], schedule.GetCron())
}

func TestCheckpointMetadataFenceFailureStopsRead(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server, bucket, cluster := checkpointMetadataClients(t)
	server.failFence.Store(true)
	_, err := readCheckpointRegistry(ctx, bucket, cluster, "L")
	require.Equal(t, codes.Unavailable, status.Code(err))
	_, err = readCheckpointSchedule(ctx, bucket, cluster, "L")
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Zero(t, server.metadataReads.Load())
}

func checkpointMetadataClients(t *testing.T) (*checkpointMetadataServer, servicepb.BucketServiceClient, clusterpb.ClusterServiceClient) {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	handler := &checkpointMetadataServer{}
	servicepb.RegisterBucketServiceServer(server, handler)
	clusterpb.RegisterClusterServiceServer(server, handler)
	go func() { _ = server.Serve(listener) /* Stop terminates Serve with an expected error. */ }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///checkpoint-metadata", grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	return handler, servicepb.NewBucketServiceClient(conn), clusterpb.NewClusterServiceClient(conn)
}

type checkpointMetadataServer struct {
	servicepb.UnimplementedBucketServiceServer
	clusterpb.UnimplementedClusterServiceServer
	fenced        atomic.Bool
	failFence     atomic.Bool
	metadataReads atomic.Int32
}

func (s *checkpointMetadataServer) GetLedger(ctx context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	if s.failFence.Load() {
		return nil, status.Error(codes.Unavailable, "fence unavailable")
	}
	md, _ := metadata.FromIncomingContext(ctx)
	if req.GetLedger() != "L" || len(md.Get("x-consistency")) != 1 || md.Get("x-consistency")[0] != "linearizable" {
		return nil, status.Error(codes.InvalidArgument, "missing linearizable ledger fence")
	}
	s.fenced.Store(true)
	return &commonpb.LedgerInfo{}, nil
}

func (s *checkpointMetadataServer) ListQueryCheckpoints(context.Context, *clusterpb.ListQueryCheckpointsRequest) (*clusterpb.ListQueryCheckpointsResponse, error) {
	s.metadataReads.Add(1)
	response := &clusterpb.ListQueryCheckpointsResponse{}
	if s.fenced.Load() {
		response.Checkpoints = []*clusterpb.QueryCheckpointInfo{{CheckpointId: 7}}
	}
	return response, nil
}

func (s *checkpointMetadataServer) GetQueryCheckpointSchedule(context.Context, *clusterpb.GetQueryCheckpointScheduleRequest) (*clusterpb.GetQueryCheckpointScheduleResponse, error) {
	s.metadataReads.Add(1)
	response := &clusterpb.GetQueryCheckpointScheduleResponse{}
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
	_, secondBucket, secondCluster := checkpointMetadataClients(t)
	first := &internal.PerNodeConn{Addr: "first", Bucket: firstBucket, Cluster: firstCluster}
	second := &internal.PerNodeConn{Addr: "second", Bucket: secondBucket, Cluster: secondCluster}
	selected, err := selectCheckpointSetupNode(ctx, internal.PerNodeConns{first, second}, "L")
	require.NoError(t, err)
	require.Same(t, second, selected)
	_, err = selectCheckpointSetupNode(ctx, internal.PerNodeConns{first}, "L")
	require.ErrorContains(t, err, "first")
	require.ErrorContains(t, err, "fence unavailable")
	require.Zero(t, firstServer.metadataReads.Load(), "selection must not issue metadata calls or setup mutations")
}
