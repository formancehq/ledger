package main

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// Global checkpoint metadata remains observable after the last ledger is
// deleted. Its local durability fence must not reopen the response-frontier
// race fixed for lifecycle reads: a later write cannot register during any RPC
// in the observation, while the processor must remain free to take c.mu.
func TestCheckpointMetadataReadsKeepResponseFrontierWithoutLiveLedgers(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"registry", "schedule"} {
		for _, failFence := range []bool{false, true} {
			name := kind + "/success"
			if failFence {
				name = kind + "/transient-fence-failure"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				c := NewChecker([]string{"L"}, nil)
				deleted := c.modelState.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_DeleteLedger{
					DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "L"},
				}}))
				require.True(t, deleted.OK)
				c.modelState = deleted.State
				require.Empty(t, c.liveLedgerNamesSnapshot())
				c.mu.Lock()
				readID := c.registerRead()
				c.mu.Unlock()
				defer c.finishRead(readID)

				handler := &checkpointEmptyMetadataServer{checkpointMetadataServer: checkpointMetadataServer{addr: "node"}}
				handler.failFence.Store(failFence)
				calls := make(chan string, 8)
				interceptor := grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
					calls <- info.FullMethod
					if c.dispatchMu.TryLock() {
						c.dispatchMu.Unlock()
						t.Errorf("a later write could register during %s", info.FullMethod)
					}
					if c.mu.TryLock() {
						c.mu.Unlock()
					} else {
						t.Errorf("the processor is blocked by a read during %s", info.FullMethod)
					}
					return next(ctx, req)
				})
				bucket, cluster := serveCheckpointMetadata(t, handler, handler, interceptor)
				node := checkpointTestNode(bucket, cluster)
				if kind == "registry" {
					runCheckpointListRead(ctx, node, c)
				} else {
					runCheckpointScheduleRead(ctx, node, c)
				}
				wantCalls := []string{clusterpb.ClusterService_GetClusterState_FullMethodName, servicepb.BucketService_Barrier_FullMethodName}
				if failFence {
					require.Zero(t, handler.metadataReads.Load())
				} else {
					wantCalls = append(wantCalls, clusterpb.ClusterService_GetClusterState_FullMethodName)
					if kind == "registry" {
						wantCalls = append(wantCalls, clusterpb.ClusterService_ListQueryCheckpoints_FullMethodName)
					} else {
						wantCalls = append(wantCalls, clusterpb.ClusterService_GetQueryCheckpointSchedule_FullMethodName)
					}
					require.Equal(t, int32(1), handler.metadataReads.Load(), "global metadata must still be read with no live ledgers")
				}
				close(calls)
				var gotCalls []string
				for method := range calls {
					gotCalls = append(gotCalls, method)
				}
				require.Equal(t, wantCalls, gotCalls)
				require.True(t, c.dispatchMu.TryLock(), "both success and error paths must release writer dispatch")
				c.dispatchMu.Unlock()
				require.Contains(t, c.reads, readID, "metadata helpers must preserve the outer read-drain gate")
			})
		}
	}
}

type checkpointEmptyMetadataServer struct {
	checkpointMetadataServer
}

func (s *checkpointEmptyMetadataServer) ListQueryCheckpoints(context.Context, *clusterpb.ListQueryCheckpointsRequest) (*clusterpb.ListQueryCheckpointsResponse, error) {
	s.metadataReads.Add(1)
	return &clusterpb.ListQueryCheckpointsResponse{}, nil
}

func (s *checkpointEmptyMetadataServer) GetQueryCheckpointSchedule(context.Context, *clusterpb.GetQueryCheckpointScheduleRequest) (*clusterpb.GetQueryCheckpointScheduleResponse, error) {
	s.metadataReads.Add(1)
	return &clusterpb.GetQueryCheckpointScheduleResponse{}, nil
}
