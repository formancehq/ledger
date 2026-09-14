package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// The cluster metadata RPCs read local storage without a consistency fence.
// Callers must supply bucket and cluster clients pinned to the same node:
// completing a linearizable ledger read first advances that node's applied
// state before its registry or schedule snapshot is opened.
func readCheckpointRegistry(ctx context.Context, bucket servicepb.BucketServiceClient, cluster clusterpb.ClusterServiceClient, ledger string) (*clusterpb.ListQueryCheckpointsResponse, error) {
	if err := fenceCheckpointMetadata(ctx, bucket, ledger); err != nil {
		return nil, err
	}
	return cluster.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
}

func readCheckpointSchedule(ctx context.Context, bucket servicepb.BucketServiceClient, cluster clusterpb.ClusterServiceClient, ledger string) (*clusterpb.GetQueryCheckpointScheduleResponse, error) {
	if err := fenceCheckpointMetadata(ctx, bucket, ledger); err != nil {
		return nil, err
	}
	return cluster.GetQueryCheckpointSchedule(ctx, &clusterpb.GetQueryCheckpointScheduleRequest{})
}

func fenceCheckpointMetadata(ctx context.Context, bucket servicepb.BucketServiceClient, ledger string) error {
	headers, _ := metadata.FromOutgoingContext(ctx)
	headers = headers.Copy()
	headers.Set("x-consistency", "linearizable")
	_, err := bucket.GetLedger(metadata.NewOutgoingContext(ctx, headers), &servicepb.GetLedgerRequest{Ledger: ledger})
	return err
}

// Probe before any setup mutation: an unavailable first address must not consume
// the complete workload lifetime when another node can serve the baseline.
func selectCheckpointSetupNode(ctx context.Context, nodes internal.PerNodeConns, ledger string) (*internal.PerNodeConn, error) {
	failures := []error{errors.New("no reachable checkpoint setup node")}
	for _, node := range nodes {
		probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := fenceCheckpointMetadata(probeCtx, node.Bucket, ledger)
		cancel()
		if err == nil {
			return node, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		failures = append(failures, fmt.Errorf("%s: %w", node.Addr, err))
	}
	return nil, errors.Join(failures...)
}
