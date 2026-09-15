package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

type checkpointSetupProbeFailure struct {
	addr string
	err  error
}

type checkpointSetupProbeFailures []checkpointSetupProbeFailure

func (failures checkpointSetupProbeFailures) Error() string {
	parts := make([]string, 0, len(failures))
	for _, failure := range failures {
		parts = append(parts, fmt.Sprintf("%s: %s", failure.addr, failure.err))
	}
	return strings.Join(parts, "; ")
}

func (failures checkpointSetupProbeFailures) allTransient() bool {
	if len(failures) == 0 {
		return false
	}
	for _, failure := range failures {
		if !internal.IsTransient(failure.err) && !errors.Is(failure.err, context.DeadlineExceeded) {
			return false
		}
	}
	return true
}

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
	failures := make(checkpointSetupProbeFailures, 0, len(nodes))
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
		failures = append(failures, checkpointSetupProbeFailure{addr: node.Addr, err: err})
	}
	if len(failures) == 0 {
		return nil, fmt.Errorf("no checkpoint setup nodes configured")
	}
	return nil, failures
}

func waitForCheckpointSetupNode(ctx context.Context, nodes internal.PerNodeConns, ledger string) (*internal.PerNodeConn, error) {
	for {
		node, err := selectCheckpointSetupNode(ctx, nodes, ledger)
		if err == nil {
			return node, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		failures, ok := err.(checkpointSetupProbeFailures)
		if !ok || !failures.allTransient() {
			return nil, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}
