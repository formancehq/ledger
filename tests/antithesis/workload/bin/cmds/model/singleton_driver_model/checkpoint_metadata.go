package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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

// Cluster metadata RPCs read local storage. A routed GetLedger can succeed on
// the leader while this node still lags, so fence the pinned node's durable
// applied cursor before opening its registry or schedule snapshot.
func readCheckpointRegistry(ctx context.Context, node *internal.PerNodeConn) (*clusterpb.ListQueryCheckpointsResponse, error) {
	if err := fenceCheckpointMetadata(ctx, node); err != nil {
		return nil, err
	}
	return node.Cluster.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
}

func readCheckpointSchedule(ctx context.Context, node *internal.PerNodeConn) (*clusterpb.GetQueryCheckpointScheduleResponse, error) {
	if err := fenceCheckpointMetadata(ctx, node); err != nil {
		return nil, err
	}
	return node.Cluster.GetQueryCheckpointSchedule(ctx, &clusterpb.GetQueryCheckpointScheduleRequest{})
}

func fenceCheckpointMetadata(ctx context.Context, node *internal.PerNodeConn) error {
	nodeID, err := checkpointMetadataNodeID(ctx, node)
	if err != nil {
		return err
	}
	barrier, err := node.Bucket.Barrier(ctx, &servicepb.BarrierRequest{})
	if err != nil {
		return err
	}
	target := barrier.GetCommitIndex()
	if target == 0 {
		return fmt.Errorf("checkpoint metadata barrier returned zero index")
	}
	// Capture one watermark. Concurrent writes may advance beyond it; chasing
	// the moving leader head would unnecessarily starve this bounded read.
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := node.Cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{NodeId: nodeID})
		if err != nil {
			return err
		}
		if state.GetLocalNode() != nodeID || state.GetRaftStatus() == nil {
			return fmt.Errorf("checkpoint metadata node %q: expected state for node %d with durable progress, got node %d", node.Addr, nodeID, state.GetLocalNode())
		}
		persisted := state.GetRaftStatus().GetLastPersistedIndex()
		syncStatus := state.GetSyncProgress().GetStatus()
		if persisted >= target && (syncStatus == "" || syncStatus == "normal") {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("checkpoint metadata node %d at %q: durable index %d below barrier %d or sync status %q: %w", nodeID, node.Addr, persisted, target, syncStatus, status.FromContextError(ctx.Err()).Err())
		case <-ticker.C:
		}
	}
}

// Follower state has no Nodes list. Use the leader's topology ONLY to verify
// identity; its progress must never authorize a read of the pinned local store.
// Refresh unresolved best-effort dial identities under the caller's deadline,
// without mutating the PerNodeConn shared by concurrent model workers.
func checkpointMetadataNodeID(ctx context.Context, node *internal.PerNodeConn) (uint32, error) {
	if node.Addr == "" {
		return 0, fmt.Errorf("checkpoint metadata node has no pinned address")
	}
	topology, err := node.Cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
	if err != nil {
		return 0, err
	}
	// Leadership can change between the RPC's routing decision and the
	// orchestrator sampling status. A demoted node legitimately omits Nodes.
	switch topology.GetState() {
	case "Leader":
	case "Follower", "Candidate", "PreCandidate", "Shutdown":
		return 0, status.Error(codes.Unavailable, "checkpoint metadata leader changed during identity discovery")
	default:
		return 0, fmt.Errorf("checkpoint metadata discovery returned invalid node state %q", topology.GetState())
	}
	for _, peer := range topology.GetNodes() {
		if peer.GetServiceAddress() != node.Addr || peer.GetId() == 0 {
			continue
		}
		if node.NodeID != 0 && node.NodeID != peer.GetId() {
			return 0, fmt.Errorf("checkpoint metadata node %q is not advertised as node %d (got %d)", node.Addr, node.NodeID, peer.GetId())
		}
		return peer.GetId(), nil
	}
	return 0, fmt.Errorf("checkpoint metadata node %q has unresolved identity in leader topology", node.Addr)
}

// Probe before changing the checkpoint baseline: an unavailable first address must not consume
// the complete workload lifetime when another node can serve the baseline.
func selectCheckpointSetupNode(ctx context.Context, nodes internal.PerNodeConns) (*internal.PerNodeConn, error) {
	failures := make(checkpointSetupProbeFailures, 0, len(nodes))
	for _, node := range nodes {
		probeCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		err := fenceCheckpointMetadata(probeCtx, node)
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

func waitForCheckpointSetupNode(ctx context.Context, nodes internal.PerNodeConns) (*internal.PerNodeConn, error) {
	for {
		node, err := selectCheckpointSetupNode(ctx, nodes)
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
