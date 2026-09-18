package main

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// Configured addresses include absent scale-up slots. Read current membership
// after source capture, so the witness and final fences also qualify this set.
// Every member (including learners and unavailable members) must be observed;
// only candidates absent from the actual membership are excluded. Resolve IDs
// anew on every attempt instead of trusting best-effort startup discovery.
func activeReplicas(ctx context.Context, source *internal.PerNodeConn, candidates internal.PerNodeConns) (internal.PerNodeConns, error) {
	if len(candidates) == 0 {
		return nil, errors.New("no replica candidates")
	}
	state, err := source.Cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
	if err != nil {
		return nil, err
	}
	if len(state.GetNodes()) == 0 {
		return nil, fmt.Errorf("%w: leader returned empty membership", errSourceChanged)
	}
	byAddress := make(map[string]*internal.PerNodeConn, len(candidates))
	for _, conn := range candidates {
		byAddress[conn.Addr] = conn
	}
	seenIDs := make(map[uint32]bool)
	seenAddresses := make(map[string]bool)
	var replicas internal.PerNodeConns
	for _, node := range state.GetNodes() {
		id, address := node.GetId(), node.GetServiceAddress()
		if id == 0 || address == "" || seenIDs[id] || seenAddresses[address] {
			return nil, fmt.Errorf("invalid membership identity: id=%d address=%q", id, address)
		}
		candidate, exists := byAddress[address]
		if !exists {
			return nil, fmt.Errorf("member %d at %q has no configured connection", id, address)
		}
		seenIDs[id], seenAddresses[address] = true, true
		conn := *candidate
		conn.NodeID = id
		replicas = append(replicas, &conn)
	}
	sort.Slice(replicas, func(i, j int) bool { return replicas[i].NodeID < replicas[j].NodeID })
	return replicas, nil
}

// An address is only a candidate: even the first configured slot may be absent
// or unreachable. Probe sequentially with a bounded call, before capturing any
// source horizon. Selecting a healthy source never removes other members from
// the later convergence verdicts.
func selectSource(ctx context.Context, candidates internal.PerNodeConns) (*internal.PerNodeConn, error) {
	lastErr := errors.New("no replica candidates")
	for _, candidate := range candidates {
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := barrier(callCtx, candidate.Bucket)
		cancel()
		if err == nil {
			return candidate, nil
		}
		lastErr = fmt.Errorf("candidate %s: %w", candidate.Addr, err)
		if ctx.Err() != nil {
			break
		}
	}
	return nil, lastErr
}
