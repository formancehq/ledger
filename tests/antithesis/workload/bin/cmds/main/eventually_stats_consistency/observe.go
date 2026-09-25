package main

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

type observation struct {
	Node              string            `json:"node,omitempty"`
	NodeID            uint32            `json:"nodeId,omitempty"`
	Converged         bool              `json:"converged"`
	WitnessReferences uint64            `json:"witnessReferences"`
	Attempts          int               `json:"attempts"`
	Mismatches        map[string]counts `json:"mismatches,omitempty"` // Last complete target sweep.
	Error             string            `json:"error,omitempty"`
}

// awaitUsage never polls the value under test as its catch-up certificate.
// ReferenceCount on a fresh witness ledger proves progress independently of
// source PostingCount/RevertCount. All reads are local to this single target.
// Mismatches remain visible at timeout; a usage reset/replay between RPCs is
// allowed to recover rather than causing an immediate safety false positive.
func awaitUsage(ctx context.Context, client servicepb.BucketServiceClient, witness *commonpb.LedgerInfo, expected map[string]expectedLedger, wait func(context.Context) error) observation {
	var result observation
	for {
		result.Attempts++
		// A complete sweep may span hundreds of ledgers. Use the shared
		// convergence budget rather than restarting it after a short RPC limit.
		matched, err := sampleUsage(internal.WithStaleConsistency(ctx), client, witness, expected, &result)
		if err != nil {
			result.Error = err.Error()
		} else {
			result.Error = ""
		}
		if matched && err == nil {
			result.Converged = true
			return result
		}
		if err != nil && !internal.IsTransient(err) && !internal.IsCanceled(err) {
			return result
		}
		if err := wait(ctx); err != nil {
			result.Error = fmt.Sprintf("usage convergence did not complete: %v; last RPC error: %s", err, result.Error)
			return result
		}
	}
}

func sampleUsage(ctx context.Context, client servicepb.BucketServiceClient, witness *commonpb.LedgerInfo, expected map[string]expectedLedger, result *observation) (bool, error) {
	mismatches := make(map[string]counts)
	witnessInfo, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: witness.GetName()})
	if err != nil {
		return false, err
	}
	if witnessInfo.GetId() != witness.GetId() {
		return false, fmt.Errorf("usage witness incarnation changed")
	}
	marker, err := client.GetLedgerStats(ctx, &servicepb.GetLedgerStatsRequest{Ledger: witness.GetName()})
	if err != nil {
		return false, err
	}
	result.WitnessReferences = marker.GetReferenceCount()
	if result.WitnessReferences != 1 {
		return false, nil
	}
	for name, want := range expected {
		info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: name})
		if err != nil {
			return false, err
		}
		if info.GetId() != want.ID {
			return false, fmt.Errorf("ledger %s incarnation changed: %d -> %d", name, want.ID, info.GetId())
		}
		stats, err := client.GetLedgerStats(ctx, &servicepb.GetLedgerStatsRequest{Ledger: name})
		if err != nil {
			return false, err
		}
		got := counts{Logs: stats.GetLogCount(), Postings: stats.GetPostingCount(), Reverts: stats.GetRevertCount()}
		if got != want.Counts {
			mismatches[name] = got
		}
	}
	// Preserve the previous complete sweep if a later attempt cannot finish its
	// RPCs. A deadline or transport failure must not erase observed bad counts.
	result.Mismatches = mismatches
	// A restart can rewind the WAL-less usage store. Do not accept a sweep if
	// its independent witness disappeared while collecting counter snapshots.
	marker, err = client.GetLedgerStats(ctx, &servicepb.GetLedgerStatsRequest{Ledger: witness.GetName()})
	if err != nil {
		return false, err
	}
	result.WitnessReferences = marker.GetReferenceCount()
	return marker.GetReferenceCount() == 1 && len(result.Mismatches) == 0, nil
}

// FSM persistence qualifies node identity and source visibility, not usage.
func awaitReplica(ctx context.Context, conn *internal.PerNodeConn, horizon uint64, witness *commonpb.LedgerInfo, expected map[string]expectedLedger, wait func(context.Context) error) observation {
	if conn.NodeID == 0 {
		return observation{Error: "replica node ID could not be resolved"}
	}
	for {
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		state, err := conn.Cluster.GetClusterState(callCtx, &clusterpb.GetClusterStateRequest{NodeId: conn.NodeID})
		cancel()
		if err == nil && state.GetLocalNode() == conn.NodeID && state.GetSyncProgress().GetStatus() == "normal" && state.GetRaftStatus().GetLastPersistedIndex() >= horizon {
			return awaitUsage(ctx, conn.Bucket, witness, expected, wait)
		}
		if err != nil && !internal.IsTransient(err) && !internal.IsCanceled(err) {
			return observation{Error: err.Error()}
		}
		if waitErr := wait(ctx); waitErr != nil {
			return observation{Error: fmt.Sprintf("replica did not reach source horizon: %v; last RPC: %v", waitErr, err)}
		}
	}
}
