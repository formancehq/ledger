package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

type oracleConfig struct {
	Attempts          int
	ConvergenceWindow time.Duration
	CallTimeout       time.Duration
	Wait              func(context.Context) error
}

var errObservationInvalidated = errors.New("identity observation invalidated")

type member struct {
	ID       uint64 `json:"id"`
	Address  string `json:"address"`
	Suffrage string `json:"suffrage"`
}

type observation struct {
	Member member       `json:"member"`
	Ready  bool         `json:"ready"`
	State  replicaState `json:"state"`
	Error  string       `json:"error,omitempty"`
}

type attemptReport struct {
	Qualified    bool          `json:"qualified"`
	Horizon      uint64        `json:"horizon"`
	Members      []member      `json:"members"`
	Observations []observation `json:"observations,omitempty"`
	Error        string        `json:"error,omitempty"`
}

// Each retry obtains fresh membership and discards all prior observations.
// Invalid windows and exhausted recovery have their own liveness property.
func runOracle(ctx context.Context, candidates internal.PerNodeConns, config oracleConfig) []attemptReport {
	var reports []attemptReport
	for range config.Attempts {
		report, err := runAttempt(ctx, candidates, config)
		if err != nil {
			report.Error = err.Error()
		}
		reports = append(reports, report)
		if err == nil || ctx.Err() != nil {
			break
		}
		if err := config.Wait(ctx); err != nil {
			break
		}
	}

	return reports
}

func runAttempt(ctx context.Context, candidates internal.PerNodeConns, config oracleConfig) (attemptReport, error) {
	var report attemptReport
	var source *internal.PerNodeConn
	var before uint64
	for _, candidate := range candidates {
		index, err := barrier(ctx, candidate.Bucket, config.CallTimeout)
		if err == nil {
			source, before = candidate, index

			break
		}
	}
	if source == nil {
		return report, errors.New("no reachable source for a quorum barrier")
	}
	members, err := membership(ctx, source, config.CallTimeout)
	if err != nil {
		return report, fmt.Errorf("initial membership: %w", err)
	}
	report.Members = members
	connections := make(map[string]*internal.PerNodeConn, len(candidates))
	for _, candidate := range candidates {
		connections[candidate.Addr] = candidate
	}
	for _, member := range members {
		if connections[member.Address] == nil {
			return report, fmt.Errorf("current member %d at %s has no configured connection", member.ID, member.Address)
		}
	}
	report.Horizon, err = barrier(ctx, source.Bucket, config.CallTimeout)
	if err != nil || report.Horizon != before+1 {
		return report, fmt.Errorf("quiescence not established: %d -> %d: %w", before, report.Horizon, err)
	}

	// Every current member participates, including unavailable voters and
	// learners. A shared bounded window prevents a dead member consuming every
	// other member's opportunity. Absent scale-up slots are not members.
	pollCtx, stop := context.WithTimeout(ctx, config.ConvergenceWindow)
	report.Observations = make([]observation, len(members))
	var workers sync.WaitGroup
	for i, member := range members {
		workers.Go(func() {
			got := observation{Member: member}
			conn := connections[member.Address]
			if err := awaitExact(pollCtx, conn, member, report.Horizon, config); err != nil {
				got.Error = err.Error()
			} else {
				got.Ready = true
			}
			report.Observations[i] = got
		})
	}
	workers.Wait()
	stop()
	for _, got := range report.Observations {
		if !got.Ready {
			return report, fmt.Errorf("member %d did not reach exact durable horizon: %s", got.Member.ID, got.Error)
		}
	}

	// Each scan stays local: stale consistency bypasses ReadIndex and its
	// forwarding fallback. Attribution requires a direct per-node connection
	// matching the advertised member address, never a load-balanced service.
	for i, member := range members {
		workers.Go(func() {
			conn := connections[member.Address]
			state, err := readReplica(internal.WithStaleConsistency(ctx), conn.Bucket)
			report.Observations[i].State = state
			if err == nil {
				err = exactState(ctx, conn, member, report.Horizon, config.CallTimeout)
			}
			if err != nil {
				report.Observations[i].Error = err.Error()
			}
		})
	}
	workers.Wait()
	for _, got := range report.Observations {
		if got.Error != "" {
			return report, fmt.Errorf("member %d observation invalid: %s", got.Member.ID, got.Error)
		}
	}

	// Buffer all equality verdicts until these fences have passed. A late
	// write, election entry or membership change invalidates the entire sweep.
	afterMembers, err := membership(ctx, source, config.CallTimeout)
	if err != nil || !slices.Equal(members, afterMembers) {
		return report, fmt.Errorf("membership changed during observation: %w", err)
	}
	after, err := barrier(ctx, source.Bucket, config.CallTimeout)
	if err != nil || after != report.Horizon+1 {
		return report, fmt.Errorf("final source horizon changed: %d -> %d: %w", report.Horizon, after, err)
	}
	report.Qualified = true

	return report, nil
}

func barrier(ctx context.Context, client servicepb.BucketServiceClient, timeout time.Duration) (uint64, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	response, err := client.Barrier(callCtx, &servicepb.BarrierRequest{})
	if err != nil {
		return 0, err
	}
	if response.GetCommitIndex() == 0 {
		return 0, errors.New("zero barrier index")
	}

	return response.GetCommitIndex(), nil
}

func membership(ctx context.Context, source *internal.PerNodeConn, timeout time.Duration) ([]member, error) {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	state, err := source.Cluster.GetClusterState(callCtx, &clusterpb.GetClusterStateRequest{})
	if err != nil {
		return nil, err
	}
	var members []member
	ids, addresses := map[uint64]bool{}, map[string]bool{}
	for _, n := range state.GetNodes() {
		id, address, suffrage := n.GetId(), n.GetServiceAddress(), n.GetSuffrage()
		if id == 0 || address == "" || ids[id] || addresses[address] || (suffrage != "Voter" && suffrage != "Learner") {
			return nil, fmt.Errorf("invalid member identity: id=%d address=%q suffrage=%q", id, address, suffrage)
		}
		ids[id], addresses[address] = true, true
		members = append(members, member{ID: id, Address: address, Suffrage: suffrage})
	}
	if len(members) == 0 {
		return nil, errors.New("empty current membership")
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })

	return members, nil
}

func awaitExact(ctx context.Context, conn *internal.PerNodeConn, member member, horizon uint64, config oracleConfig) error {
	for {
		err := exactState(ctx, conn, member, horizon, config.CallTimeout)
		if err == nil {
			return nil
		}
		if errors.Is(err, errObservationInvalidated) {
			return err
		}
		if waitErr := config.Wait(ctx); waitErr != nil {
			return fmt.Errorf("%w; last observation: %w", waitErr, err)
		}
	}
}

func exactState(ctx context.Context, conn *internal.PerNodeConn, member member, horizon uint64, timeout time.Duration) error {
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	// GetClusterState can forward to the requested ID. LocalNode therefore
	// verifies the returned status identity, not the physical Bucket endpoint;
	// the direct advertised-address mapping above is the attribution contract.
	state, err := conn.Cluster.GetClusterState(callCtx, &clusterpb.GetClusterStateRequest{NodeId: member.ID})
	if err != nil {
		return err
	}
	if state.GetLocalNode() != member.ID || state.GetRaftStatus().GetLastPersistedIndex() > horizon {
		return fmt.Errorf("%w: want node=%d persisted=%d; got node=%d persisted=%d", errObservationInvalidated, member.ID, horizon, state.GetLocalNode(), state.GetRaftStatus().GetLastPersistedIndex())
	}
	if state.GetSyncProgress().GetStatus() != "normal" || state.GetRaftStatus().GetLastPersistedIndex() != horizon {
		return fmt.Errorf("want node=%d normal persisted=%d; got node=%d status=%q persisted=%d", member.ID, horizon, state.GetLocalNode(), state.GetSyncProgress().GetStatus(), state.GetRaftStatus().GetLastPersistedIndex())
	}

	return nil
}
