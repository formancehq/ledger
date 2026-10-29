package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
)

// drainNodeStatus is a node's own report, never a leader replication estimate.
type drainNodeStatus struct {
	NodeID             uint64
	DrainEpoch         string
	DrainIndex         uint64
	LastPersistedIndex uint64
	SyncStatus         string
}

type drainDecision struct {
	Epoch   string
	Index   uint64
	Members []uint64
}

type drainCluster interface {
	BeginDrain(context.Context, string) (drainDecision, error)
	CurrentDrain(context.Context) (drainDecision, error)
	NodeStatus(context.Context, uint64) (drainNodeStatus, error)
}

// ensureDrain is idempotent across operator restarts. The first Raft marker
// fixes its cutoff and original membership; retries return that same decision.
// The caller must keep the epoch stable for the lifetime of this shutdown.
func ensureDrain(ctx context.Context, cluster drainCluster, epoch string) (bool, error) {
	if epoch == "" {
		return false, errors.New("drain epoch is required")
	}
	decision, err := cluster.BeginDrain(ctx, epoch)
	if err != nil {
		return false, fmt.Errorf("beginning drain: %w", err)
	}
	if err := validateDrainDecision(decision, epoch); err != nil {
		return false, err
	}
	current, err := cluster.CurrentDrain(ctx)
	if err != nil {
		return false, fmt.Errorf("reading current drain: %w", err)
	}
	if current.Epoch != decision.Epoch || current.Index != decision.Index || !sameMembers(current.Members, decision.Members) {
		return false, errors.New("drain decision or cluster membership changed")
	}
	for _, id := range decision.Members {
		status, err := cluster.NodeStatus(ctx, id)
		if err != nil {
			return false, fmt.Errorf("reading drain status of node %d: %w", id, err)
		}
		if status.NodeID != id || status.DrainEpoch != epoch || status.DrainIndex != decision.Index {
			return false, fmt.Errorf("node %d has not applied the expected drain marker", id)
		}
		if status.SyncStatus != "normal" || status.LastPersistedIndex < decision.Index {
			return false, nil
		}
	}

	return true, nil
}

func validateDrainDecision(decision drainDecision, epoch string) error {
	if decision.Epoch != epoch || decision.Index == 0 || len(decision.Members) == 0 {
		return errors.New("invalid drain decision")
	}
	seen := make(map[uint64]struct{}, len(decision.Members))
	for _, id := range decision.Members {
		if id == 0 {
			return errors.New("drain membership contains node zero")
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("drain membership repeats node %d", id)
		}
		seen[id] = struct{}{}
	}

	return nil
}

func sameMembers(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)

	return slices.Equal(a, b)
}
