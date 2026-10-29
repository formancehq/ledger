package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// ErrDrainEpochConflict means the first committed marker belongs to another
// shutdown attempt and cannot be replaced.
var ErrDrainEpochConflict = errors.New("cluster is already draining under another epoch")

// BeginDrain proposes the Raft-ordered admission cutoff. All proposals that
// cross Propose before this marker are ahead of it in the local queue. The
// FSM enforces the cutoff by Raft index even after a leadership change.
func (node *Node) BeginDrain(ctx context.Context, epoch string, members []uint64) (*raftcmdpb.DrainState, error) {
	if epoch == "" || len(epoch) > 128 {
		return nil, errors.New("drain epoch must contain 1 to 128 bytes")
	}
	if err := node.WaitLeaderReady(ctx); err != nil {
		return nil, fmt.Errorf("waiting for drain leader readiness: %w", err)
	}
	if current := node.fsm.DrainDecision(); current != nil {
		if current.GetEpoch() != epoch {
			return nil, ErrDrainEpochConflict
		}
		if err := node.fsm.WaitForApplied(ctx, current.GetIndex()); err != nil {
			return nil, fmt.Errorf("waiting for persisted drain marker: %w", err)
		}

		return current, nil
	}
	if len(members) == 0 {
		return nil, errors.New("drain requires cluster members")
	}

	cmd := commands.NewCommand()
	cmd.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterPeer)
	cmd.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{
		Kind: &raftcmdpb.TechnicalUpdate_DrainMarker{DrainMarker: &raftcmdpb.DrainMarker{Epoch: epoch, Members: append([]uint64(nil), members...)}},
	}}
	data, err := cmd.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("marshaling drain marker: %w", err)
	}
	proposal := NewProposal(cmd.GetId(), data)
	proposal.drainMarker = true

	// The same tracker lock used by business admission serializes our queue
	// insertion with its final proposal step. PredictedIndex stays zero because
	// the marker has no preload and does not need an index prediction.
	node.indexTracker.Lock()
	future, err := node.Propose(ctx, proposal)
	node.indexTracker.Unlock()
	if err != nil {
		return nil, fmt.Errorf("proposing drain marker: %w", err)
	}
	if _, err := proposal.Wait(ctx); err != nil {
		return nil, fmt.Errorf("waiting for drain consensus: %w", err)
	}
	result, err := future.Wait(ctx)
	if err != nil {
		if errors.Is(err, ErrLeadershipLost) {
			// A later term applied without this marker, so its accepted Raft
			// proposal was definitively truncated. A timeout is not equivalent.
			node.releaseDroppedDrain()
		}

		return nil, fmt.Errorf("waiting for drain apply: %w", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("applying drain marker: %w", result.Error)
	}
	decision := node.fsm.DrainDecision()
	if decision == nil || decision.GetEpoch() != epoch || decision.GetIndex() == 0 {
		return nil, ErrDrainEpochConflict
	}

	return decision, nil
}

// SyncDrainWAL makes an already applied drain marker durable in Pebble's WAL.
// The caller must capture LastPersistedIndex before calling this method and
// report only that captured index: later batches may commit during the fsync.
func (node *Node) SyncDrainWAL() error {
	return node.applier.Store().SyncWAL()
}
