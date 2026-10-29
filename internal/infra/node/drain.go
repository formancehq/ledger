package node

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// ErrDrainEpochConflict means the first committed marker belongs to another
// shutdown attempt and cannot be replaced.
var ErrDrainEpochConflict = errors.New("cluster is already draining under another epoch")

// ErrDrainEpochReused means a released epoch cannot start a new cutoff.
var ErrDrainEpochReused = errors.New("drain epoch has already been released")

// ErrNoActiveDrain means there is no active marker with the supplied epoch.
var ErrNoActiveDrain = errors.New("no active drain with the supplied epoch")

func (node *Node) drainActiveOrPending() bool {
	node.proposalAdmissionMu.Lock()
	pending := node.drainOutstanding != 0
	releaseSeen := node.drainReleaseSeen
	node.proposalAdmissionMu.Unlock()
	// Lightweight membership tests construct a Node without an FSM.
	if node.fsm == nil {
		return pending
	}
	_, index := node.fsm.Drain()
	if index != 0 {
		return true
	}
	// An applied release supersedes a prior generation's local marker latch.
	return pending && node.fsm.DrainReleaseIndex() <= releaseSeen
}

func (node *Node) waitForPriorMembership(ctx context.Context) error {
	if err := node.WaitLeaderReady(ctx); err != nil {
		return err
	}
	// A canceled membership call may leave an admitted command in the
	// orchestrator's queue. Fence that queue first; the Raft entry below then
	// waits for any ConfChange that command proposed to apply.
	if err := node.execClusterCommand(ctx, true, func() error { return nil }); err != nil {
		return fmt.Errorf("fencing membership commands: %w", err)
	}
	cmd := commands.NewCommand()
	cmd.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterPeer)
	data, err := cmd.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling membership barrier: %w", err)
	}
	proposal := NewProposal(cmd.GetId(), data)
	proposal.membershipBarrier = true
	node.indexTracker.Lock()
	future, err := node.Propose(ctx, proposal)
	node.indexTracker.Unlock()
	if err != nil {
		return fmt.Errorf("proposing membership barrier: %w", err)
	}
	if _, err := proposal.Wait(ctx); err != nil {
		return fmt.Errorf("waiting for membership barrier consensus: %w", err)
	}
	result, err := future.Wait(ctx)
	if err != nil {
		return fmt.Errorf("waiting for membership barrier apply: %w", err)
	}
	if result.Error != nil {
		return fmt.Errorf("applying membership barrier: %w", result.Error)
	}

	return nil
}

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
		if slices.Contains(current.GetPriorEpochs(), epoch) {
			return nil, ErrDrainEpochReused
		}
		if current.GetReleasedIndex() == 0 {
			if current.GetEpoch() != epoch {
				return nil, ErrDrainEpochConflict
			}
			if err := node.fsm.WaitForApplied(ctx, current.GetIndex()); err != nil {
				return nil, fmt.Errorf("waiting for persisted drain marker: %w", err)
			}

			return current, nil
		}
		if current.GetEpoch() == epoch {
			return nil, ErrDrainEpochReused
		}
		if err := node.fsm.WaitForApplied(ctx, current.GetReleasedIndex()); err != nil {
			return nil, fmt.Errorf("waiting for persisted drain release: %w", err)
		}
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

// EndDrain commits an epoch-matched release marker. Admission stays closed
// until this marker is applied locally, and every follower reopens at the
// same Raft position before applying subsequent writes.
func (node *Node) EndDrain(ctx context.Context, epoch string) (*raftcmdpb.DrainState, error) {
	if epoch == "" || len(epoch) > 128 {
		return nil, errors.New("drain epoch must contain 1 to 128 bytes")
	}
	if err := node.WaitLeaderReady(ctx); err != nil {
		return nil, fmt.Errorf("waiting for drain leader readiness: %w", err)
	}
	current := node.fsm.DrainDecision()
	if current == nil || current.GetEpoch() != epoch {
		return nil, ErrNoActiveDrain
	}
	if current.GetReleasedIndex() != 0 {
		if err := node.fsm.WaitForApplied(ctx, current.GetReleasedIndex()); err != nil {
			return nil, fmt.Errorf("waiting for persisted drain release: %w", err)
		}

		return current, nil
	}
	if err := node.fsm.WaitForApplied(ctx, current.GetIndex()); err != nil {
		return nil, fmt.Errorf("waiting for persisted drain marker: %w", err)
	}

	cmd := commands.NewCommand()
	cmd.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterPeer)
	cmd.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{
		Kind: &raftcmdpb.TechnicalUpdate_DrainRelease{DrainRelease: &raftcmdpb.DrainRelease{Epoch: epoch}},
	}}
	data, err := cmd.MarshalVT()
	if err != nil {
		return nil, fmt.Errorf("marshaling drain release: %w", err)
	}
	proposal := NewProposal(cmd.GetId(), data)
	proposal.releaseMarker = true
	node.indexTracker.Lock()
	future, err := node.Propose(ctx, proposal)
	node.indexTracker.Unlock()
	if err != nil {
		return nil, fmt.Errorf("proposing drain release: %w", err)
	}
	if _, err := proposal.Wait(ctx); err != nil {
		return nil, fmt.Errorf("waiting for release consensus: %w", err)
	}
	result, err := future.Wait(ctx)
	if err != nil {
		return nil, fmt.Errorf("waiting for release apply: %w", err)
	}
	if result.Error != nil {
		return nil, fmt.Errorf("applying drain release: %w", result.Error)
	}
	decision := node.fsm.DrainDecision()
	if decision == nil || decision.GetEpoch() != epoch || decision.GetReleasedIndex() == 0 {
		return nil, ErrNoActiveDrain
	}

	return decision, nil
}

// SyncDrainWAL makes an already applied drain marker durable in Pebble's WAL.
// The caller must capture LastPersistedIndex before calling this method and
// report only that captured index: later batches may commit during the fsync.
func (node *Node) SyncDrainWAL() error {
	return node.applier.Store().SyncWAL()
}
