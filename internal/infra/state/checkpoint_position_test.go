package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestPrepareEntries_RejectsCheckpointMidProposal exercises the FSM-level
// safety net: a Proposal whose checkpoint trigger is not the last order must
// be rejected by applyProposal even if it slips past admission (replay of a
// pre-fix proposal, hand-crafted bypass, etc.).
func TestPrepareEntries_RejectsCheckpointMidProposal(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)
	ctx := context.Background()

	// Bootstrap a ledger so subsequent orders see consistent state.
	_, err := machine.ApplyEntries(ctx, makeEntry(t, 1, makeProposal(1, createLedgerOrder("ledger-a"))))
	require.NoError(t, err)

	// Forge a proposal with a checkpoint trigger followed by another order.
	bad := &raftcmdpb.Proposal{
		Id: 99,
		Orders: []*raftcmdpb.Order{
			{Type: &raftcmdpb.Order_CreateQueryCheckpoint{CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{}}},
			createLedgerOrder("ledger-b"),
		},
		Date: makeProposal(99).GetDate(),
	}

	_, err = machine.ApplyEntries(ctx, makeEntry(t, 2, bad))
	require.Error(t, err)
	require.Contains(t, err.Error(), "checkpoint trigger order not last")
}

// TestPrepareEntries_RejectsCheckpointMidBatch ensures the FSM refuses a
// PrepareEntries slice that places a checkpoint-trigger entry before the last
// position. The applier pre-split is the primary guard; this is the
// ceinture-bretelles inside the FSM.
func TestPrepareEntries_RejectsCheckpointMidBatch(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)
	ctx := context.Background()

	// Bootstrap a ledger.
	_, err := machine.ApplyEntries(ctx, makeEntry(t, 1, makeProposal(1, createLedgerOrder("ledger-x"))))
	require.NoError(t, err)

	// Two entries: the first is a pure checkpoint, the second is a no-op.
	checkpointEntry := makeEntry(t, 2, makeProposal(10,
		&raftcmdpb.Order{Type: &raftcmdpb.Order_CreateQueryCheckpoint{CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{}}},
	))
	followupEntry := makeEntry(t, 3, makeProposal(11, createLedgerOrder("ledger-y")))

	_, err = machine.PrepareEntries(ctx, checkpointEntry, followupEntry)
	require.Error(t, err)
	require.Contains(t, err.Error(), "applier must pre-split")
}

// TestPrepareEntries_RejectsCheckpointMidBatchLeavesStateUntouched verifies
// that when PrepareEntries refuses a slice with a misplaced checkpoint trigger,
// no in-memory FSM state has been mutated — neither lastAppliedIndex nor the
// sequence counter advances. The upfront ValidateCheckpointEntryPositions check
// runs before any mutation, so the rejection is observable as a pure no-op.
func TestPrepareEntries_RejectsCheckpointMidBatchLeavesStateUntouched(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)
	ctx := context.Background()

	_, err := machine.ApplyEntries(ctx, makeEntry(t, 1, makeProposal(1, createLedgerOrder("ledger-pre"))))
	require.NoError(t, err)

	indexBefore := machine.lastAppliedIndex
	sequenceBefore := machine.nextSequenceID

	checkpointEntry := makeEntry(t, 2, makeProposal(10,
		&raftcmdpb.Order{Type: &raftcmdpb.Order_CreateQueryCheckpoint{CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{}}},
	))
	tailEntry := makeEntry(t, 3, makeProposal(11, createLedgerOrder("ledger-tail")))

	_, err = machine.PrepareEntries(ctx, checkpointEntry, tailEntry)
	require.Error(t, err)
	require.Contains(t, err.Error(), "applier must pre-split")

	require.Equal(t, indexBefore, machine.lastAppliedIndex,
		"lastAppliedIndex must not advance when PrepareEntries rejects a malformed batch upfront")
	require.Equal(t, sequenceBefore, machine.nextSequenceID,
		"nextSequenceID must not advance when PrepareEntries rejects a malformed batch upfront")
}

// TestPrepareEntries_AcceptsCheckpointAsLastEntry confirms the new contract:
// a checkpoint-trigger entry as the last of a multi-entry slice goes through
// the normal pipelined path with CheckpointRequired set on the result.
func TestPrepareEntries_AcceptsCheckpointAsLastEntry(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)
	ctx := context.Background()

	_, err := machine.ApplyEntries(ctx, makeEntry(t, 1, makeProposal(1, createLedgerOrder("ledger-z"))))
	require.NoError(t, err)

	otherEntry := makeEntry(t, 2, makeProposal(2, createLedgerOrder("ledger-w")))
	checkpointEntry := makeEntry(t, 3, makeProposal(3,
		&raftcmdpb.Order{Type: &raftcmdpb.Order_CreateQueryCheckpoint{CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{}}},
	))

	result, err := machine.ApplyEntries(ctx, otherEntry, checkpointEntry)
	require.NoError(t, err)
	require.True(t, result.CheckpointRequired, "checkpoint trigger as last entry must set CheckpointRequired")
	require.NotZero(t, result.QueryCheckpointID, "QueryCheckpointID should propagate")
}
