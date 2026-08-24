package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Recovery rehydrates the live query-checkpoint set from the stored rows, so the
// FSM's cap and existence checks survive a restart.
func TestRecoverState_QueryCheckpointLiveSet(t *testing.T) {
	t.Parallel()

	machine, store, _ := newTestMachine(t)

	require.Empty(t, machine.State.LiveQueryCheckpointIDs, "a fresh store has no live checkpoints")

	batch := store.OpenWriteSession()
	require.NoError(t, SaveQueryCheckpoint(batch, &raftcmdpb.QueryCheckpointState{
		CheckpointId: 3, MaxSequence: 30, CreatedAt: &commonpb.Timestamp{Data: 300},
	}))
	require.NoError(t, SaveQueryCheckpoint(batch, &raftcmdpb.QueryCheckpointState{
		CheckpointId: 7, MaxSequence: 70, CreatedAt: &commonpb.Timestamp{Data: 700},
	}))
	require.NoError(t, batch.Commit())

	require.NoError(t, NewRecovery(machine, store).RecoverState())

	require.Len(t, machine.State.LiveQueryCheckpointIDs, 2)
	require.Contains(t, machine.State.LiveQueryCheckpointIDs, uint64(3))
	require.Contains(t, machine.State.LiveQueryCheckpointIDs, uint64(7))
}

// The live count folds this proposal's staged creates and deletes exactly: a
// create followed by a delete of that same new id nets to zero, so a later
// create in the same proposal is not spuriously capped.
func TestWriteSet_LiveQueryCheckpointCount_FoldsStagedChanges(t *testing.T) {
	t.Parallel()

	machine, _, _ := newTestMachine(t)
	machine.State.LiveQueryCheckpointIDs = map[uint64]struct{}{1: {}, 2: {}}

	buf := NewWriteSet(machine)
	require.Equal(t, uint64(2), buf.LiveQueryCheckpointCount())

	// Create a new id, then delete that same new id: net zero.
	buf.SaveQueryCheckpoint(&raftcmdpb.QueryCheckpointState{CheckpointId: 3})
	require.Equal(t, uint64(3), buf.LiveQueryCheckpointCount())
	buf.DeleteQueryCheckpoint(3)
	require.Equal(t, uint64(2), buf.LiveQueryCheckpointCount(),
		"a create then delete of the same new id must net to zero")
	require.False(t, buf.QueryCheckpointExists(3))

	// Deleting a committed id frees a slot.
	buf.DeleteQueryCheckpoint(1)
	require.Equal(t, uint64(1), buf.LiveQueryCheckpointCount())
	require.False(t, buf.QueryCheckpointExists(1))
	require.True(t, buf.QueryCheckpointExists(2))
}
