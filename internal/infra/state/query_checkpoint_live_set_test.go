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
