package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestReclaimOrphanedQueryCheckpoints pins that recovery removes physical
// query-checkpoint directories with no live row (the snapshot-install orphan
// case) while keeping the ones that are still live. The live set is read from
// Pebble, so only id 2 has a persisted row here.
func TestReclaimOrphanedQueryCheckpoints(t *testing.T) {
	t.Parallel()

	machine, dataStore, _ := newTestMachine(t)

	// Physical directories for 1, 2, 3; only 2 has a live Pebble row.
	for _, id := range []uint64{1, 2, 3} {
		_, err := dataStore.CreateQueryCheckpoint(id)
		require.NoError(t, err)
	}

	batch := dataStore.OpenWriteSession()
	require.NoError(t, SaveQueryCheckpoint(batch, &raftcmdpb.QueryCheckpointState{CheckpointId: 2}))
	require.NoError(t, batch.Commit())

	handle, err := dataStore.NewReadHandle()
	require.NoError(t, err)
	t.Cleanup(func() { _ = handle.Close() })

	NewRecovery(machine, dataStore).reclaimOrphanedQueryCheckpoints(handle)

	remaining, err := dataStore.ListQueryCheckpointDirs()
	require.NoError(t, err)
	require.ElementsMatch(t, []uint64{2}, remaining,
		"orphaned directories 1 and 3 must be reclaimed; the live one (2) kept")
}
