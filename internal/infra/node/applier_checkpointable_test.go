package node

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/state"
)

// TestApplierWaitForCheckpointable pins which applier states may serve a
// checkpoint: a store behind its Raft log (out of sync, fetching a
// checkpoint, installing a snapshot) refuses at once; a store paused for a
// local maintenance task is still a valid donor.
func TestApplierWaitForCheckpointable(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	setup := newTestApplierSetup(t)
	applier := setup.applier

	require.NoError(t, applier.WaitForCheckpointable(ctx, 0))

	applier.setOutOfSync()
	require.ErrorIs(t, applier.WaitForCheckpointable(ctx, 0), state.ErrStoreResynchronizing)

	applier.status.Store(statusGated)
	applier.gatingReason.Store(gatingReasonQueryCheckpoint)
	require.NoError(t, applier.WaitForCheckpointable(ctx, 0))

	applier.gatingReason.Store(gatingReasonSyncing)
	require.ErrorIs(t, applier.WaitForCheckpointable(ctx, 0), state.ErrStoreResynchronizing)

	applier.status.Store(statusInstallingSnapshot)
	require.ErrorIs(t, applier.WaitForCheckpointable(ctx, 0), state.ErrStoreResynchronizing)
}
