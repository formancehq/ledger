package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestEN3393ReproduceIdenticalCommittedEntries records the current FSM
// divergence when the same Raft entries meet different startup thresholds.
// The join checkpoint guard prevents this sequence on a fresh learner.
func TestEN3393ReproduceIdenticalCommittedEntries(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		initial   uint64
		wantEpoch uint64
		wantStale bool
	}{
		{"existing", 1000, 2, false},
		{"fresh joiner", 5000, 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			machine, store, _ := newTestMachineWithThreshold(t, tc.initial)
			installTestClusterPolicy(machine)
			for i, threshold := range []uint64{1000, 5000} {
				proposal := commands.NewCommand()
				proposal.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterConfig)
				proposal.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: threshold}}}}
				prepared, err := machine.PrepareEntries(context.Background(), store, makeEntry(t, uint64(i+1), proposal))
				require.NoError(t, err)
				require.NoError(t, machine.CommitPreparedBatch(context.Background(), prepared))
			}
			require.Equal(t, tc.wantEpoch, machine.Registry.Cache.Epoch())
			business := makeProposal(3, createLedgerOrder("en3393-ledger"))
			business.ExecutionPlan.CacheEpoch = 2
			prepared, err := machine.PrepareEntries(context.Background(), store, makeEntry(t, 3, business))
			require.NoError(t, err)
			require.Len(t, prepared.Result.Results, 1)
			if tc.wantStale {
				require.ErrorIs(t, prepared.Result.Results[0].Error, domain.ErrStaleProposal)
			} else {
				require.NoError(t, prepared.Result.Results[0].Error)
			}
			require.NoError(t, machine.CommitPreparedBatch(context.Background(), prepared))
			if tc.wantStale {
				require.Empty(t, listAuditEntries(t, store, 0))
			} else {
				require.NotEmpty(t, listAuditEntries(t, store, 0))
			}
		})
	}
}

func TestEN3393CheckpointRestoresDonorEpochBeforeNextEntry(t *testing.T) {
	t.Parallel()
	leader, leaderStore, _ := newTestMachineWithThreshold(t, 1000)
	for i, threshold := range []uint64{1000, 5000} {
		proposal := commands.NewCommand()
		proposal.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterConfig)
		proposal.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: threshold}}}}
		prepared, err := leader.PrepareEntries(t.Context(), leaderStore, makeEntry(t, uint64(i+1), proposal))
		require.NoError(t, err)
		require.NoError(t, leader.CommitPreparedBatch(t.Context(), prepared))
	}
	require.Equal(t, uint64(2), leader.Registry.Cache.Epoch())
	checkpointID, err := leaderStore.CreateSnapshot()
	require.NoError(t, err)
	checkpointPath, err := leaderStore.GetCheckpointPath(checkpointID)
	require.NoError(t, err)

	joiner, joinerStore, _ := newTestMachineWithThreshold(t, 5000)
	syncer := NewSynchronizer(joiner, NewRecovery(joiner, joinerStore), dal.NewIncomingRestoreFactory(joinerStore))
	index, err := syncer.SynchronizeWithLeader(t.Context(), &copyDirFetcher{srcDir: checkpointPath}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2), index)
	require.Equal(t, uint64(2), joiner.Registry.Cache.Epoch())
	require.Equal(t, uint64(5000), joiner.Registry.Cache.GenerationThreshold())

	installTestClusterPolicy(joiner)
	business := makeProposal(3, createLedgerOrder("en3393-checkpoint-ledger"))
	business.ExecutionPlan.CacheEpoch = leader.Registry.Cache.Epoch()
	prepared, err := joiner.PrepareEntries(t.Context(), joinerStore, makeEntry(t, 3, business))
	require.NoError(t, err)
	require.Len(t, prepared.Result.Results, 1)
	require.NoError(t, prepared.Result.Results[0].Error)
	require.NoError(t, joiner.CommitPreparedBatch(t.Context(), prepared))
}

func TestEN3393CheckpointPreservesHistoricalFirstConfigEpoch(t *testing.T) {
	t.Parallel()
	_, leaderStore, _ := newTestMachineWithThreshold(t, 1000)
	batch := leaderStore.OpenWriteSession()
	require.NoError(t, batch.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterConfig},
		&commonpb.PersistedClusterState{Config: &commonpb.ClusterConfig{RotationThreshold: 1000}, CacheEpoch: 1}))
	require.NoError(t, SetAppliedIndex(batch, 2))
	require.NoError(t, batch.Commit())
	checkpointID, err := leaderStore.CreateSnapshot()
	require.NoError(t, err)
	checkpointPath, err := leaderStore.GetCheckpointPath(checkpointID)
	require.NoError(t, err)

	joiner, joinerStore, _ := newTestMachineWithThreshold(t, 5000)
	syncer := NewSynchronizer(joiner, NewRecovery(joiner, joinerStore), dal.NewIncomingRestoreFactory(joinerStore))
	_, err = syncer.SynchronizeWithLeader(t.Context(), &copyDirFetcher{srcDir: checkpointPath}, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), joiner.Registry.Cache.Epoch(), "released persisted epoch must survive a fresh join")
	require.Equal(t, uint64(1000), joiner.Registry.Cache.GenerationThreshold())
}
