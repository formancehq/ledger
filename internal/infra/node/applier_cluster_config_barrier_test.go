package node

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func makeClusterConfigEntry(t *testing.T, index, threshold uint64) *raftpb.Entry {
	t.Helper()
	proposal := commands.NewCommand()
	proposal.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterConfig)
	proposal.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{
		Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{
			ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: threshold},
		},
	}}
	data, err := proposal.MarshalVT()
	require.NoError(t, err)

	return &raftpb.Entry{Index: new(index), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data}
}

func TestFindApplyBoundarySplitsClusterConfigAndCheckpoint(t *testing.T) {
	t.Parallel()
	write, _ := makeCreateLedgerEntry(t, 1, "before-config")
	config := makeClusterConfigEntry(t, 2, 10)
	checkpoint, _ := makeCreateQueryCheckpointEntry(t, 3)
	decoded, err := state.DecodeEntries([]*raftpb.Entry{write, config, checkpoint})
	require.NoError(t, err)
	defer state.ReleaseDecodedEntries(decoded)

	length, drain := findApplyBoundaryDecoded(decoded)
	require.Equal(t, 1, length)
	require.False(t, drain)
	length, drain = findApplyBoundaryDecoded(decoded[1:])
	require.Equal(t, 1, length)
	require.True(t, drain)
	length, drain = findApplyBoundaryDecoded(decoded[2:])
	require.Equal(t, 1, length)
	require.False(t, drain)
}

func TestClusterConfigWaitsForPreviousCommitBeforeReset(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)
	const nextThreshold = uint64(10)

	// Both entries arrive in one Raft apply work item. The first preparation
	// updates the in-memory ledger cache, but its Pebble batch is held below.
	write, _ := makeCreateLedgerEntry(t, 1, "pending-before-reset")
	config := makeClusterConfigEntry(t, 2, nextThreshold)
	decoded, err := state.DecodeEntries([]*raftpb.Entry{write, config})
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		result <- setup.applier.applyDecodedEntriesToFSM(ctx, setup.stop, setup.confState, nil, decoded)
	}()

	var previous commitWork
	select {
	case previous = <-setup.applier.commitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("preceding write batch was not prepared")
	}
	require.Equal(t, uint64(1000), setup.fsm.Registry.Cache.GenerationThreshold(),
		"the config reset must wait while the preceding cache write is not durable")
	require.False(t, listLedgerContains(setup.store, "pending-before-reset"))

	require.NoError(t, setup.fsm.CommitPreparedBatch(ctx, previous.prepared))
	previous.done <- nil

	var configWork commitWork
	select {
	case configWork = <-setup.applier.commitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("cluster config was not prepared after the preceding commit")
	}
	require.True(t, listLedgerContains(setup.store, "pending-before-reset"),
		"the earlier write must be visible in Pebble before the reset")
	require.Equal(t, nextThreshold, setup.fsm.Registry.Cache.GenerationThreshold())
	require.NoError(t, setup.fsm.CommitPreparedBatch(ctx, configWork.prepared))
	configWork.done <- nil
	require.NoError(t, <-result)
	require.NoError(t, setup.applier.waitPendingCommit(ctx))
}

func TestClusterConfigDoesNotPrepareAfterFailedPreviousCommit(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)
	previousError := errors.New("previous Pebble commit failed")
	done := make(chan error, 1)
	done <- previousError
	setup.applier.pending = &pendingCommit{done: done}
	config := makeClusterConfigEntry(t, 1, 10)
	decoded, err := state.DecodeEntries([]*raftpb.Entry{config})
	require.NoError(t, err)

	err = setup.applier.applyDecodedEntriesToFSM(ctx, setup.stop, setup.confState, nil, decoded)
	require.ErrorIs(t, err, previousError)
	require.Equal(t, uint64(1000), setup.fsm.Registry.Cache.GenerationThreshold(),
		"a failed previous commit must not let config preparation clear the cache")
}
