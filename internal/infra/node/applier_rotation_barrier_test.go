package node

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestFindApplyBoundarySplitsBeforeRotation(t *testing.T) {
	t.Parallel()
	entries := make([]*raftpb.Entry, 5)
	for i := range entries {
		entries[i], _ = makeCreateLedgerEntry(t, uint64(i+1), "ledger-"+string(rune('a'+i)))
	}
	decoded, err := state.DecodeEntries(entries)
	require.NoError(t, err)
	defer state.ReleaseDecodedEntries(decoded)

	require.Equal(t, 2, findApplyBoundaryDecoded(decoded, 2))
	require.Equal(t, 2, findApplyBoundaryDecoded(decoded[2:], 2))
	require.Equal(t, 1, findApplyBoundaryDecoded(decoded[4:], 2))
}

func TestFindApplyBoundaryRechecksThresholdAfterConfig(t *testing.T) {
	t.Parallel()
	decoded := []state.DecodedEntry{
		{Entry: &raftpb.Entry{Index: new(uint64(1))}},
		{
			Entry: &raftpb.Entry{Index: new(uint64(2))},
			Proposal: &raftcmdpb.Proposal{TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
				Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{
					ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: 2},
				},
			}}},
		},
		{Entry: &raftpb.Entry{Index: new(uint64(3))}},
	}
	require.Equal(t, 1, findApplyBoundaryDecoded(decoded, 1000))
	require.Equal(t, 1, findApplyBoundaryDecoded(decoded[1:], 1000))
	require.Equal(t, 1, findApplyBoundaryDecoded(decoded[2:], 2))
}

func TestRotationWaitsForPreviousCommit(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)
	setup.fsm.Registry.Cache.SetGenerationThreshold(2)

	entries := make([]*raftpb.Entry, 3)
	for i := range entries {
		entries[i], _ = makeCreateLedgerEntry(t, uint64(i+1), "rotation-ledger-"+string(rune('a'+i)))
	}
	decoded, err := state.DecodeEntries(entries)
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		result <- setup.applier.applyDecodedEntriesToFSM(ctx, setup.stop, setup.confState, nil, decoded)
	}()

	var previous commitWork
	select {
	case previous = <-setup.applier.commitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("pre-rotation batch was not prepared")
	}
	require.Equal(t, uint64(0), setup.fsm.Registry.Cache.CurrentGeneration())
	require.False(t, listLedgerContains(setup.store, "rotation-ledger-a"))

	// The rotating entry is in the same Raft apply work item. It must not
	// prepare (and rotate the cache) until the preceding batch is durable.
	require.NoError(t, setup.fsm.CommitPreparedBatch(ctx, previous.prepared))
	previous.done <- nil

	var rotating commitWork
	select {
	case rotating = <-setup.applier.commitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("rotating batch was not prepared after the previous commit")
	}
	require.True(t, listLedgerContains(setup.store, "rotation-ledger-a"))
	require.Equal(t, uint64(1), setup.fsm.Registry.Cache.CurrentGeneration())
	require.NoError(t, setup.fsm.CommitPreparedBatch(ctx, rotating.prepared))
	rotating.done <- nil
	require.NoError(t, <-result)
	require.NoError(t, setup.applier.waitPendingCommit(ctx))
}

func TestRotationDoesNotPrepareAfterFailedCommit(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)
	setup.fsm.Registry.Cache.SetGenerationThreshold(2)
	setup.fsm.Registry.Cache.SetCurrentGeneration(0)
	previousError := errors.New("previous Pebble commit failed")
	done := make(chan error, 1)
	done <- previousError
	setup.applier.pending = &pendingCommit{done: done}
	entry, _ := makeCreateLedgerEntry(t, 3, "must-not-rotate")
	decoded, err := state.DecodeEntries([]*raftpb.Entry{entry})
	require.NoError(t, err)

	err = setup.applier.applyDecodedEntriesToFSM(ctx, setup.stop, setup.confState, nil, decoded)
	require.ErrorIs(t, err, previousError)
	require.Equal(t, uint64(0), setup.fsm.Registry.Cache.CurrentGeneration())
}
