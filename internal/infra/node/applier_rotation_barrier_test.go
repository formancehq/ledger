package node

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
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

func TestFindApplyBoundaryKeepsUnchangedThresholdConfigInBatch(t *testing.T) {
	t.Parallel()
	decoded := []state.DecodedEntry{
		{Entry: &raftpb.Entry{Index: new(uint64(1))}},
		{
			Entry: &raftpb.Entry{Index: new(uint64(2))},
			Proposal: &raftcmdpb.Proposal{TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
				Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{
					ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: 1000},
				},
			}}},
		},
		{Entry: &raftpb.Entry{Index: new(uint64(3))}},
	}

	require.Equal(t, len(decoded), findApplyBoundaryDecoded(decoded, 1000))
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
	require.Never(t, func() bool {
		return setup.fsm.Registry.Cache.CurrentGeneration() != 0
	}, 100*time.Millisecond, 5*time.Millisecond)
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

// TestRotationPreloadReadsCommittedValue checks the original stale-seed
// symptom. The write at index 3 is in the cache while its Pebble commit is
// withheld. At index 5, rotation moves it out of Gen0. For a proposal at
// index 9, CheckCache then requires a Pebble seed; that seed must contain the
// index-3 value. The read below is the cache-verdict + store-read path used
// by plan.Builder (which cannot be imported by node tests without a cycle).
func TestRotationPreloadReadsCommittedValue(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)
	setup.fsm.Registry.Cache.SetGenerationThreshold(4)
	ledgerName := "pre-rotation-ledger"
	ledgerKey := domain.LedgerKey{Name: ledgerName}
	ledgerID, _ := attributes.MakeKey(ledgerKey.Bytes())
	create, _ := makeCreateLedgerEntry(t, 3, ledgerName)
	entries := []*raftpb.Entry{
		{Index: new(uint64(1)), Type: new(raftpb.EntryNormal)},
		{Index: new(uint64(2)), Type: new(raftpb.EntryNormal)},
		create,
		{Index: new(uint64(4)), Type: new(raftpb.EntryNormal)},
		{Index: new(uint64(5)), Type: new(raftpb.EntryNormal)},
	}
	decoded, err := state.DecodeEntries(entries)
	require.NoError(t, err)
	result := make(chan error, 1)
	go func() {
		result <- setup.applier.applyDecodedEntriesToFSM(ctx, setup.stop, setup.confState, nil, decoded)
	}()

	readSeed := func() (cache.CacheStatus, *commonpb.LedgerInfo, error) {
		status := setup.fsm.Registry.Cache.Ledgers.CheckCache(9, ledgerID)
		handle, err := setup.store.NewReadHandle()
		if err != nil {
			return status, nil, err
		}
		defer func() { _ = handle.Close() }()
		seed, err := setup.fsm.Registry.Attrs.Ledger.Get(handle, ledgerKey.Bytes())

		return status, seed, err
	}

	var previous commitWork
	select {
	case previous = <-setup.applier.commitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("pre-rotation batch was not prepared")
	}
	// Give the applier time to reach a forbidden early rotation. Keep draining
	// its commit work even if this assertion fails, so the test cannot hang.
	rotationWaited := assert.Never(t, func() bool {
		return setup.fsm.Registry.Cache.CurrentGeneration() != 0
	}, 100*time.Millisecond, 5*time.Millisecond)
	status, seed, seedErr := readSeed()
	observedRotation := !rotationWaited

	serviceCommit := func(work commitWork) {
		if !observedRotation && setup.fsm.Registry.Cache.CurrentGeneration() != 0 {
			status, seed, seedErr = readSeed()
			observedRotation = true
		}
		err := setup.fsm.CommitPreparedBatch(ctx, work.prepared)
		work.done <- err
		require.NoError(t, err)
	}
	serviceCommit(previous)

	// The apply loop can submit more than one batch before returning. Service
	// every submission, including on the regression path, before asserting.
	var applyErr error
	deadline := time.After(5 * time.Second)
	applyDone := false
	for !applyDone {
		select {
		case work := <-setup.applier.commitCh:
			serviceCommit(work)
		case applyErr = <-result:
			applyDone = true
		case <-deadline:
			t.Fatal("applier did not finish after draining commit work")
		}
	}
	// result is sent only after the applier has queued its final commit. Both
	// channels may be ready in the select above, so drain the buffered work
	// before waiting on the final commit's completion.
drainQueued:
	for {
		select {
		case work := <-setup.applier.commitCh:
			serviceCommit(work)
		default:
			break drainQueued
		}
	}
	require.NoError(t, applyErr)
	require.NoError(t, setup.applier.waitPendingCommit(ctx))
	require.True(t, observedRotation, "the test must observe a cache rotation")
	require.Equal(t, cache.CacheMiss, status, "index 9 cannot use this Gen1-only ledger")
	require.NoError(t, seedErr, "the required preload seed must already be durable")
	require.NotNil(t, seed)
	require.Equal(t, ledgerName, seed.GetName())
}

type replayRotationNotifier struct {
	onLogsCommitted func()
}

func (n *replayRotationNotifier) NotifyLogsCommitted(uint64) {
	if n.onLogsCommitted != nil {
		n.onLogsCommitted()
	}
}

func (*replayRotationNotifier) NotifyConfigChanged() {}

// TestReplayRotationCommitsBeforeEviction exercises the synchronous WAL/spool
// replay path. A checkpoint-only split would prepare the whole slice, rotate
// at index 5, and commit once at index 7. The notification after each commit
// observes that indexes 1-4 are durable while the cache is still in Gen0.
func TestReplayRotationCommitsBeforeEviction(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()
	notifier := &replayRotationNotifier{}
	setup := newTestApplierSetupWithNotifier(t, make(LocalResponses, 1), notifier)
	setup.fsm.Registry.Cache.SetGenerationThreshold(4)

	type commitObservation struct {
		persistedIndex uint64
		generation     uint64
		firstPersisted bool
		lastPersisted  bool
	}
	var commits []commitObservation
	notifier.onLogsCommitted = func() {
		commits = append(commits, commitObservation{
			persistedIndex: setup.fsm.LastPersistedIndex(),
			generation:     setup.fsm.Registry.Cache.CurrentGeneration(),
			firstPersisted: listLedgerContains(setup.store, "replay-ledger-a"),
			lastPersisted:  listLedgerContains(setup.store, "replay-ledger-g"),
		})
	}

	entries := make([]*raftpb.Entry, 7)
	for i := range entries {
		entries[i], _ = makeCreateLedgerEntry(t, uint64(i+1), "replay-ledger-"+string(rune('a'+i)))
	}
	require.NoError(t, setup.applier.applyReplayEntries(ctx, entries))
	require.Equal(t, []commitObservation{
		{persistedIndex: 4, generation: 0, firstPersisted: true, lastPersisted: false},
		{persistedIndex: 7, generation: 1, firstPersisted: true, lastPersisted: true},
	}, commits)
	require.True(t, listLedgerContains(setup.store, "replay-ledger-g"), "the entire replay tail must apply")
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

func TestThresholdResetDoesNotPrepareAfterFailedCommit(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)
	previousError := errors.New("previous Pebble commit failed")
	done := make(chan error, 1)
	done <- previousError
	setup.applier.pending = &pendingCommit{done: done}

	proposal := &raftcmdpb.Proposal{TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
		Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{
			ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: 2},
		},
	}}}
	data, err := proposal.MarshalVT()
	require.NoError(t, err)
	decoded, err := state.DecodeEntries([]*raftpb.Entry{{
		Index: new(uint64(2)), Type: new(raftpb.EntryNormal), Data: data,
	}})
	require.NoError(t, err)

	epoch := setup.fsm.Registry.Cache.Epoch()
	err = setup.applier.applyDecodedEntriesToFSM(ctx, setup.stop, setup.confState, nil, decoded)
	require.ErrorIs(t, err, previousError)
	require.Equal(t, epoch, setup.fsm.Registry.Cache.Epoch())
	require.Equal(t, uint64(1000), setup.fsm.Registry.Cache.GenerationThreshold())
}

func TestThresholdResetWaitsForPreviousCommit(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)
	entry, _ := makeCreateLedgerEntry(t, 1, "before-reset")
	proposal := commands.NewCommand()
	proposal.CallerSnapshot = commands.SystemCallerSnapshot(commands.ComponentClusterConfig)
	proposal.TechnicalUpdates = []*raftcmdpb.TechnicalUpdate{{
		Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{
			ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: 2},
		},
	}}
	data, err := proposal.MarshalVT()
	require.NoError(t, err)
	configEntry := &raftpb.Entry{Index: new(uint64(2)), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data}
	decoded, err := state.DecodeEntries([]*raftpb.Entry{entry, configEntry})
	require.NoError(t, err)

	epoch := setup.fsm.Registry.Cache.Epoch()
	result := make(chan error, 1)
	go func() {
		result <- setup.applier.applyDecodedEntriesToFSM(ctx, setup.stop, setup.confState, nil, decoded)
	}()

	var previous commitWork
	select {
	case previous = <-setup.applier.commitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("pre-reset batch was not prepared")
	}
	require.Never(t, func() bool { return setup.fsm.Registry.Cache.Epoch() != epoch }, 100*time.Millisecond, 5*time.Millisecond)
	require.NoError(t, setup.fsm.CommitPreparedBatch(ctx, previous.prepared))
	previous.done <- nil

	var configCommit commitWork
	select {
	case configCommit = <-setup.applier.commitCh:
	case <-time.After(5 * time.Second):
		t.Fatal("config batch was not prepared after the previous commit")
	}
	require.True(t, listLedgerContains(setup.store, "before-reset"))
	require.Equal(t, epoch+1, setup.fsm.Registry.Cache.Epoch())
	require.Equal(t, uint64(2), setup.fsm.Registry.Cache.GenerationThreshold())
	require.NoError(t, setup.fsm.CommitPreparedBatch(ctx, configCommit.prepared))
	configCommit.done <- nil
	require.NoError(t, <-result)
	require.NoError(t, setup.applier.waitPendingCommit(ctx))
}
