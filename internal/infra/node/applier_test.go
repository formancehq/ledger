package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/commands"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/futures"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/spool"
	"github.com/formancehq/ledger-v3-poc/internal/storage/wal"
)

type noopNotifier struct{}

func (noopNotifier) NotifyLogsCommitted(uint64) {}
func (noopNotifier) NotifyConfigChanged()       {}

func listLedgerContains(s *dal.Store, name string) bool {
	cursor, err := query.ReadLedgers(context.Background(), s)
	if err != nil {
		return false
	}

	defer func() { _ = cursor.Close() }()

	for {
		ledger, err := cursor.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return false
		}

		if ledger.GetName() == name {
			return true
		}
	}

	return false
}

// testApplierSetup holds all the infrastructure needed to test the Applier in isolation.
type testApplierSetup struct {
	applier   *Applier
	store     *dal.Store
	wal       wal.WAL
	spool     *spool.Default
	fsm       *state.Machine
	stop      chan struct{}
	confState *raftpb.ConfState
}

// newTestApplierSetup creates a minimal Applier with real infrastructure (Pebble, WAL, spool, FSM).
func newTestApplierSetup(t *testing.T) *testApplierSetup {
	t.Helper()

	logger := logging.Testing()
	meter := noop.Meter{}

	walDir := t.TempDir()
	dataDir := t.TempDir()
	spoolDir := t.TempDir()

	w, err := wal.New(walDir, logger, meter)
	require.NoError(t, err)

	defaultSpool, err := spool.NewDefault(spool.DefaultSpoolConfig{Dir: spoolDir})
	require.NoError(t, err)

	pebbleStore, err := dal.NewStore(dataDir, logger, meter, dal.DefaultConfig())
	require.NoError(t, err)

	// Create initial snapshot at index 0 so the WAL is initialized.
	confState := raftpb.ConfState{Voters: []uint64{1}}

	nodeCache, err := cache.New(1000, nil)
	require.NoError(t, err)

	nodeAttrs := attributes.New()

	fsm, err := state.NewMachine(
		logger, pebbleStore, meter, nodeCache, nodeAttrs,
		nil, state.NewSharedState(), noopNotifier{}, nil, 0, false, 0,
	)
	require.NoError(t, err)

	// Create initial snapshot (no FSM data) in the WAL.
	require.NoError(t, w.CreateSnapshot(0, &confState, nil))

	applier, err := NewApplier(
		fsm, defaultSpool, pebbleStore, w, logger, meter,
		0, 1000, nil,
	)
	require.NoError(t, err)

	stop := make(chan struct{})

	t.Cleanup(func() {
		_ = pebbleStore.Close()
		_ = defaultSpool.Close()
		_ = w.Close()
	})

	return &testApplierSetup{
		applier:   applier,
		store:     pebbleStore,
		wal:       w,
		spool:     defaultSpool,
		fsm:       fsm,
		stop:      stop,
		confState: &confState,
	}
}

// makeCreateLedgerEntry creates a valid raftpb.Entry containing a CreateLedgerOrder.
// Returns the entry and the proposal ID for future registration.
func makeCreateLedgerEntry(t *testing.T, index uint64, name string) (raftpb.Entry, uint64) {
	t.Helper()

	cmd := commands.NewCommand(&raftcmdpb.Order{
		Type: &raftcmdpb.Order_CreateLedger{
			CreateLedger: &raftcmdpb.CreateLedgerOrder{
				Name: name,
			},
		},
	})

	data, err := cmd.MarshalVT()
	require.NoError(t, err)

	return raftpb.Entry{
		Term:  1,
		Index: index,
		Type:  raftpb.EntryNormal,
		Data:  data,
	}, cmd.GetId()
}

func TestApplierRunAppliesEntries(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)

	// Start the Run goroutine.
	runDone := make(chan error, 1)

	go func() {
		runDone <- setup.applier.Run(ctx, setup.stop)
	}()

	// Submit 3 CreateLedger entries.
	for i := uint64(1); i <= 3; i++ {
		entry, _ := makeCreateLedgerEntry(t, i, fmt.Sprintf("ledger-%d", i))
		setup.applier.Submit([]raftpb.Entry{entry}, nil, setup.stop)
	}

	// Drain to ensure all entries are processed.
	setup.applier.Drain(setup.stop)

	// Verify all 3 ledgers exist in the store.
	for i := uint64(1); i <= 3; i++ {
		require.True(t, listLedgerContains(setup.store, fmt.Sprintf("ledger-%d", i)),
			"ledger-%d should exist after apply", i)
	}

	// Stop and verify Run returns nil.
	close(setup.stop)

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after stop")
	}
}

func TestApplierRunSpoolsWhenNotNormal(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)

	// Set status to syncing before starting.
	setup.applier.SetOutOfSync() // Set to non-normal status for spooling test

	// Start the Run goroutine.
	runDone := make(chan error, 1)

	go func() {
		runDone <- setup.applier.Run(ctx, setup.stop)
	}()

	// Submit 3 CreateLedger entries.
	for i := uint64(1); i <= 3; i++ {
		entry, _ := makeCreateLedgerEntry(t, i, fmt.Sprintf("ledger-%d", i))
		setup.applier.Submit([]raftpb.Entry{entry}, nil, setup.stop)
	}

	// Drain to ensure all entries are processed.
	setup.applier.Drain(setup.stop)

	// Verify ledgers do NOT exist in store (they were spooled, not applied).
	for i := uint64(1); i <= 3; i++ {
		require.False(t, listLedgerContains(setup.store, fmt.Sprintf("ledger-%d", i)),
			"ledger-%d should NOT exist when spooling", i)
	}

	// Verify spool has data (End position advanced).
	pos, err := setup.spool.End()
	require.NoError(t, err)
	require.Positive(t, pos.Offset, "spool should have data (offset=%d)", pos.Offset)

	// Stop.
	close(setup.stop)

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after stop")
	}
}

func TestApplierDrainAbortsOnStop(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)

	// Close stop before calling Drain.
	close(setup.stop)

	drainDone := make(chan struct{})

	go func() {
		setup.applier.Drain(setup.stop)
		close(drainDone)
	}()

	select {
	case <-drainDone:
		// Drain returned as expected.
	case <-time.After(1 * time.Second):
		t.Fatal("Drain did not return when stop is closed")
	}
}

func TestApplierSubmitAbortsOnStop(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)

	// Fill the channel buffer manually.
	entry, _ := makeCreateLedgerEntry(t, 1, "filler")
	setup.applier.ch <- applyWork{entries: []raftpb.Entry{entry}}

	// Close stop.
	close(setup.stop)

	// Submit should return immediately via stop.
	submitDone := make(chan struct{})

	go func() {
		entry2, _ := makeCreateLedgerEntry(t, 2, "blocked")
		setup.applier.Submit([]raftpb.Entry{entry2}, nil, setup.stop)
		close(submitDone)
	}()

	select {
	case <-submitDone:
		// Submit returned as expected.
	case <-time.After(1 * time.Second):
		t.Fatal("Submit did not return when stop is closed and channel is full")
	}
}

func TestApplierRunExitsOnStop(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)

	runDone := make(chan error, 1)

	go func() {
		runDone <- setup.applier.Run(ctx, setup.stop)
	}()

	// Close stop and verify Run returns nil.
	close(setup.stop)

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not exit after stop")
	}
}

func TestApplierFutureResolution(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)

	// Create entry and register a future for the proposal ID.
	entry, proposalID := makeCreateLedgerEntry(t, 1, "future-ledger")
	future := futures.New[state.ApplyResult]()
	setup.applier.StoreFuture(proposalID, future)

	// Start the Run goroutine.
	runDone := make(chan error, 1)

	go func() {
		runDone <- setup.applier.Run(ctx, setup.stop)
	}()

	// Submit the entry.
	setup.applier.Submit([]raftpb.Entry{entry}, nil, setup.stop)

	// Wait for the future to resolve.
	resultCh := make(chan state.ApplyResult, 1)
	errCh := make(chan error, 1)

	go func() {
		result, err := future.Wait()
		if err != nil {
			errCh <- err

			return
		}

		resultCh <- result
	}()

	select {
	case result := <-resultCh:
		require.NotEmpty(t, result.Logs, "future result should have non-empty Logs")
	case err := <-errCh:
		t.Fatalf("future resolved with error: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("future did not resolve in time")
	}

	// Stop.
	close(setup.stop)

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after stop")
	}
}

// makeCreateQueryCheckpointEntry creates a valid raftpb.Entry containing a CreateQueryCheckpointOrder.
func makeCreateQueryCheckpointEntry(t *testing.T, index uint64) (raftpb.Entry, uint64) {
	t.Helper()

	cmd := commands.NewCommand(&raftcmdpb.Order{
		Type: &raftcmdpb.Order_CreateQueryCheckpoint{
			CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
		},
	})

	data, err := cmd.MarshalVT()
	require.NoError(t, err)

	return raftpb.Entry{
		Term:  1,
		Index: index,
		Type:  raftpb.EntryNormal,
		Data:  data,
	}, cmd.GetId()
}

// TestApplierCascadingQueryCheckpointsDuringReplay verifies that entries after
// a second CreateQueryCheckpoint are not lost during spool replay.
//
// Regression test: handleQueryCheckpointDuringReplay did not loop on cascading
// checkpoints, so RemainingEntries from the second checkpoint were silently
// dropped.
//
// The test bypasses the Run loop to avoid timing issues: it applies the first
// entry directly, fills the spool with entries containing two query checkpoints,
// then calls replaySpool to exercise the exact buggy code path.
func TestApplierCascadingQueryCheckpointsDuringReplay(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)

	// Entry 1: CreateLedger — applied directly so the FSM has lastAppliedIndex=1.
	e1, _ := makeCreateLedgerEntry(t, 1, "qcp-ledger")
	_, err := setup.fsm.ApplyEntries(ctx, e1)
	require.NoError(t, err)

	// Entries 2-5: fill the spool as if they arrived during a maintenance task.
	//   2 = CreateLedger "before-first-cp"
	//   3 = CreateQueryCheckpoint (first)
	//   4 = CreateLedger "between-cps"
	//   5 = CreateQueryCheckpoint (second, cascading)
	//   6 = CreateLedger "after-second-cp"  ← lost without the fix
	e2, _ := makeCreateLedgerEntry(t, 2, "before-first-cp")
	e3, _ := makeCreateQueryCheckpointEntry(t, 3)
	e4, _ := makeCreateLedgerEntry(t, 4, "between-cps")
	e5, _ := makeCreateQueryCheckpointEntry(t, 5)
	e6, _ := makeCreateLedgerEntry(t, 6, "after-second-cp")

	require.NoError(t, setup.spool.AppendCommittedEntries(ctx, e2, e3, e4, e5, e6))

	// Replay the spool starting after lastAppliedIndex=1.
	err = setup.applier.replaySpool(ctx, 1)
	require.NoError(t, err)

	// All ledgers must exist — including "after-second-cp".
	require.True(t, listLedgerContains(setup.store, "qcp-ledger"), "qcp-ledger should exist")
	require.True(t, listLedgerContains(setup.store, "before-first-cp"), "before-first-cp should exist")
	require.True(t, listLedgerContains(setup.store, "between-cps"), "between-cps should exist")
	require.True(t, listLedgerContains(setup.store, "after-second-cp"),
		"after-second-cp should exist — it was lost before the cascading checkpoint fix")
}

func TestApplierSnapshotGatingCycle(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	// Snapshot threshold of 5: after 5 applied entries, a snapshot is triggered.
	setup := newTestApplierSetup(t)

	// Start the Run goroutine.
	runDone := make(chan error, 1)

	go func() {
		runDone <- setup.applier.Run(ctx, setup.stop)
	}()

	// Submit 8 entries sequentially.
	// Entries 1-5 are applied directly. Entry 5 triggers snapshot + gating.
	// Entries 6-8 are spooled during gating, then replayed after unspool.
	for i := uint64(1); i <= 8; i++ {
		entry, _ := makeCreateLedgerEntry(t, i, fmt.Sprintf("gating-ledger-%d", i))
		setup.applier.Submit([]raftpb.Entry{entry}, setup.confState, setup.stop)
	}

	// Eventually all 8 ledgers should exist (after unspool replays the spooled entries).
	require.Eventually(t, func() bool {
		for i := uint64(1); i <= 8; i++ {
			if !listLedgerContains(setup.store, fmt.Sprintf("gating-ledger-%d", i)) {
				return false
			}
		}

		return true
	}, 30*time.Second, 100*time.Millisecond, "all 8 ledgers should exist after snapshot gating cycle")

	// Verify status returned to normal.
	require.Eventually(t, func() bool {
		return setup.applier.Status() == statusNormal
	}, 5*time.Second, 50*time.Millisecond, "status should return to normal after gating")

	// Stop.
	close(setup.stop)

	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after stop")
	}
}
