package node

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"

	"github.com/formancehq/go-libs/v3/logging"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/commands"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/futures"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/spool"
	"github.com/formancehq/ledger-v3-poc/internal/storage/wal"
)

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
func newTestApplierSetup(t *testing.T, snapshotThreshold uint64) *testApplierSetup {
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

	// Persist audit config before creating the machine.
	auditBatch := pebbleStore.NewBatch()
	require.NoError(t, state.SaveAuditConfig(auditBatch, true))
	require.NoError(t, auditBatch.Commit())

	fsm, err := state.NewMachine(
		logger, pebbleStore, meter, nodeCache, nodeAttrs,
		1000, nil, state.NewSharedState(), state.NoopNotifier{}, state.NoopNotifier{}, state.NoopNotifier{}, 0,
	)
	require.NoError(t, err)

	// Create initial snapshot data and store it in the WAL.
	snapshotData, err := fsm.CreateSnapshot(context.Background())
	require.NoError(t, err)
	require.NoError(t, w.CreateSnapshot(0, &confState, snapshotData))

	initialStatus := atomic.Int32{}
	initialStatus.Store(statusNormal)

	if snapshotThreshold == 0 {
		snapshotThreshold = 1000
	}

	applier := &Applier{
		fsm:               fsm,
		spool:             defaultSpool,
		store:             pebbleStore,
		wal:               w,
		futures:           &SyncMap[uint64, *futures.Future[state.ApplyResult]]{},
		taskExecutor:      newSingleTaskExecutor(logger),
		logger:            logger,
		snapshotThreshold: snapshotThreshold,
		compactionMargin:  0,
		replayBatchSize:   1000,
		status:            &initialStatus,
		ch:                make(chan applyWork, 1),
	}

	// Initialize all metric instruments with noop meter.
	applier.applyEntriesHistogram, _ = meter.Int64Histogram("test.apply_entries")
	applier.applyEntriesBatchSizeCounter, _ = meter.Int64Counter("test.batch_size")
	applier.applyEntriesBatchSizeHistogram, _ = meter.Int64Histogram("test.batch_size_dist")
	applier.createSnapshotHistogram, _ = meter.Float64Histogram("test.create_snapshot")
	applier.snapshotTriggeredCounter, _ = meter.Int64Counter("test.snapshot_triggered")
	applier.unspoolDurationHistogram, _ = meter.Float64Histogram("test.unspool")
	applier.gatingWaitDurationHistogram, _ = meter.Int64Histogram("test.gating_wait")
	applier.readiesDuringGatingHistogram, _ = meter.Int64Histogram("test.readies_during_gating")
	applier.maintenanceSnapshotHistogram, _ = meter.Float64Histogram("test.maintenance_snapshot")
	applier.maintenanceReplaySpoolHistogram, _ = meter.Float64Histogram("test.maintenance_replay")

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
	setup := newTestApplierSetup(t, 0)

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
	setup := newTestApplierSetup(t, 0)

	// Set status to syncing before starting.
	setup.applier.status.Store(statusSyncing)

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

	setup := newTestApplierSetup(t, 0)

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

	setup := newTestApplierSetup(t, 0)

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
	setup := newTestApplierSetup(t, 0)

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
	setup := newTestApplierSetup(t, 0)

	// Create entry and register a future for the proposal ID.
	entry, proposalID := makeCreateLedgerEntry(t, 1, "future-ledger")
	future := futures.New[state.ApplyResult]()
	setup.applier.futures.Store(proposalID, future)

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

func TestApplierSnapshotGatingCycle(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	// Snapshot threshold of 5: after 5 applied entries, a snapshot is triggered.
	setup := newTestApplierSetup(t, 5)

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
