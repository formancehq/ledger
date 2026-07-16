package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/commands"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/spool"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

func newNoopNotifier(t *testing.T) *MockNotifier {
	t.Helper()

	n := NewMockNotifier(gomock.NewController(t))
	n.EXPECT().NotifyLogsCommitted(gomock.Any()).AnyTimes()
	n.EXPECT().NotifyConfigChanged().AnyTimes()

	return n
}

func listLedgerContains(s *dal.Store, name string) bool {
	handle, err := s.NewDirectReadHandle()
	if err != nil {
		return false
	}

	defer func() { _ = handle.Close() }()

	cursor, err := query.ReadLedgers(context.Background(), handle)
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
	applier      *Applier
	store        *dal.Store
	wal          wal.WAL
	spool        *spool.Default
	fsm          *state.Machine
	stop         chan struct{}
	confState    *raftpb.ConfState
	responseSink LocalResponses
}

// newTestApplierSetup creates a minimal Applier with real infrastructure (Pebble, WAL, spool, FSM)
// and a buffered async-storage response sink that no test observer drains. Tests that assert on
// response delivery use newTestApplierSetupWithSink and pass their own channel; tests that don't
// care get a large-enough buffer so runCommitter never blocks on the send during Drain.
func newTestApplierSetup(t *testing.T) *testApplierSetup {
	t.Helper()

	return newTestApplierSetupWithSink(t, make(LocalResponses, 1024))
}

// newTestApplierSetupWithSink is newTestApplierSetup with a caller-provided
// LocalResponses channel wired into the applier. Tests that assert on
// MsgStorageApplyResp delivery use this variant.
func newTestApplierSetupWithSink(t *testing.T, sink LocalResponses) *testApplierSetup {
	t.Helper()

	logger := logging.Testing()
	meterProvider := noop.NewMeterProvider()
	meter := meterProvider.Meter("test")

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
	confState := &raftpb.ConfState{Voters: []uint64{1}}

	nodeCache, err := cache.New(1000, nil)
	require.NoError(t, err)

	nodeAttrs := attributes.New()

	nodeRegistry := state.NewStateRegistry(nodeCache, nodeAttrs, 0)
	nodeSnapshotter := state.NewCacheSnapshotter(logger, nodeRegistry, nil)
	fsm, err := state.NewMachine(
		logger, nodeRegistry, nodeSnapshotter, pebbleStore, dal.NewSentinelFactory(pebbleStore, false), meterProvider,
		nil, state.NewSharedState(), newNoopNotifier(t), nil, "test-cluster", 0, false,
		func(*raftpb.Entry, *dal.WriteSession) error { return nil },
	)
	require.NoError(t, err)

	recovery := state.NewRecovery(fsm, pebbleStore)
	require.NoError(t, recovery.RecoverState())
	synchronizer := state.NewSynchronizer(fsm, recovery, dal.NewIncomingRestoreFactory(pebbleStore))

	// Create initial snapshot (no FSM data) in the WAL.
	require.NoError(t, w.CreateSnapshot(0, confState, nil))

	applier, err := NewApplier(
		fsm, recovery, synchronizer, defaultSpool, pebbleStore, w, logger, meter,
		0, 1000, nil, func() {}, sink,
	)
	require.NoError(t, err)

	stop := make(chan struct{})

	t.Cleanup(func() {
		_ = pebbleStore.Close()
		_ = defaultSpool.Close()
		_ = w.Close()
	})

	return &testApplierSetup{
		applier:      applier,
		store:        pebbleStore,
		wal:          w,
		spool:        defaultSpool,
		fsm:          fsm,
		stop:         stop,
		confState:    confState,
		responseSink: sink,
	}
}

// makeCreateLedgerEntry creates a valid raftpb.Entry containing a CreateLedgerOrder.
// Returns the entry and the proposal ID for future registration.
func makeCreateLedgerEntry(t *testing.T, index uint64, name string) (*raftpb.Entry, uint64) {
	t.Helper()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: name,
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{},
				},
			},
		},
	}
	cmd := commands.NewCommand(order)

	// Declare the LedgerKey so the FSM-side Plan admits the read
	// processCreateLedger performs on WriteSet.GetLedger before writing.
	ledgerID, _ := attributes.MakeKey(domain.LedgerKey{Name: name}.Bytes())
	cmd.ExecutionPlan = &raftcmdpb.ExecutionPlan{
		Attributes: []*raftcmdpb.AttributeCoverage{{
			Id: &raftcmdpb.AttributeID{Id: ledgerID[:]}, AttrCode: uint32(dal.SubAttrLedger),
		}},
	}

	// Single-order coverage: every bit set.
	order.CoverageBits = []byte{0b1}

	data, err := cmd.MarshalVT()
	require.NoError(t, err)

	return &raftpb.Entry{
		Term:  proto.Uint64(1),
		Index: new(index),
		Type:  new(raftpb.EntryNormal),
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
		setup.applier.Submit([]*raftpb.Entry{entry}, nil, nil, setup.stop)
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

// TestApplierRunStopPrecedenceOverCtxCancel verifies the invariant the
// bootstrap OnStop hook depends on (#345): closing the `stop` channel must
// be the unambiguous shutdown signal even if the surrounding ctx is also
// cancelled. The applier's select loop watches only `stop`, so a closed
// stop should yield a clean nil regardless of ctx state. If this ever
// regresses (e.g. someone adds a `case <-ctx.Done(): return ctx.Err()`
// branch), the bootstrap hook would start panicking on shutdown again.
func TestApplierRunStopPrecedenceOverCtxCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(logging.TestingContext())
	setup := newTestApplierSetup(t)

	runDone := make(chan error, 1)

	go func() {
		runDone <- setup.applier.Run(ctx, setup.stop)
	}()

	// Cancel ctx FIRST, then close stop — the order seen by the buggy
	// pre-fix bootstrap hook (cancelRun() before node.Stop()).
	cancel()
	close(setup.stop)

	select {
	case err := <-runDone:
		require.NoError(t, err, "Run must return nil when stop is closed even if ctx was cancelled")
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after stop")
	}
}

func TestApplierRunSpoolsWhenNotNormal(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)

	// Set status to syncing before starting.
	setup.applier.setOutOfSync() // Set to non-normal status for spooling test

	// Start the Run goroutine.
	runDone := make(chan error, 1)

	go func() {
		runDone <- setup.applier.Run(ctx, setup.stop)
	}()

	// Submit 3 CreateLedger entries.
	for i := uint64(1); i <= 3; i++ {
		entry, _ := makeCreateLedgerEntry(t, i, fmt.Sprintf("ledger-%d", i))
		setup.applier.Submit([]*raftpb.Entry{entry}, nil, nil, setup.stop)
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
	setup.applier.ch <- applyWork{entries: []*raftpb.Entry{entry}}

	// Close stop.
	close(setup.stop)

	// Submit should return immediately via stop.
	submitDone := make(chan struct{})

	go func() {
		entry2, _ := makeCreateLedgerEntry(t, 2, "blocked")
		setup.applier.Submit([]*raftpb.Entry{entry2}, nil, nil, setup.stop)
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

// TestApplierTrySyncSnapshotIsNonBlocking verifies the EN-1431 follow-up
// invariant: TrySyncSnapshot must never block. When the applier's work
// channel is full (buffered size 1), a second call returns false rather
// than blocking. The processReady out-of-sync fallback fires on every
// Ready; blocking would stall the raft-ready goroutine, and enqueuing
// duplicate syncLeader items causes startMaintenanceTask to interrupt
// its own in-flight checkpoint fetch.
func TestApplierTrySyncSnapshotIsNonBlocking(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)

	// First call succeeds: channel has capacity 1.
	require.True(t, setup.applier.TrySyncSnapshot(1),
		"first TrySyncSnapshot must enqueue with an empty channel")

	// Second call must return false immediately (channel is full — the
	// Run goroutine isn't started in this test, so nothing drains it).
	done := make(chan bool, 1)
	go func() {
		done <- setup.applier.TrySyncSnapshot(1)
	}()

	select {
	case result := <-done:
		require.False(t, result, "second TrySyncSnapshot on a full channel must return false")
	case <-time.After(1 * time.Second):
		t.Fatal("TrySyncSnapshot blocked instead of returning false when channel was full")
	}
}

func TestApplierFutureResolution(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)

	// Create entry and register a future for the proposal ID. Term matches
	// the entry's term so the post-apply sweep leaves it untouched.
	entry, proposalID := makeCreateLedgerEntry(t, 1, "future-ledger")
	future := futures.New[state.ApplyResult]()
	setup.applier.StoreFuture(proposalID, entry.GetTerm(), future)

	// Start the Run goroutine.
	runDone := make(chan error, 1)

	go func() {
		runDone <- setup.applier.Run(ctx, setup.stop)
	}()

	// Submit the entry.
	setup.applier.Submit([]*raftpb.Entry{entry}, nil, nil, setup.stop)

	// Wait for the future to resolve.
	resultCh := make(chan state.ApplyResult, 1)
	errCh := make(chan error, 1)

	go func() {
		result, err := future.Wait(t.Context())
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

// makeCreateLedgerEntryWithTerm is a variant of makeCreateLedgerEntry that
// lets the caller pick the term, so tests can simulate term advances on the
// apply path.
func makeCreateLedgerEntryWithTerm(t *testing.T, term, index uint64, name string) (*raftpb.Entry, uint64) {
	t.Helper()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: name,
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{},
				},
			},
		},
	}
	cmd := commands.NewCommand(order)

	ledgerID, _ := attributes.MakeKey(domain.LedgerKey{Name: name}.Bytes())
	cmd.ExecutionPlan = &raftcmdpb.ExecutionPlan{
		Attributes: []*raftcmdpb.AttributeCoverage{{
			Id: &raftcmdpb.AttributeID{Id: ledgerID[:]}, AttrCode: uint32(dal.SubAttrLedger),
		}},
	}
	order.CoverageBits = []byte{0b1}

	data, err := cmd.MarshalVT()
	require.NoError(t, err)

	return &raftpb.Entry{
		Term:  new(term),
		Index: new(index),
		Type:  new(raftpb.EntryNormal),
		Data:  data,
	}, cmd.GetId()
}

// waitFutureBounded waits for a future to resolve within d, returning the
// error and whether it resolved. Used to assert "future stays pending" with
// a short timeout and "future resolves" without hanging the test.
func waitFutureBounded(f *futures.Future[state.ApplyResult], d time.Duration) (err error, resolved bool) {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()

	_, werr := f.Wait(ctx)
	if errors.Is(werr, context.DeadlineExceeded) {
		return nil, false
	}

	return werr, true
}

func TestApplierFailFuturesBelowTerm(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)

	type entry struct {
		id   uint64
		term uint64
	}

	entries := []entry{
		{id: 1, term: 2},
		{id: 2, term: 3},
		{id: 3, term: 3},
		{id: 4, term: 5},
	}

	fs := make(map[uint64]*futures.Future[state.ApplyResult])

	for _, e := range entries {
		fs[e.id] = futures.New[state.ApplyResult]()
		setup.applier.StoreFuture(e.id, e.term, fs[e.id])
	}

	sentinel := errors.New("sweep sentinel")
	setup.applier.FailFuturesBelowTerm(3, sentinel)

	// Only the term=2 future is below threshold and should resolve.
	gotErr, ok := waitFutureBounded(fs[1], time.Second)
	require.True(t, ok, "term=2 future must resolve below threshold")
	require.ErrorIs(t, gotErr, sentinel)

	// Higher-term futures stay pending.
	for _, id := range []uint64{2, 3, 4} {
		_, resolved := waitFutureBounded(fs[id], 100*time.Millisecond)
		require.False(t, resolved, "future id=%d at term >= threshold must remain pending", id)
	}
}

func TestApplierTermOlderThanBatchFails(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)

	// Orphan future at term=2 (no matching entry in the batch).
	orphanFuture := futures.New[state.ApplyResult]()
	const orphanProposalID uint64 = 99999

	setup.applier.StoreFuture(orphanProposalID, 2, orphanFuture)

	// Apply an unrelated entry at term=3.
	entry, _ := makeCreateLedgerEntryWithTerm(t, 3, 1, "term-advance-trigger")

	runDone := make(chan error, 1)
	go func() { runDone <- setup.applier.Run(ctx, setup.stop) }()

	setup.applier.Submit([]*raftpb.Entry{entry}, nil, nil, setup.stop)
	setup.applier.Drain(setup.stop)

	gotErr, resolved := waitFutureBounded(orphanFuture, 2*time.Second)
	require.True(t, resolved, "orphan future must resolve once higher-term entry applies")
	require.ErrorIs(t, gotErr, ErrLeadershipLost)

	close(setup.stop)
	<-runDone
}

func TestApplierMixedTermBatchV1RaceFixed(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)

	// committedEntry at term=2 — has a matching future.
	committedEntry, committedID := makeCreateLedgerEntryWithTerm(t, 2, 1, "committed-via-old-leader")
	advanceEntry, _ := makeCreateLedgerEntryWithTerm(t, 3, 2, "new-leader-noop")

	committedFuture := futures.New[state.ApplyResult]()
	orphanFuture := futures.New[state.ApplyResult]()
	const orphanID uint64 = 99999

	setup.applier.StoreFuture(committedID, 2, committedFuture)
	setup.applier.StoreFuture(orphanID, 2, orphanFuture)

	runDone := make(chan error, 1)
	go func() { runDone <- setup.applier.Run(ctx, setup.stop) }()

	setup.applier.Submit([]*raftpb.Entry{committedEntry, advanceEntry}, nil, nil, setup.stop)
	setup.applier.Drain(setup.stop)

	// committed future resolves with success (commandID match, leader-completeness).
	committedErr, committedResolved := waitFutureBounded(committedFuture, 2*time.Second)
	require.True(t, committedResolved, "committed future must resolve")
	require.NoError(t, committedErr, "committed future must resolve with success even though the batch also advances term")

	// orphan future fails with ErrLeadershipLost (truncated by new term).
	orphanErr, orphanResolved := waitFutureBounded(orphanFuture, 2*time.Second)
	require.True(t, orphanResolved, "orphan future must resolve once the higher-term entry applies")
	require.ErrorIs(t, orphanErr, ErrLeadershipLost)

	close(setup.stop)
	<-runDone
}

func TestApplierResolveDroppedFuture(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)

	dropped := futures.New[state.ApplyResult]()
	const droppedID uint64 = 42

	setup.applier.StoreFuture(droppedID, 1, dropped)

	dropErr := errors.New("simulated raft.ErrProposalDropped")
	setup.applier.ResolveDroppedFuture(droppedID, dropErr)

	gotErr, resolved := waitFutureBounded(dropped, time.Second)
	require.True(t, resolved, "dropped future must resolve immediately")
	require.ErrorIs(t, gotErr, dropErr)

	// Calling again is a no-op (no future in the map).
	require.NotPanics(t, func() { setup.applier.ResolveDroppedFuture(droppedID, dropErr) })
}

// TestApplierExtractDeferredFutureNoRegisteredFuture verifies the helper
// returns (lastResult, nil) when the last result's proposalID is not in
// the futures map — the defensive nil-future path the callers rely on
// (e.g. ConfChange entries that don't register an FSM future).
func TestApplierExtractDeferredFutureNoRegisteredFuture(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)

	result := &state.ApplyEntriesResult{
		Results: []state.ApplyResult{
			{ProposalID: 12345},
		},
		CheckpointRequired: true,
	}

	lastResult, deferred := setup.applier.extractDeferredFuture(result)
	require.NotNil(t, lastResult, "lastResult must be returned even when no future is registered")
	require.Equal(t, uint64(12345), lastResult.ProposalID)
	require.Nil(t, deferred, "deferred future must be nil when no proposalID match")
}

// TestApplierExtractDeferredFutureEmptyResults verifies the helper safely
// returns (nil, nil) when the apply result has no Results entries.
func TestApplierExtractDeferredFutureEmptyResults(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)

	lastResult, deferred := setup.applier.extractDeferredFuture(&state.ApplyEntriesResult{})
	require.Nil(t, lastResult)
	require.Nil(t, deferred)
}

// TestApplierExtractBatchFuturesSkipsDeferred verifies the "last entry is
// deferred" convention: when CheckpointRequired is set, the last result's
// future stays in the map for the deferred path to pick up later, while
// preceding futures are extracted via LoadAndDelete.
func TestApplierExtractBatchFuturesSkipsDeferred(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)

	const term uint64 = 1

	fs := make(map[uint64]*futures.Future[state.ApplyResult], 3)
	for _, id := range []uint64{1, 2, 3} {
		fs[id] = futures.New[state.ApplyResult]()
		setup.applier.StoreFuture(id, term, fs[id])
	}

	result := &state.ApplyEntriesResult{
		Results: []state.ApplyResult{
			{ProposalID: 1},
			{ProposalID: 2},
			{ProposalID: 3},
		},
		CheckpointRequired: true,
	}

	pfs := setup.applier.extractBatchFutures(result)
	require.Len(t, pfs, 2, "deferred (last) entry must be skipped")
	require.Equal(t, uint64(1), pfs[0].proposalID)
	require.Equal(t, uint64(2), pfs[1].proposalID)

	// Futures 1 and 2 were extracted (LoadAndDelete'd) — the map no longer
	// holds them. Future 3 (the deferred one) is still there.
	for _, id := range []uint64{1, 2} {
		_, ok := setup.applier.futures.LoadAndDelete(id)
		require.False(t, ok, "future %d should have been extracted", id)
	}
	cur, ok := setup.applier.futures.LoadAndDelete(uint64(3))
	require.True(t, ok, "deferred future 3 should remain in the map")
	require.Equal(t, fs[3], cur.future)
}

// TestApplierExtractBatchFuturesWithoutCheckpointRequired verifies that
// when CheckpointRequired is false, every result's future is extracted
// (no deferred-skip).
func TestApplierExtractBatchFuturesWithoutCheckpointRequired(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)

	const term uint64 = 1

	for _, id := range []uint64{1, 2} {
		setup.applier.StoreFuture(id, term, futures.New[state.ApplyResult]())
	}

	result := &state.ApplyEntriesResult{
		Results: []state.ApplyResult{
			{ProposalID: 1},
			{ProposalID: 2},
		},
		CheckpointRequired: false,
	}

	pfs := setup.applier.extractBatchFutures(result)
	require.Len(t, pfs, 2, "both futures must be extracted when no checkpoint is required")
}

// TestApplierConcurrentResolveAndSweep is the race-safety regression test
// for the LoadAndDelete + identity-check pattern in FailFuturesBelowTerm
// and ResolveDroppedFuture. With go test -race, any double-Resolve or
// concurrent map mutation triggers a failure.
//
// For each iteration we register a future and concurrently invoke the two
// paths that can race to resolve it. The future must end up resolved
// exactly once (we only check it eventually resolves; -race catches the
// double-Resolve / data race case).
func TestApplierConcurrentResolveAndSweep(t *testing.T) {
	t.Parallel()

	setup := newTestApplierSetup(t)

	const iterations = 200

	for i := range iterations {
		commandID := uint64(i + 1)
		f := futures.New[state.ApplyResult]()
		setup.applier.StoreFuture(commandID, 1, f)

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			setup.applier.ResolveDroppedFuture(commandID, errors.New("dropped"))
		}()

		go func() {
			defer wg.Done()
			setup.applier.FailFuturesBelowTerm(5, errors.New("sweep"))
		}()

		wg.Wait()

		// The future MUST be resolved exactly once. We can't easily detect
		// "exactly once" here, but we can detect "at least once" (the future
		// resolves within timeout) and rely on -race to detect double
		// Resolve via internal mutex contention if it happened.
		_, resolved := waitFutureBounded(f, 100*time.Millisecond)
		require.True(t, resolved, "future for iter=%d must be resolved by one of the paths", i)
	}
}

// makeCreateQueryCheckpointEntry creates a valid raftpb.Entry containing a CreateQueryCheckpointOrder.
func makeCreateQueryCheckpointEntry(t *testing.T, index uint64) (*raftpb.Entry, uint64) {
	t.Helper()

	cmd := commands.NewCommand(&raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
					CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
				},
			},
		},
	})

	data, err := cmd.MarshalVT()
	require.NoError(t, err)

	return &raftpb.Entry{
		Term:  proto.Uint64(1),
		Index: new(index),
		Type:  new(raftpb.EntryNormal),
		Data:  data,
	}, cmd.GetId()
}

// makeStaleCreateQueryCheckpointEntry builds a CreateQueryCheckpoint entry
// whose PredictedIndex deliberately differs from the raft index, so
// checkStaleProposal rejects it before any order runs. Used to exercise the
// "structural trigger that does not actually fire" branch of applyDecodedEntriesToFSM.
func makeStaleCreateQueryCheckpointEntry(t *testing.T, index, predictedIndex uint64) (*raftpb.Entry, uint64) {
	t.Helper()

	cmd := commands.NewCommand(&raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
					CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
				},
			},
		},
	})
	cmd.PredictedIndex = predictedIndex

	data, err := cmd.MarshalVT()
	require.NoError(t, err)

	return &raftpb.Entry{
		Term:  proto.Uint64(1),
		Index: new(index),
		Type:  new(raftpb.EntryNormal),
		Data:  data,
	}, cmd.GetId()
}

// TestApplierCascadingQueryCheckpointsDuringReplay verifies that entries after
// a second CreateQueryCheckpoint are not lost during spool replay.
//
// Regression test: the previous replay path did not loop on cascading
// checkpoints, so entries after the second checkpoint were silently dropped.
// The current path (applyReplayEntries) pre-splits at each trigger and the
// caller loops on the remaining tail until the entire slice has been applied.
//
// The test bypasses the Run loop to avoid timing issues: it applies the first
// entry directly, fills the spool with entries containing two query checkpoints,
// then calls replaySpool to exercise the cascading path.
func TestApplierCascadingQueryCheckpointsDuringReplay(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)

	// Entry 1: CreateLedger — applied directly so the FSM has lastAppliedIndex=1.
	e1, _ := makeCreateLedgerEntry(t, 1, "qcp-ledger")
	_, err := setup.fsm.ApplyEntries(ctx, setup.store, e1)
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
	_, err = setup.applier.replaySpool(ctx, 1)
	require.NoError(t, err)

	// All ledgers must exist — including "after-second-cp".
	require.True(t, listLedgerContains(setup.store, "qcp-ledger"), "qcp-ledger should exist")
	require.True(t, listLedgerContains(setup.store, "before-first-cp"), "before-first-cp should exist")
	require.True(t, listLedgerContains(setup.store, "between-cps"), "between-cps should exist")
	require.True(t, listLedgerContains(setup.store, "after-second-cp"),
		"after-second-cp should exist — it was lost before the cascading checkpoint fix")
}

// TestApplierRejectedTriggerDoesNotDropTail is a regression test for an
// antithesis-discovered bug where a leadership transition caused all nodes to
// panic with "task pool error: preparing entries: invalid index, got 231,
// expected 230 [recovered, repanicked]".
//
// Root cause: applyDecodedEntriesToFSM pre-splits the committed entries slice at any
// CreateQueryCheckpoint / CloseChapter trigger so each FSM batch contains at
// most one trigger as its last entry. The pre-split is purely structural — it
// does not know whether the trigger will actually fire at apply time. When
// checkStaleProposal rejected a CreateQueryCheckpoint after a leadership
// transfer, the proposal's orders never ran, so ApplyEntriesResult.
// CheckpointRequired stayed false and the function returned early — silently
// dropping the tail entries. The next Raft Ready brought entry index N+2 to a
// FSM still at lastAppliedIndex=N, tripping the gap detector.
//
// The fix loops on the remaining tail when CheckpointRequired is false, so a
// rejected trigger no longer hides the entries that followed it.
func TestApplierRejectedTriggerDoesNotDropTail(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)

	runDone := make(chan error, 1)
	go func() { runDone <- setup.applier.Run(ctx, setup.stop) }()

	// Entry 1: a CreateQueryCheckpoint whose PredictedIndex (99) does not
	// match the raft index (1), so checkStaleProposal rejects it. The
	// structural pre-split still detects the trigger, so the slice is split
	// into head=[1] and tail=[2]. CheckpointRequired remains false because
	// the order never runs.
	stale, _ := makeStaleCreateQueryCheckpointEntry(t, 1, 99)

	// Entry 2: a regular CreateLedger that must still be applied — it was
	// the entry being dropped by the bug.
	tail, _ := makeCreateLedgerEntry(t, 2, "tail-ledger")

	setup.applier.Submit([]*raftpb.Entry{stale, tail}, setup.confState, nil, setup.stop)
	setup.applier.Drain(setup.stop)

	require.True(t, listLedgerContains(setup.store, "tail-ledger"),
		"tail-ledger must exist — entries past a rejected checkpoint trigger were dropped before the fix")

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
		setup.applier.Submit([]*raftpb.Entry{entry}, setup.confState, nil, setup.stop)
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

func TestStartMaintenanceTaskReportsFailureOnTaskError(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t)
	taskErr := errors.New("checkpoint failed")

	gatingTerminated := setup.applier.startMaintenanceTask(ctx, func(ctx context.Context) (maintenanceTaskResult, error) {
		return maintenanceTaskResult{}, taskErr
	}, nil)

	var result gatingResult
	require.Eventually(t, func() bool {
		select {
		case result = <-gatingTerminated:
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	require.True(t, result.taskFailed)
	require.False(t, result.syncFailed)

	var err error
	require.Eventually(t, func() bool {
		select {
		case err = <-setup.applier.TaskError():
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	require.ErrorIs(t, err, taskErr)
}
