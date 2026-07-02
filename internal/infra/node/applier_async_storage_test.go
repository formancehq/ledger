package node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	raftpb "go.etcd.io/raft/v3/raftpb"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// makeApplyResp builds a MsgStorageApplyResp targeted at nodeID for use as
// the response payload in async-storage tests. Content is opaque to the
// Applier — it just forwards the slice to the sink.
func makeApplyResp(nodeID uint64, index uint64) raftpb.Message {
	return raftpb.Message{
		Type:  raftpb.MsgStorageApplyResp,
		To:    nodeID,
		From:  raft.LocalApplyThread,
		Index: index,
	}
}

// TestApplierFiresResponsesAfterCommit — statusNormal happy path. Verifies
// that MsgStorageApplyResp arrives on the sink AFTER CommitPreparedBatch has
// landed (proxy: the ledger written by the applied entry is queryable in the
// store at the moment the response is received). Directly addresses
// NumaryBot finding af4915f6/41749f9676.
func TestApplierFiresResponsesAfterCommit(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	sink := make(LocalResponses, 4)
	setup := newTestApplierSetupWithSink(t, sink)

	runDone := make(chan error, 1)
	go func() { runDone <- setup.applier.Run(ctx, setup.stop) }()

	entry, _ := makeCreateLedgerEntry(t, 1, "async-normal")
	resp := makeApplyResp(1, entry.Index)
	setup.applier.Submit([]raftpb.Entry{entry}, setup.confState, []raftpb.Message{resp}, setup.stop)

	select {
	case got := <-sink:
		require.Len(t, got, 1)
		require.Equal(t, resp.Index, got[0].Index)

		// Response arrived — the commit MUST have completed. Verify FSM
		// state reflects the entry (this is the timing assertion behind
		// the "Applied tracks FSM-applied" claim).
		require.True(t, listLedgerContains(setup.store, "async-normal"),
			"ledger must be committed to store before response fires")
	case <-time.After(3 * time.Second):
		t.Fatal("expected response on sink after successful commit")
	}

	close(setup.stop)
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after stop")
	}
}

// TestApplierFiresResponsesFromSpoolBranch — non-statusNormal path. When the
// applier is out-of-sync (or otherwise gated), entries are spooled and
// responses fire eagerly (documented in the applier: spool durability is
// enough to acknowledge Applied).
func TestApplierFiresResponsesFromSpoolBranch(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	sink := make(LocalResponses, 4)
	setup := newTestApplierSetupWithSink(t, sink)

	setup.applier.setOutOfSync()

	runDone := make(chan error, 1)
	go func() { runDone <- setup.applier.Run(ctx, setup.stop) }()

	entry, _ := makeCreateLedgerEntry(t, 1, "async-spool")
	resp := makeApplyResp(1, entry.Index)
	setup.applier.Submit([]raftpb.Entry{entry}, setup.confState, []raftpb.Message{resp}, setup.stop)

	select {
	case got := <-sink:
		require.Len(t, got, 1)
		require.Equal(t, resp.Index, got[0].Index)

		// Under out-of-sync the entry is spooled, NOT applied — the sink
		// must fire on spool durability, not on FSM state.
		require.False(t, listLedgerContains(setup.store, "async-spool"),
			"spool branch must not commit to store")
	case <-time.After(3 * time.Second):
		t.Fatal("expected response on sink from spool branch")
	}

	close(setup.stop)
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after stop")
	}
}

// TestApplierNoFireWhenSinkNil — regression guard: a nil responseSink must
// not panic even when work.responses is populated. Sync-mode callers (and
// tests that don't wire a sink) rely on this.
func TestApplierNoFireWhenSinkNil(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	setup := newTestApplierSetup(t) // sink is nil

	runDone := make(chan error, 1)
	go func() { runDone <- setup.applier.Run(ctx, setup.stop) }()

	entry, _ := makeCreateLedgerEntry(t, 1, "async-nil-sink")
	resp := makeApplyResp(1, entry.Index)
	setup.applier.Submit([]raftpb.Entry{entry}, setup.confState, []raftpb.Message{resp}, setup.stop)

	setup.applier.Drain(setup.stop)

	require.True(t, listLedgerContains(setup.store, "async-nil-sink"),
		"entry must still be applied even when sink is nil")

	close(setup.stop)
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after stop")
	}
}

// TestApplierNoFireWhenResponsesEmpty — regression guard: a Submit without
// responses (empty slice) must not fire an empty batch on the sink.
func TestApplierNoFireWhenResponsesEmpty(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	sink := make(LocalResponses, 4)
	setup := newTestApplierSetupWithSink(t, sink)

	runDone := make(chan error, 1)
	go func() { runDone <- setup.applier.Run(ctx, setup.stop) }()

	entry, _ := makeCreateLedgerEntry(t, 1, "async-no-resp")
	setup.applier.Submit([]raftpb.Entry{entry}, setup.confState, nil, setup.stop)

	setup.applier.Drain(setup.stop)

	select {
	case got := <-sink:
		t.Fatalf("sink must be empty when Submit carries no responses, got %d msg(s)", len(got))
	case <-time.After(200 * time.Millisecond):
		// expected: nothing fires
	}

	close(setup.stop)
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after stop")
	}
}

// TestApplierMultipleBatchesOnlyLastResponsesFire — with multiple sub-batches
// resulting from applyEntriesToFSM's checkpoint-boundary splitting (or from
// multiple Submits), each Submit's responses must fire independently: no
// aggregation, no loss, exactly once per Submit that carries responses.
func TestApplierMultipleBatchesOnlyLastResponsesFire(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	sink := make(LocalResponses, 8)
	setup := newTestApplierSetupWithSink(t, sink)

	runDone := make(chan error, 1)
	go func() { runDone <- setup.applier.Run(ctx, setup.stop) }()

	entry1, _ := makeCreateLedgerEntry(t, 1, "async-multi-1")
	entry2, _ := makeCreateLedgerEntry(t, 2, "async-multi-2")
	entry3, _ := makeCreateLedgerEntry(t, 3, "async-multi-3")

	resp1 := makeApplyResp(1, entry1.Index)
	resp2 := makeApplyResp(1, entry2.Index)
	resp3 := makeApplyResp(1, entry3.Index)

	setup.applier.Submit([]raftpb.Entry{entry1}, setup.confState, []raftpb.Message{resp1}, setup.stop)
	setup.applier.Submit([]raftpb.Entry{entry2}, setup.confState, []raftpb.Message{resp2}, setup.stop)
	setup.applier.Submit([]raftpb.Entry{entry3}, setup.confState, []raftpb.Message{resp3}, setup.stop)

	setup.applier.Drain(setup.stop)

	// Collect responses. Order is guaranteed by the single-writer applier→
	// committer chain: batches commit in the order they're submitted.
	receivedIdx := collectResponseIndices(t, sink, 3)
	require.Equal(t, []uint64{entry1.Index, entry2.Index, entry3.Index}, receivedIdx)

	close(setup.stop)
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after stop")
	}
}

// TestApplierRunCommitterDoesNotDeadlockOnFullSink — regression guard for
// findings 34540caa/9047f08a. If the response sink stops being drained
// (consumer down, orchestrate halted) and ctx is cancelled, runCommitter
// must NOT return from mid-batch: it must fall through the ctx.Done arm and
// still signal work.done, otherwise Applier.Run's deferred waitPendingCommit
// blocks forever on <-a.pending.done.
func TestApplierRunCommitterDoesNotDeadlockOnFullSink(t *testing.T) {
	t.Parallel()

	// Unbuffered sink + zero consumer: any send blocks immediately.
	sink := make(LocalResponses)
	setup := newTestApplierSetupWithSink(t, sink)

	ctx, cancel := context.WithCancel(logging.TestingContext())
	defer cancel()

	runDone := make(chan error, 1)
	go func() { runDone <- setup.applier.Run(ctx, setup.stop) }()

	entry, _ := makeCreateLedgerEntry(t, 1, "async-full-sink")
	resp := makeApplyResp(1, entry.Index)
	setup.applier.Submit([]raftpb.Entry{entry}, setup.confState, []raftpb.Message{resp}, setup.stop)

	// Give runCommitter time to fire the response — it will block on the
	// unbuffered send. If the deadlock regressed, the test hangs here.
	time.Sleep(200 * time.Millisecond)

	// Cancel ctx → runCommitter's select takes the ctx.Done arm. The fix
	// requires it to fall through (not return), so work.done fires and
	// Applier.Run's deferred cleanup can drain pending without hanging.
	cancel()

	// Signal shutdown to Applier.Run.
	close(setup.stop)

	select {
	case err := <-runDone:
		// Either nil (clean stop) or a ctx-related error — either is fine.
		// The test PASSES as long as Run returned; the previous code
		// (early `return` from runCommitter without signalling work.done)
		// would hang forever here.
		_ = err
	case <-time.After(5 * time.Second):
		t.Fatal("Applier.Run did not return after ctx cancel + stop — deadlock regression on findings 34540caa/9047f08a")
	}
}

// collectResponseIndices drains want messages from sink and returns their
// Index fields in receipt order. Fails the test if fewer than want arrive
// within the deadline.
func collectResponseIndices(t *testing.T, sink LocalResponses, want int) []uint64 {
	t.Helper()

	deadline := time.After(3 * time.Second)
	out := make([]uint64, 0, want)

	for len(out) < want {
		select {
		case batch := <-sink:
			for _, m := range batch {
				out = append(out, m.Index)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %d responses, got %d", want, len(out))
		}
	}

	return out
}
