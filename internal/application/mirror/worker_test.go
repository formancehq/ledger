package mirror

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	v2 "github.com/formancehq/ledger/v3/internal/adapter/v2"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// A mirror ledger with no boundary row starts fetching from source ID 1,
// i.e. it asks the source for logs after ID 0.
func TestWorker_FreshLedgerFetchesFromSourceIDOne(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	ctrl := gomock.NewController(t)
	source := v2.NewMockSource(ctrl)

	// No boundary row written: the ledger has never ingested.
	source.EXPECT().
		FetchLogs(gomock.Any(), uint64(0), gomock.Any()).
		Return(nil, false, nil).
		Times(1)

	w := newWorkerForTest(t, "fresh", source, store, builder)

	_, err := w.processBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(0), w.lastAppliedV2LogID)
	require.True(t, w.boundariesLoaded)
}

// A ledger whose FSM-applied boundary is 50 always resumes after 50.
func TestWorker_ResumesFromAppliedBoundary(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	writeBoundaries(t, store, "mirrored", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 7,
		LastMirrorV2LogId: 50,
	})

	ctrl := gomock.NewController(t)
	source := v2.NewMockSource(ctrl)
	source.EXPECT().
		FetchLogs(gomock.Any(), uint64(50), gomock.Any()).
		Return(nil, false, nil).
		Times(1)

	w := newWorkerForTest(t, "mirrored", source, store, builder)

	_, err := w.processBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(50), w.lastAppliedV2LogID)
	require.Equal(t, uint64(7), w.nextTxID, "one boundary read must serve both fields")
}

// A worker rebuilt after the FSM committed a later boundary resumes from the
// newly persisted value, not from anything it held before.
func TestWorker_RestartAfterCommitResumesFromNewBoundary(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	writeBoundaries(t, store, "mirrored", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1,
		LastMirrorV2LogId: 10,
	})

	ctrl := gomock.NewController(t)
	first := v2.NewMockSource(ctrl)
	first.EXPECT().FetchLogs(gomock.Any(), uint64(10), gomock.Any()).Return(nil, false, nil).Times(1)

	w1 := newWorkerForTest(t, "mirrored", first, store, builder)
	_, err := w1.processBatch(context.Background())
	require.NoError(t, err)

	// FSM advances the boundary; the old worker is discarded without ever
	// learning about it (leadership change / crash after commit).
	writeBoundaries(t, store, "mirrored", &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1,
		LastMirrorV2LogId: 42,
	})

	second := v2.NewMockSource(ctrl)
	second.EXPECT().FetchLogs(gomock.Any(), uint64(42), gomock.Any()).Return(nil, false, nil).Times(1)

	w2 := newWorkerForTest(t, "mirrored", second, store, builder)
	_, err = w2.processBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(42), w2.lastAppliedV2LogID)
}

// A pending prefetch is only usable when it was issued from the position the
// worker is actually resuming from. prefetchResult.afterID records that
// position; these cases pin the three outcomes of the validity check
// (EN-1513).
func TestWorker_PrefetchValidity(t *testing.T) {
	t.Parallel()

	// The boundary the worker resumes from in every case below.
	const boundary uint64 = 10

	tests := []struct {
		name string
		// prefetch queued before processBatch runs.
		prefetch prefetchResult
		// whether the prefetch must be discarded and the source re-queried.
		wantSyncFetch bool
	}{
		{
			name:          "issued from the current position is consumed",
			prefetch:      prefetchResult{logs: []v2.V2Log{}, afterID: boundary},
			wantSyncFetch: false,
		},
		{
			name:          "issued from a stale position is discarded",
			prefetch:      prefetchResult{logs: []v2.V2Log{}, afterID: boundary - 1},
			wantSyncFetch: true,
		},
		{
			name:          "failed fetch is discarded even at the right position",
			prefetch:      prefetchResult{err: context.DeadlineExceeded, afterID: boundary},
			wantSyncFetch: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			builder, store := newTestBuilder(t)
			writeBoundaries(t, store, "mirrored", &raftcmdpb.LedgerBoundaries{
				LastMirrorV2LogId: boundary,
			})

			ctrl := gomock.NewController(t)
			source := v2.NewMockSource(ctrl)

			wantCalls := 0
			if tt.wantSyncFetch {
				wantCalls = 1
			}

			// A discarded prefetch must fall back to the source, and must do
			// so from the boundary — never from the stale prefetch position.
			source.EXPECT().
				FetchLogs(gomock.Any(), boundary, gomock.Any()).
				Return(nil, false, nil).
				Times(wantCalls)

			w := newWorkerForTest(t, "mirrored", source, store, builder)

			w.prefetchCh = make(chan prefetchResult, 1)
			w.prefetchCh <- tt.prefetch

			_, err := w.processBatch(context.Background())
			require.NoError(t, err)

			require.Nil(t, w.prefetchCh, "the pending prefetch must be consumed either way")
			require.Equal(t, boundary, w.lastAppliedV2LogID,
				"an empty batch must not move the position")
		})
	}
}

// Invalidating the snapshot makes the next batch re-read the durable
// authority rather than reusing a stale in-memory position (EN-1513).
func TestWorker_InvalidatedSnapshotRereadsBoundary(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	writeBoundaries(t, store, "mirrored", &raftcmdpb.LedgerBoundaries{LastMirrorV2LogId: 10})

	ctrl := gomock.NewController(t)
	source := v2.NewMockSource(ctrl)
	gomock.InOrder(
		source.EXPECT().FetchLogs(gomock.Any(), uint64(10), gomock.Any()).Return(nil, false, nil),
		source.EXPECT().FetchLogs(gomock.Any(), uint64(99), gomock.Any()).Return(nil, false, nil),
	)

	w := newWorkerForTest(t, "mirrored", source, store, builder)
	_, err := w.processBatch(context.Background())
	require.NoError(t, err)

	// Simulate the error path: the snapshot is dropped, and meanwhile the
	// durable boundary moved.
	w.boundariesLoaded = false
	writeBoundaries(t, store, "mirrored", &raftcmdpb.LedgerBoundaries{LastMirrorV2LogId: 99})

	_, err = w.processBatch(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(99), w.lastAppliedV2LogID)
}

// An idle, caught-up worker publishes the observed source head on its own, and
// does not re-publish it on the next tick. Without the standalone publication
// SubPLMirrorSourceHead stays at whatever the store held — 0 on a restored
// node — and ReadMirrorSyncProgress reports SYNCING forever (EN-1773).
func TestWorker_IdleStatusPublishesSourceHeadOnce(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	writeBoundaries(t, store, "mirrored", &raftcmdpb.LedgerBoundaries{LastMirrorV2LogId: 4})

	ctrl := gomock.NewController(t)
	source := v2.NewMockSource(ctrl)
	source.EXPECT().GetLatestLogID(gomock.Any()).Return(uint64(4), nil).Times(1)
	source.EXPECT().FetchLogs(gomock.Any(), uint64(4), gomock.Any()).Return(nil, false, nil).Times(2)

	proposer := &stubProposer{outcome: proposeApplied}
	w := newWorkerWithProposer(t, "mirrored", source, store, builder, proposer)

	w.refreshSourceHead(context.Background())

	_, err := w.processBatch(context.Background())
	require.NoError(t, err)

	recorded := proposer.recorded()
	require.Len(t, recorded, 1, "a caught-up worker must publish its status once")
	require.Equal(t, uint64(4), recorded[0].update.GetSourceLogCount())
	require.True(t, recorded[0].update.GetClearError())
	require.Equal(t, "mirrored", recorded[0].update.GetLedgerName())
	require.Equal(t, uint64(4), w.lastPublishedSourceHead)
	require.True(t, w.statusClearConfirmed)

	// Second tick, nothing moved: the guard must suppress the proposal rather
	// than re-propose the same value every poll interval.
	_, err = w.processBatch(context.Background())
	require.NoError(t, err)
	require.Len(t, proposer.recorded(), 1, "an unchanged idle status must not be re-proposed")
}

// The regression behind the blocking review finding: once an error is
// persisted, a source that recovers WITHOUT producing a new log must still get
// the error cleared. Gating the publication on the source head alone left the
// API serving a stale error until the head happened to advance.
func TestWorker_IdleStatusClearsErrorWhenHeadUnchanged(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	writeBoundaries(t, store, "mirrored", &raftcmdpb.LedgerBoundaries{LastMirrorV2LogId: 4})

	ctrl := gomock.NewController(t)
	source := v2.NewMockSource(ctrl)
	source.EXPECT().GetLatestLogID(gomock.Any()).Return(uint64(4), nil).AnyTimes()
	source.EXPECT().FetchLogs(gomock.Any(), uint64(4), gomock.Any()).Return(nil, false, nil).AnyTimes()

	proposer := &stubProposer{outcome: proposeApplied}
	w := newWorkerWithProposer(t, "mirrored", source, store, builder, proposer)

	w.refreshSourceHead(context.Background())

	// Reach the caught-up steady state: head published, no error outstanding.
	_, err := w.processBatch(context.Background())
	require.NoError(t, err)
	require.Len(t, proposer.recorded(), 1)

	// A fetch fails and the error is persisted. The head does not move.
	w.reportError(context.Background(), "source unreachable")
	require.False(t, w.statusClearConfirmed,
		"a persisted error must mark the status as needing a clear")

	recorded := proposer.recorded()
	require.Len(t, recorded, 2)
	require.Equal(t, "source unreachable", recorded[1].update.GetError().GetMessage())

	// The source recovers with no new log. The head is unchanged, so only the
	// error-clear bit can justify this proposal.
	_, err = w.processBatch(context.Background())
	require.NoError(t, err)

	recorded = proposer.recorded()
	require.Len(t, recorded, 3, "a recovered source must clear the persisted error")
	require.True(t, recorded[2].update.GetClearError())
	require.Equal(t, uint64(4), recorded[2].update.GetSourceLogCount())
	require.True(t, w.statusClearConfirmed)

	// And once the clear is confirmed, it stops repeating.
	_, err = w.processBatch(context.Background())
	require.NoError(t, err)
	require.Len(t, proposer.recorded(), 3, "a confirmed clear must not repeat")
}

// An empty source is the strongest form of the same defect: a successfully
// observed head of zero is indistinguishable from "never observed" unless the
// observation itself is recorded, so the error could never clear — not even
// after a worker restart, because a fresh worker also starts at zero.
func TestWorker_IdleStatusClearsErrorForEmptySource(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)

	ctrl := gomock.NewController(t)
	source := v2.NewMockSource(ctrl)
	source.EXPECT().GetLatestLogID(gomock.Any()).Return(uint64(0), nil).AnyTimes()
	source.EXPECT().FetchLogs(gomock.Any(), uint64(0), gomock.Any()).Return(nil, false, nil).AnyTimes()

	proposer := &stubProposer{outcome: proposeApplied}
	w := newWorkerForTest(t, "empty", source, store, builder)
	w.proposer = proposer

	// Never observed yet: nothing to say about the source, so nothing is
	// proposed even though statusClearConfirmed is false.
	_, err := w.processBatch(context.Background())
	require.NoError(t, err)
	require.Empty(t, proposer.recorded(),
		"an unobserved source head must not be published")

	// Observed, and it is genuinely empty.
	w.refreshSourceHead(context.Background())
	require.True(t, w.sourceHeadObserved)
	require.Zero(t, w.sourceLogCount)

	_, err = w.processBatch(context.Background())
	require.NoError(t, err)

	recorded := proposer.recorded()
	require.Len(t, recorded, 1, "an observed empty source must still publish a clear")
	require.True(t, recorded[0].update.GetClearError())
	require.Zero(t, recorded[0].update.GetSourceLogCount(),
		"a zero head is 'no write' for that field, so no head is invented")
	require.True(t, w.statusClearConfirmed)

	_, err = w.processBatch(context.Background())
	require.NoError(t, err)
	require.Len(t, proposer.recorded(), 1, "a confirmed clear must not repeat")
}

// Only a confirmed application may advance the published state. Each failure
// stage must leave both bits untouched so the next poll tick retries — the
// alternative is a worker that believes it published something the store never
// received.
func TestWorker_IdleStatusRetriesAfterFailedPropose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		outcome proposeOutcome
	}{
		{name: "propose call fails", outcome: proposeRunError},
		{name: "raft rejects the proposal", outcome: proposeRaftReject},
		{name: "fsm apply fails", outcome: proposeFSMError},
		{name: "apply returns a business error", outcome: proposeBusinessError},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			builder, store := newTestBuilder(t)
			writeBoundaries(t, store, "mirrored", &raftcmdpb.LedgerBoundaries{LastMirrorV2LogId: 4})

			ctrl := gomock.NewController(t)
			source := v2.NewMockSource(ctrl)
			source.EXPECT().GetLatestLogID(gomock.Any()).Return(uint64(4), nil).AnyTimes()
			source.EXPECT().FetchLogs(gomock.Any(), uint64(4), gomock.Any()).Return(nil, false, nil).AnyTimes()

			proposer := &stubProposer{outcome: tt.outcome}
			w := newWorkerWithProposer(t, "mirrored", source, store, builder, proposer)

			w.refreshSourceHead(context.Background())

			// A failed publication is not a batch error: it is logged and the
			// worker stays on its poll interval.
			_, err := w.processBatch(context.Background())
			require.NoError(t, err)

			require.Len(t, proposer.recorded(), 1)
			require.Zero(t, w.lastPublishedSourceHead,
				"an unconfirmed publication must not advance the published head")
			require.False(t, w.statusClearConfirmed,
				"an unconfirmed publication must not mark the status clean")

			// Retry eligibility: the very next tick proposes again.
			_, err = w.processBatch(context.Background())
			require.NoError(t, err)
			require.Len(t, proposer.recorded(), 2, "the next tick must retry")
		})
	}
}

// A committed error report whose confirmation wait is abandoned must still
// mark the status dirty. proposeMirrorSync reports confirmation, not
// application: cancelling a wait does not un-commit a Raft entry, so treating
// its false as "nothing was persisted" would leave the worker believing the
// status is clean while an error sits in the store — and publishIdleStatus
// would then suppress the clear until the source head moved.
func TestWorker_ReportErrorMarksStatusDirtyWithoutConfirmation(t *testing.T) {
	t.Parallel()

	builder, store := newTestBuilder(t)
	writeBoundaries(t, store, "mirrored", &raftcmdpb.LedgerBoundaries{LastMirrorV2LogId: 4})

	ctrl := gomock.NewController(t)
	source := v2.NewMockSource(ctrl)
	source.EXPECT().GetLatestLogID(gomock.Any()).Return(uint64(4), nil).AnyTimes()
	source.EXPECT().FetchLogs(gomock.Any(), uint64(4), gomock.Any()).Return(nil, false, nil).AnyTimes()

	proposer := &stubProposer{outcome: proposeApplied}
	w := newWorkerWithProposer(t, "mirrored", source, store, builder, proposer)

	w.refreshSourceHead(context.Background())

	// Caught-up steady state: the head is published and no error is outstanding.
	_, err := w.processBatch(context.Background())
	require.NoError(t, err)
	require.Len(t, proposer.recorded(), 1)
	require.True(t, w.statusClearConfirmed)

	// The error report commits, but the worker's wait is interrupted before it
	// resolves, so proposeMirrorSync returns false for an applied proposal.
	proposer.setOutcome(proposeWaitAbandoned)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()

	w.reportError(cancelled, "source unreachable")

	recorded := proposer.recorded()
	require.Len(t, recorded, 2, "the error report must still reach Raft")
	require.Equal(t, "source unreachable", recorded[1].update.GetError().GetMessage())
	require.False(t, w.statusClearConfirmed,
		"an unconfirmed error report may still have been applied, so the status must be marked dirty")

	// The consequence that matters: the source recovers with no new log, and
	// the clear is issued even though the head never moved.
	proposer.setOutcome(proposeApplied)

	_, err = w.processBatch(context.Background())
	require.NoError(t, err)

	recorded = proposer.recorded()
	require.Len(t, recorded, 3, "a recovered source must clear the possibly-persisted error")
	require.True(t, recorded[2].update.GetClearError())
}
