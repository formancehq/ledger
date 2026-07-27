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
