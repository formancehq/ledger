package node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

func TestPromoteLearnerRetryPreservesCommitDuringWALPersistence(t *testing.T) {
	t.Parallel()
	testPromoteLearnerRetryDuringWALPersistence(t, false)
}

func TestPromoteLearnerRetryCancellationDuringWALPersistence(t *testing.T) {
	t.Parallel()
	testPromoteLearnerRetryDuringWALPersistence(t, true)
}

func testPromoteLearnerRetryDuringWALPersistence(t *testing.T, cancelDuringWAL bool) {
	t.Helper()

	n := newConfiguredPeersTestNode(t)
	n.logger = logging.Testing()
	setup := newTestApplierSetupWithConfChangeHandler(t, make(LocalResponses, 1024), n.membership.WriteConfChange)
	n.fsm = setup.fsm
	waitCtx, cancelWait := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancelWait()
	ctx, cancel := context.WithCancel(waitCtx)
	defer cancel()
	results := make(chan error, 1)
	go func() { results <- n.PromoteLearner(ctx, 2) }()

	var first *clusterCommand
	select {
	case first = <-n.clusterCommandCh:
	case <-waitCtx.Done():
		t.Fatal("promotion did not reach orchestrate")
	}
	require.NoError(t, first.fn())
	entry, cc := requireCompletePromotion(t, n)
	first.errCh <- nil

	// processReady appends entries before finishReady changes live membership.
	realWAL := n.wal
	prefix, err := realWAL.Entries(1, entry.GetIndex(), ^uint64(0))
	require.NoError(t, err)
	require.NoError(t, realWAL.Append(nil, []*raftpb.Entry{entry}))
	entered := make(chan struct{})
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	blockedWAL := wal.NewMockWAL(gomock.NewController(t))
	blockedWAL.EXPECT().Snapshot().DoAndReturn(realWAL.Snapshot)
	blockedWAL.EXPECT().UpdateSnapshotConfState(gomock.Any()).DoAndReturn(func(cs *raftpb.ConfState) error {
		close(entered)
		<-release

		return realWAL.UpdateSnapshotConfState(cs)
	})
	n.wal = blockedWAL
	finished := make(chan error, 1)
	go func() {
		finished <- n.finishReady(readyResult{confChanges: []committedConfChange{{index: entry.GetIndex(), change: cc}}}, make(chan struct{}))
	}()
	select {
	case <-entered:
	case <-waitCtx.Done():
		t.Fatal("commit did not reach WAL persistence")
	}

	// Receiving the retry proves the real retry timer elapsed while the
	// committed proposal's response was blocked. Do not execute it until
	// finishReady releases the orchestrate goroutine, as production requires.
	var retry *clusterCommand
	select {
	case retry = <-n.clusterCommandCh:
	case <-waitCtx.Done():
		t.Fatal("promotion did not retry during blocked WAL persistence")
	}
	if cancelDuringWAL {
		cancel()
		select {
		case err := <-results:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("canceled promotion waited for blocked WAL persistence")
		}
	}
	close(release)
	select {
	case err := <-finished:
		require.NoError(t, err)
	case <-waitCtx.Done():
		t.Fatal("commit did not finish after releasing WAL persistence")
	}
	require.False(t, n.rawNode.Status().Progress[2].IsLearner)
	retryErr := retry.fn()
	retry.errCh <- retryErr
	require.NoError(t, retryErr, "a queued retry must recognize its own committed promotion instead of rejecting the new voter role")

	if !cancelDuringWAL {
		select {
		case err := <-results:
			t.Fatalf("promotion returned before durable local apply: %v", err)
		default:
		}
	}
	// Cancellation changes the caller's outcome, never the committed effect.
	_, err = setup.fsm.ApplyEntries(t.Context(), setup.store, append(prefix, entry)...)
	require.NoError(t, err)
	require.Equal(t, entry.GetIndex(), setup.fsm.LastPersistedIndex())
	if !cancelDuringWAL {
		select {
		case err := <-results:
			require.NoError(t, err)
		case <-waitCtx.Done():
			t.Fatal("promotion lost its original commit after retry")
		}
	}
	snapshot, err := realWAL.Snapshot()
	require.NoError(t, err)
	require.ElementsMatch(t, []uint64{1, 2}, snapshot.GetMetadata().GetConfState().GetVoters())
	require.Empty(t, snapshot.GetMetadata().GetConfState().GetLearners())
}
