package grpc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

type blockingCheckpointStore struct {
	root          string
	calls         atomic.Int32
	active        atomic.Int32
	maxActive     atomic.Int32
	firstStarted  chan struct{}
	secondStarted chan struct{}
	releaseFirst  chan struct{}
}

func (s *blockingCheckpointStore) CreateTemporaryCheckpoint(name string) (string, error) {
	call := s.calls.Add(1)
	active := s.active.Add(1)
	for {
		old := s.maxActive.Load()
		if active <= old || s.maxActive.CompareAndSwap(old, active) {
			break
		}
	}
	defer s.active.Add(-1)
	switch call {
	case 1:
		close(s.firstStarted)
		if s.releaseFirst != nil {
			<-s.releaseFirst
		}
	case 2:
		close(s.secondStarted)
	}
	path := filepath.Join(s.root, name)
	if err := os.MkdirAll(path, 0o750); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(path, "MANIFEST-000001"), []byte("manifest"), 0o640); err != nil {
		return "", err
	}

	return path, nil
}

func (s *blockingCheckpointStore) RemoveTemporaryCheckpoint(name string) error {
	return os.RemoveAll(filepath.Join(s.root, name))
}

func newBlockingSnapshotServer(t *testing.T, store *blockingCheckpointStore) *SnapshotServiceServerImpl {
	t.Helper()

	return NewSnapshotServiceServer(noopLogger{}, store, 0, testSnapshotDonor{}).(*SnapshotServiceServerImpl)
}

type testSnapshotDonor struct {
	waitErr            error
	lastPersistedIndex uint64
	resynchronizing    bool
	waitCalls          *atomic.Int32
}

func (d testSnapshotDonor) WaitForCheckpointable(context.Context, uint64) error {
	if d.waitCalls != nil {
		d.waitCalls.Add(1)
	}
	return d.waitErr
}

func (d testSnapshotDonor) SnapshotReadiness() (uint64, bool) {
	return d.lastPersistedIndex, d.resynchronizing
}

// TestPrepareSnapshotRefusesResynchronizingDonor checks the code a requester
// relies on to move to another donor without waiting: FailedPrecondition is
// final for the fetcher, unlike the retried transient codes.
func TestPrepareSnapshotRefusesResynchronizingDonor(t *testing.T) {
	t.Parallel()

	server := NewSnapshotServiceServer(noopLogger{}, nil, 0, testSnapshotDonor{waitErr: state.ErrStoreResynchronizing})

	_, err := server.PrepareSnapshot(context.Background(), &snapshotpb.PrepareSnapshotRequest{MinAppliedIndex: 7})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestPrepareSnapshotRefusesResynchronizingDonorAtZeroIndex(t *testing.T) {
	t.Parallel()

	server := NewSnapshotServiceServer(noopLogger{}, nil, 0, testSnapshotDonor{waitErr: state.ErrStoreResynchronizing})

	_, err := server.PrepareSnapshot(context.Background(), &snapshotpb.PrepareSnapshotRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestPrepareSnapshotKeepsOtherWaitErrorsOpaque guards the branch above: an
// ordinary wait failure must not be reported as a donor refusal.
func TestPrepareSnapshotKeepsOtherWaitErrorsOpaque(t *testing.T) {
	t.Parallel()

	server := NewSnapshotServiceServer(noopLogger{}, nil, 0, testSnapshotDonor{waitErr: errors.New("wait aborted")})

	_, err := server.PrepareSnapshot(context.Background(), &snapshotpb.PrepareSnapshotRequest{MinAppliedIndex: 7})
	require.Error(t, err)
	require.NotEqual(t, codes.FailedPrecondition, status.Code(err))
}

func TestCheckSnapshotReadinessDoesNotCreateCheckpoint(t *testing.T) {
	t.Parallel()

	server := NewSnapshotServiceServer(noopLogger{}, nil, 0, testSnapshotDonor{lastPersistedIndex: 42})

	resp, err := server.CheckSnapshotReadiness(context.Background(), &snapshotpb.CheckSnapshotReadinessRequest{
		MinAppliedIndex: 41,
	})
	require.NoError(t, err)
	require.True(t, resp.GetReady())
	require.Equal(t, uint64(42), resp.GetLastPersistedIndex())
	require.False(t, resp.GetResynchronizing())
}

func TestCheckSnapshotReadinessRejectsStaleOrResynchronizingDonor(t *testing.T) {
	t.Parallel()

	t.Run("stale", func(t *testing.T) {
		server := NewSnapshotServiceServer(noopLogger{}, nil, 0, testSnapshotDonor{lastPersistedIndex: 40})

		resp, err := server.CheckSnapshotReadiness(context.Background(), &snapshotpb.CheckSnapshotReadinessRequest{
			MinAppliedIndex: 41,
		})
		require.NoError(t, err)
		require.False(t, resp.GetReady())
		require.Equal(t, uint64(40), resp.GetLastPersistedIndex())
	})

	t.Run("resynchronizing", func(t *testing.T) {
		server := NewSnapshotServiceServer(noopLogger{}, nil, 0, testSnapshotDonor{lastPersistedIndex: 42, resynchronizing: true})

		resp, err := server.CheckSnapshotReadiness(context.Background(), &snapshotpb.CheckSnapshotReadinessRequest{
			MinAppliedIndex: 41,
		})
		require.NoError(t, err)
		require.False(t, resp.GetReady())
		require.True(t, resp.GetResynchronizing())
	})
}

func TestPrepareSnapshotSharesSameFollowerPreparation(t *testing.T) {
	store := &blockingCheckpointStore{root: t.TempDir(), firstStarted: make(chan struct{}), secondStarted: make(chan struct{}), releaseFirst: make(chan struct{})}
	server := newBlockingSnapshotServer(t, store)
	firstDone := make(chan *snapshotpb.PrepareSnapshotResponse, 1)
	firstErr := make(chan error, 1)
	go func() {
		resp, err := server.PrepareSnapshot(t.Context(), &snapshotpb.PrepareSnapshotRequest{NodeId: "7"})
		firstDone <- resp
		firstErr <- err
	}()
	<-store.firstStarted
	secondDone := make(chan *snapshotpb.PrepareSnapshotResponse, 1)
	secondErr := make(chan error, 1)
	go func() {
		resp, err := server.PrepareSnapshot(t.Context(), &snapshotpb.PrepareSnapshotRequest{NodeId: "7"})
		secondDone <- resp
		secondErr <- err
	}()
	require.Eventually(t, func() bool {
		server.prepareMu.Lock()
		defer server.prepareMu.Unlock()

		return server.preparations["7"].waiters.Load() > 0
	}, time.Second, time.Millisecond)
	close(store.releaseFirst)
	firstResp, secondResp := <-firstDone, <-secondDone
	require.NoError(t, <-firstErr)
	require.NoError(t, <-secondErr)
	require.Equal(t, firstResp.GetSessionId(), secondResp.GetSessionId())
	require.Equal(t, int32(1), store.calls.Load())
}

func TestPrepareSnapshotHigherFloorCreatesFreshPreparation(t *testing.T) {
	store := &blockingCheckpointStore{root: t.TempDir(), firstStarted: make(chan struct{}), secondStarted: make(chan struct{}), releaseFirst: make(chan struct{})}
	server := newBlockingSnapshotServer(t, store)
	firstDone := make(chan error, 1)
	go func() {
		_, err := server.PrepareSnapshot(t.Context(), &snapshotpb.PrepareSnapshotRequest{NodeId: "7", MinAppliedIndex: 10})
		firstDone <- err
	}()
	<-store.firstStarted
	secondDone := make(chan error, 1)
	go func() {
		_, err := server.PrepareSnapshot(t.Context(), &snapshotpb.PrepareSnapshotRequest{NodeId: "7", MinAppliedIndex: 20})
		secondDone <- err
	}()
	require.Eventually(t, func() bool {
		server.prepareMu.Lock()
		defer server.prepareMu.Unlock()
		prep := server.preparations["7"]

		return prep != nil && prep.waiters.Load() > 0
	}, time.Second, time.Millisecond)
	require.Equal(t, int32(1), store.calls.Load())
	close(store.releaseFirst)
	<-store.secondStarted
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
	require.Equal(t, int32(2), store.calls.Load())
}

func TestPrepareSnapshotSerializesDifferentFollowers(t *testing.T) {
	store := &blockingCheckpointStore{root: t.TempDir(), firstStarted: make(chan struct{}), secondStarted: make(chan struct{}), releaseFirst: make(chan struct{})}
	server := newBlockingSnapshotServer(t, store)
	firstDone := make(chan error, 1)
	go func() {
		_, err := server.PrepareSnapshot(t.Context(), &snapshotpb.PrepareSnapshotRequest{NodeId: "7"})
		firstDone <- err
	}()
	<-store.firstStarted
	secondDone := make(chan error, 1)
	go func() {
		_, err := server.PrepareSnapshot(t.Context(), &snapshotpb.PrepareSnapshotRequest{NodeId: "8"})
		secondDone <- err
	}()
	require.Eventually(t, func() bool {
		server.prepareMu.Lock()
		defer server.prepareMu.Unlock()
		_, ok := server.preparations["8"]

		return ok
	}, time.Second, time.Millisecond)
	require.Equal(t, int32(1), store.calls.Load())
	close(store.releaseFirst)
	<-store.secondStarted
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
	require.Equal(t, int32(1), store.maxActive.Load())
}

func TestPrepareSnapshotDoesNotShareUnidentifiedRequests(t *testing.T) {
	store := &blockingCheckpointStore{root: t.TempDir(), firstStarted: make(chan struct{}), secondStarted: make(chan struct{}), releaseFirst: make(chan struct{})}
	waitCalls := new(atomic.Int32)
	server := NewSnapshotServiceServer(noopLogger{}, store, 0, testSnapshotDonor{waitCalls: waitCalls}).(*SnapshotServiceServerImpl)
	firstDone := make(chan error, 1)
	go func() {
		_, err := server.PrepareSnapshot(t.Context(), &snapshotpb.PrepareSnapshotRequest{})
		firstDone <- err
	}()
	<-store.firstStarted
	secondDone := make(chan error, 1)
	go func() {
		_, err := server.PrepareSnapshot(t.Context(), &snapshotpb.PrepareSnapshotRequest{})
		secondDone <- err
	}()
	require.Eventually(t, func() bool { return waitCalls.Load() == 2 }, time.Second, time.Millisecond)
	require.Equal(t, int32(1), store.calls.Load())
	close(store.releaseFirst)
	<-store.secondStarted
	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
	require.Equal(t, int32(2), store.calls.Load())
}

func TestPrepareSnapshotLiveWaiterReplacesCanceledQueuedPreparation(t *testing.T) {
	store := &blockingCheckpointStore{root: t.TempDir(), firstStarted: make(chan struct{}), secondStarted: make(chan struct{})}
	server := newBlockingSnapshotServer(t, store)
	server.prepareSlots <- struct{}{}
	firstCtx, cancelFirst := context.WithCancel(t.Context())
	firstDone := make(chan error, 1)
	go func() {
		_, err := server.PrepareSnapshot(firstCtx, &snapshotpb.PrepareSnapshotRequest{NodeId: "7"})
		firstDone <- err
	}()
	require.Eventually(t, func() bool {
		server.prepareMu.Lock()
		defer server.prepareMu.Unlock()
		_, ok := server.preparations["7"]

		return ok
	}, time.Second, time.Millisecond)
	secondDone := make(chan error, 1)
	go func() {
		_, err := server.PrepareSnapshot(t.Context(), &snapshotpb.PrepareSnapshotRequest{NodeId: "7"})
		secondDone <- err
	}()
	require.Eventually(t, func() bool {
		server.prepareMu.Lock()
		defer server.prepareMu.Unlock()
		prep := server.preparations["7"]

		return prep != nil && prep.waiters.Load() > 0
	}, time.Second, time.Millisecond)
	cancelFirst()
	require.ErrorIs(t, <-firstDone, context.Canceled)
	<-server.prepareSlots
	require.NoError(t, <-secondDone)
	require.Equal(t, int32(1), store.calls.Load())
}
