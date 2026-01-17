package raft

import (
	"context"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/wal"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
)

func TestSyncerApplyEntries(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	ctrl := gomock.NewController(t)
	fsm := NewMockFSM(ctrl)
	spool := NewMockSpool(ctrl)
	store := NewMockStore(ctrl)

	wal, err := wal.New(t.TempDir(), logging.FromContext(ctx))
	require.NoError(t, err)

	syncer := newSyncer(
		spool,
		fsm,
		logging.FromContext(ctx),
		wal,
		noop.Meter{},
		store,
		10, 10,
	)

	fsm.EXPECT().
		ApplyEntries(gomock.Any(), raftpb.Entry{
			Index: 1,
		}).
		Return([]ApplyResult{{}}, nil)

	_, err = syncer.ApplyEntries(ctx, &raftpb.ConfState{}, raftpb.Entry{
		Index: 1,
	})
	require.NoError(t, err)
}

func TestSyncerSyncSnapshot(t *testing.T) {
	t.Parallel()

	t.Run("normal sync flow", func(t *testing.T) {
		t.Parallel()

		logger := logging.Testing()

		spoolDir := t.TempDir()

		mockCtrl := gomock.NewController(t)
		fsm := NewMockFSM(mockCtrl)
		wal := NewMockWAL(mockCtrl)
		store := NewMockStore(mockCtrl)

		spool, err := NewDefaultSpool(DefaultSpoolConfig{
			Dir: spoolDir,
		})
		require.NoError(t, err)

		const (
			snapshotThreshold = 10
			compactionMargin  = 2
		)

		syncer := newSyncer(spool, fsm, logger, wal, noop.Meter{}, store, snapshotThreshold, compactionMargin)

		syncingDone := make(chan struct{})
		syncingSnapshot := make(chan struct{})
		fsm.EXPECT().
			SyncSnapshot(gomock.Any(), gomock.Any(), gomock.Any()).
			DoAndReturn(func(ctx context.Context, snapshotIndex uint64, snapshot raftpb.Snapshot) error {
				// Block the sync for the test, it allows us to test the spool
				close(syncingSnapshot)
				<-syncingDone
				return nil
			})

		syncErr := make(chan error, 1)
		go func() {
			syncErr <- syncer.SyncSnapshot(context.Background(), 1, raftpb.Snapshot{})
		}()

		select {
		case <-syncingSnapshot:
		case <-time.After(100 * time.Millisecond):
			t.Fatal("snapshot creation did not start")
		}

		for i := range uint64(50) {
			_, err := syncer.ApplyEntries(context.Background(), &raftpb.ConfState{}, raftpb.Entry{
				Index: i + 1,
			})
			require.NoError(t, err)
		}

		// At this point the spool should be filled since the syncing process is blocked
		require.True(t, mockCtrl.Satisfied())

		store.EXPECT().
			GetLastAppliedIndex().
			Return(uint64(0), nil)

		for i := range uint64(50) {
			fsm.EXPECT().
				ApplyEntries(gomock.Any(), raftpb.Entry{
					Index: i + 1,
				}).
				Return([]ApplyResult{{}}, nil)
		}

		// Unblock the syncing process, it will empty the spool
		close(syncingDone)

		require.Eventually(t, mockCtrl.Satisfied, time.Second, 10*time.Millisecond, "expected all entries to be applied")

		// we'll trigger a new entry to trigger completion of the syncing process
		store.EXPECT().
			GetLastAppliedIndex().
			Return(uint64(50), nil)

		fsm.EXPECT().
			ApplyEntries(gomock.Any(), raftpb.Entry{
				Index: 51,
			}).
			Return([]ApplyResult{{}}, nil)

		fsm.EXPECT().
			CreateSnapshot(gomock.Any()).
			Return([]byte{}, nil)

		wal.EXPECT().
			Snapshot().
			Return(raftpb.Snapshot{}, nil)

		wal.EXPECT().
			CreateSnapshot(gomock.Any(), gomock.Any(), gomock.Any()).
			Return(nil)

		wal.EXPECT().
			Compact(uint64(49)). // 51-2
			Return(nil)

		_, err = syncer.ApplyEntries(context.Background(), &raftpb.ConfState{}, raftpb.Entry{
			Index: 51,
		})
		require.NoError(t, err)
	})
}
