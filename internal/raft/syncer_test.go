package raft

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/mock/gomock"
)

func TestSyncerApplyEntries(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type State struct{}

	ctrl := gomock.NewController(t)
	fsm := NewMockFSM[State](ctrl)
	spool := NewMockSpool(ctrl)
	snapshotStore := NewMockSnapshotStore(ctrl)
	syncer := newSyncer(spool, fsm, logging.FromContext(ctx), snapshotStore)
	go syncer.run()
	t.Cleanup(syncer.stop)

	cmd := Command{
		Type: "echo",
	}

	fsm.EXPECT().
		ApplyEntries(gomock.Any(), cmd).
		Return([]ApplyResult{{}}, nil)

	_, err := syncer.ApplyEntries(ctx, cmd)
	require.NoError(t, err)
}

func TestSyncerCreateSnapshot(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type State struct{}

	ctrl := gomock.NewController(t)
	fsm := NewMockFSM[State](ctrl)
	spool := NewMockSpool(ctrl)
	snapshotStore := NewMockSnapshotStore(ctrl)
	syncer := newSyncer(spool, fsm, logging.FromContext(ctx), snapshotStore)
	go syncer.run()
	t.Cleanup(syncer.stop)

	snapshotReady := make(chan struct{})
	snapshotting := make(chan struct{})
	fsm.EXPECT().
		CreateSnapshot(gomock.Any()).
		DoAndReturn(func(ctx context.Context) ([]byte, error) {
			close(snapshotting)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-snapshotReady:
				return []byte("foo"), nil
			}
		})
	snapshotStore.EXPECT().
		CreateSnapshot(uint64(0), &raftpb.ConfState{}, []byte("foo")).
		Return(raftpb.Snapshot{}, nil)

	snapshotErr := make(chan error)
	go func() {
		snapshotErr <- syncer.CreateSnapshot(ctx, 0, &raftpb.ConfState{})
	}()

	select {
	case err := <-snapshotErr:
		require.NoError(t, err)
	case <-snapshotting:
	}

	cmd := Command{Type: "echo"}
	fsm.EXPECT().
		ApplyEntries(gomock.Any(), cmd).
		Return(nil, nil)

	_, err := syncer.ApplyEntries(ctx, cmd)
	require.NoError(t, err)

	close(snapshotReady)
	require.NoError(t, <-snapshotErr)

	fsm.EXPECT().
		ApplyEntries(gomock.Any(), cmd).
		Return(nil, nil)

	_, err = syncer.ApplyEntries(ctx, cmd)
	require.NoError(t, err)
}

func TestSyncerCreateSnapshotWhileAlreadySnapshotting(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	type State struct{}

	ctrl := gomock.NewController(t)
	fsm := NewMockFSM[State](ctrl)
	spool := NewMockSpool(ctrl)
	snapshotStore := NewMockSnapshotStore(ctrl)
	syncer := newSyncer(spool, fsm, logging.FromContext(ctx), snapshotStore)
	go syncer.run()
	t.Cleanup(syncer.stop)

	snapshotReady := make(chan struct{})
	snapshotting1 := make(chan struct{})
	fsm.EXPECT().
		CreateSnapshot(gomock.Any()).
		DoAndReturn(func(ctx context.Context) ([]byte, error) {
			close(snapshotting1)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})

	snapshotErr1 := make(chan error)
	go func() {
		snapshotErr1 <- syncer.CreateSnapshot(ctx, 0, &raftpb.ConfState{})
	}()

	select {
	case err := <-snapshotErr1:
		require.NoError(t, err)
	case <-snapshotting1:
	}

	snapshotting2 := make(chan struct{})
	fsm.EXPECT().
		CreateSnapshot(gomock.Any()).
		DoAndReturn(func(ctx context.Context) ([]byte, error) {
			close(snapshotting2)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		})

	snapshotErr2 := make(chan error)
	go func() {
		snapshotErr2 <- syncer.CreateSnapshot(ctx, 0, &raftpb.ConfState{})
	}()

	select {
	case err := <-snapshotErr1:
		require.Truef(t, errors.Is(err, context.Canceled), "Expected context.Canceled, got %v", err)
	case <-time.After(100 * time.Millisecond):
		require.Fail(t, "Timeout waiting for snapshotErr1")
	}

	snapshotting3 := make(chan struct{})
	fsm.EXPECT().
		CreateSnapshot(gomock.Any()).
		DoAndReturn(func(ctx context.Context) ([]byte, error) {
			close(snapshotting3)
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-snapshotReady:
				return []byte("foo"), nil
			}
		})

	snapshotErr3 := make(chan error)
	go func() {
		snapshotErr3 <- syncer.CreateSnapshot(ctx, 0, &raftpb.ConfState{})
	}()

	select {
	case err := <-snapshotErr2:
		require.Truef(t, errors.Is(err, context.Canceled), "Expected context.Canceled, got %v", err)
	case <-time.After(100 * time.Millisecond):
		require.Fail(t, "Timeout waiting for snapshotErr2")
	}

	snapshotStore.EXPECT().
		CreateSnapshot(uint64(0), &raftpb.ConfState{}, []byte("foo")).
		Return(raftpb.Snapshot{}, nil)

	close(snapshotReady)

	require.NoError(t, <-snapshotErr3)
}
