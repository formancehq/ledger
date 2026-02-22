package wal

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/mock/gomock"
)

func TestInterceptor_Delegate(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	require.Same(t, mock, interceptor.Delegate())
}

func TestInterceptor_InitialState_Passthrough(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	expectedHS := raftpb.HardState{Term: 1, Vote: 2, Commit: 3}
	expectedCS := raftpb.ConfState{Voters: []uint64{1, 2}}
	mock.EXPECT().InitialState().Return(expectedHS, expectedCS, nil)

	hs, cs, err := interceptor.InitialState()
	require.NoError(t, err)
	require.Equal(t, expectedHS, hs)
	require.Equal(t, expectedCS, cs)
}

func TestInterceptor_InitialState_Intercepted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	customHS := raftpb.HardState{Term: 99}
	customCS := raftpb.ConfState{Voters: []uint64{99}}
	interceptor.SetInitialStateInterceptor(func(delegate WAL) (raftpb.HardState, raftpb.ConfState, error) {
		return customHS, customCS, nil
	})

	hs, cs, err := interceptor.InitialState()
	require.NoError(t, err)
	require.Equal(t, customHS, hs)
	require.Equal(t, customCS, cs)
}

func TestInterceptor_Entries_Passthrough(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	expected := []raftpb.Entry{{Index: 1, Term: 1}}
	mock.EXPECT().Entries(uint64(1), uint64(2), uint64(100)).Return(expected, nil)

	entries, err := interceptor.Entries(1, 2, 100)
	require.NoError(t, err)
	require.Equal(t, expected, entries)
}

func TestInterceptor_Entries_Intercepted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	errInjected := errors.New("injected")
	interceptor.SetEntriesInterceptor(func(delegate WAL, lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
		return nil, errInjected
	})

	_, err := interceptor.Entries(1, 2, 100)
	require.ErrorIs(t, err, errInjected)
}

func TestInterceptor_Term_Passthrough(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	mock.EXPECT().Term(uint64(5)).Return(uint64(2), nil)

	term, err := interceptor.Term(5)
	require.NoError(t, err)
	require.Equal(t, uint64(2), term)
}

func TestInterceptor_Term_Intercepted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	interceptor.SetTermInterceptor(func(delegate WAL, i uint64) (uint64, error) {
		return 42, nil
	})

	term, err := interceptor.Term(5)
	require.NoError(t, err)
	require.Equal(t, uint64(42), term)
}

func TestInterceptor_LastIndex_Passthrough(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	mock.EXPECT().LastIndex().Return(uint64(10), nil)

	idx, err := interceptor.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(10), idx)
}

func TestInterceptor_LastIndex_Intercepted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	interceptor.SetLastIndexInterceptor(func(delegate WAL) (uint64, error) {
		return 999, nil
	})

	idx, err := interceptor.LastIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(999), idx)
}

func TestInterceptor_FirstIndex_Passthrough(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	mock.EXPECT().FirstIndex().Return(uint64(5), nil)

	idx, err := interceptor.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(5), idx)
}

func TestInterceptor_FirstIndex_Intercepted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	interceptor.SetFirstIndexInterceptor(func(delegate WAL) (uint64, error) {
		return 1, nil
	})

	idx, err := interceptor.FirstIndex()
	require.NoError(t, err)
	require.Equal(t, uint64(1), idx)
}

func TestInterceptor_Snapshot_Passthrough(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	expected := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 10}}
	mock.EXPECT().Snapshot().Return(expected, nil)

	snap, err := interceptor.Snapshot()
	require.NoError(t, err)
	require.Equal(t, expected, snap)
}

func TestInterceptor_Snapshot_Intercepted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	custom := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 42}}
	interceptor.SetSnapshotInterceptor(func(delegate WAL) (raftpb.Snapshot, error) {
		return custom, nil
	})

	snap, err := interceptor.Snapshot()
	require.NoError(t, err)
	require.Equal(t, custom, snap)
}

func TestInterceptor_CreateSnapshot_Passthrough(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	cs := &raftpb.ConfState{Voters: []uint64{1}}
	mock.EXPECT().CreateSnapshot(uint64(5), cs, []byte("data")).Return(nil)

	err := interceptor.CreateSnapshot(5, cs, []byte("data"))
	require.NoError(t, err)
}

func TestInterceptor_CreateSnapshot_Intercepted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	errInjected := errors.New("snapshot error")
	interceptor.SetCreateSnapshotInterceptor(func(delegate WAL, i uint64, r *raftpb.ConfState, data []byte) error {
		return errInjected
	})

	err := interceptor.CreateSnapshot(5, nil, nil)
	require.ErrorIs(t, err, errInjected)
}

func TestInterceptor_Compact_Passthrough(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	mock.EXPECT().Compact(uint64(10)).Return(nil)

	err := interceptor.Compact(10)
	require.NoError(t, err)
}

func TestInterceptor_Compact_Intercepted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	interceptor.SetCompactInterceptor(func(delegate WAL, u uint64) error {
		return errors.New("compact error")
	})

	err := interceptor.Compact(10)
	require.Error(t, err)
}

func TestInterceptor_Append_Passthrough(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	hs := raftpb.HardState{Term: 1}
	entries := []raftpb.Entry{{Index: 1, Term: 1}}
	mock.EXPECT().Append(hs, entries).Return(nil)

	err := interceptor.Append(hs, entries)
	require.NoError(t, err)
}

func TestInterceptor_Append_Intercepted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	var capturedEntries []raftpb.Entry
	interceptor.SetAppendInterceptor(func(delegate WAL, state raftpb.HardState, entries []raftpb.Entry) error {
		capturedEntries = entries
		return nil
	})

	entries := []raftpb.Entry{{Index: 1, Term: 1}}
	err := interceptor.Append(raftpb.HardState{}, entries)
	require.NoError(t, err)
	require.Equal(t, entries, capturedEntries)
}

func TestInterceptor_ApplySnapshot_Passthrough(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	snap := raftpb.Snapshot{Metadata: raftpb.SnapshotMetadata{Index: 5}}
	mock.EXPECT().ApplySnapshot(snap).Return(nil)

	err := interceptor.ApplySnapshot(snap)
	require.NoError(t, err)
}

func TestInterceptor_ApplySnapshot_Intercepted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	interceptor.SetApplySnapshotInterceptor(func(delegate WAL, snapshot raftpb.Snapshot) error {
		return errors.New("apply error")
	})

	err := interceptor.ApplySnapshot(raftpb.Snapshot{})
	require.Error(t, err)
}

func TestInterceptor_Close_Passthrough(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	mock.EXPECT().Close().Return(nil)

	err := interceptor.Close()
	require.NoError(t, err)
}

func TestInterceptor_Close_Intercepted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	interceptor.SetCloseInterceptor(func(delegate WAL) error {
		return errors.New("close error")
	})

	err := interceptor.Close()
	require.Error(t, err)
}

func TestInterceptor_ClearInterceptors(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockWAL(ctrl)
	interceptor := NewWALInterceptor(mock)

	// Set all interceptors
	interceptor.SetInitialStateInterceptor(func(delegate WAL) (raftpb.HardState, raftpb.ConfState, error) {
		return raftpb.HardState{}, raftpb.ConfState{}, errors.New("should not be called")
	})
	interceptor.SetEntriesInterceptor(func(delegate WAL, lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
		return nil, errors.New("should not be called")
	})
	interceptor.SetTermInterceptor(func(delegate WAL, i uint64) (uint64, error) {
		return 0, errors.New("should not be called")
	})
	interceptor.SetLastIndexInterceptor(func(delegate WAL) (uint64, error) {
		return 0, errors.New("should not be called")
	})
	interceptor.SetFirstIndexInterceptor(func(delegate WAL) (uint64, error) {
		return 0, errors.New("should not be called")
	})
	interceptor.SetSnapshotInterceptor(func(delegate WAL) (raftpb.Snapshot, error) {
		return raftpb.Snapshot{}, errors.New("should not be called")
	})
	interceptor.SetCreateSnapshotInterceptor(func(delegate WAL, i uint64, r *raftpb.ConfState, data []byte) error {
		return errors.New("should not be called")
	})
	interceptor.SetCompactInterceptor(func(delegate WAL, u uint64) error {
		return errors.New("should not be called")
	})
	interceptor.SetAppendInterceptor(func(delegate WAL, state raftpb.HardState, entries []raftpb.Entry) error {
		return errors.New("should not be called")
	})
	interceptor.SetApplySnapshotInterceptor(func(delegate WAL, snapshot raftpb.Snapshot) error {
		return errors.New("should not be called")
	})
	interceptor.SetCloseInterceptor(func(delegate WAL) error {
		return errors.New("should not be called")
	})

	// Clear all
	interceptor.ClearInterceptors()

	// Now all calls should pass through to the mock
	mock.EXPECT().InitialState().Return(raftpb.HardState{}, raftpb.ConfState{}, nil)
	_, _, err := interceptor.InitialState()
	require.NoError(t, err)

	mock.EXPECT().LastIndex().Return(uint64(0), nil)
	_, err = interceptor.LastIndex()
	require.NoError(t, err)

	mock.EXPECT().Close().Return(nil)
	err = interceptor.Close()
	require.NoError(t, err)
}
