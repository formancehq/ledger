package spool

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/mock/gomock"
)

func TestInterceptorPassthroughWhenNoInterceptors(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockSpool(ctrl)

	interceptor := NewInterceptor(mock)

	ctx := context.Background()
	entry := raftpb.Entry{Index: 1}

	mock.EXPECT().AppendCommittedEntries(ctx, entry).Return(nil)
	require.NoError(t, interceptor.AppendCommittedEntries(ctx, entry))

	mock.EXPECT().End().Return(&Position{SegID: 1, Offset: 42}, nil)
	pos, err := interceptor.End()
	require.NoError(t, err)
	require.Equal(t, uint64(1), pos.SegID)
	require.Equal(t, int64(42), pos.Offset)

	mock.EXPECT().ReplayUntil(ctx, Position{}, uint64(0), gomock.Any()).Return(nil)
	require.NoError(t, interceptor.ReplayUntil(ctx, Position{}, 0, func(_ raftpb.Entry) error { return nil }))

	mock.EXPECT().Prune(uint64(10)).Return(nil)
	require.NoError(t, interceptor.Prune(10))

	mock.EXPECT().Close().Return(nil)
	require.NoError(t, interceptor.Close())
}

func TestInterceptorSetAppendCommittedEntriesInterceptor(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockSpool(ctrl)
	interceptor := NewInterceptor(mock)

	var interceptedEntries []raftpb.Entry

	interceptor.SetAppendCommittedEntriesInterceptor(func(ctx context.Context, delegate Spool, entries []raftpb.Entry) error {
		interceptedEntries = entries

		return nil
	})

	entry := raftpb.Entry{Index: 5}
	require.NoError(t, interceptor.AppendCommittedEntries(context.Background(), entry))

	require.Len(t, interceptedEntries, 1)
	require.Equal(t, uint64(5), interceptedEntries[0].Index)
}

func TestInterceptorSetEndInterceptor(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockSpool(ctrl)
	interceptor := NewInterceptor(mock)

	expected := &Position{SegID: 99, Offset: 1234}
	interceptor.SetEndInterceptor(func(_ Spool) (*Position, error) {
		return expected, nil
	})

	pos, err := interceptor.End()
	require.NoError(t, err)
	require.Equal(t, expected, pos)
}

func TestInterceptorSetReplayUntilInterceptor(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockSpool(ctrl)
	interceptor := NewInterceptor(mock)

	var capturedLastApplied uint64
	interceptor.SetReplayUntilInterceptor(func(_ context.Context, _ Spool, _ Position, lastApplied uint64, _ func(raftpb.Entry) error) error {
		capturedLastApplied = lastApplied

		return nil
	})

	require.NoError(t, interceptor.ReplayUntil(context.Background(), Position{}, 42, nil))
	require.Equal(t, uint64(42), capturedLastApplied)
}

func TestInterceptorSetPruneInterceptor(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockSpool(ctrl)
	interceptor := NewInterceptor(mock)

	pruneErr := errors.New("prune failed")
	interceptor.SetPruneInterceptor(func(_ Spool, _ uint64) error {
		return pruneErr
	})

	err := interceptor.Prune(10)
	require.ErrorIs(t, err, pruneErr)
}

func TestInterceptorSetCloseInterceptor(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockSpool(ctrl)
	interceptor := NewInterceptor(mock)

	var closeCalled bool
	interceptor.SetCloseInterceptor(func(delegate Spool) error {
		closeCalled = true

		return delegate.Close()
	})

	mock.EXPECT().Close().Return(nil)
	require.NoError(t, interceptor.Close())
	require.True(t, closeCalled)
}

func TestInterceptorClearInterceptors(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockSpool(ctrl)
	interceptor := NewInterceptor(mock)

	// Set all interceptors
	interceptor.SetAppendCommittedEntriesInterceptor(func(_ context.Context, _ Spool, _ []raftpb.Entry) error {
		return errors.New("should not be called")
	})
	interceptor.SetEndInterceptor(func(_ Spool) (*Position, error) {
		return nil, errors.New("should not be called")
	})
	interceptor.SetPruneInterceptor(func(_ Spool, _ uint64) error {
		return errors.New("should not be called")
	})

	// Clear them
	interceptor.ClearInterceptors()

	// Now calls should pass through to delegate
	mock.EXPECT().AppendCommittedEntries(gomock.Any()).Return(nil)
	require.NoError(t, interceptor.AppendCommittedEntries(context.Background()))

	mock.EXPECT().End().Return(&Position{}, nil)
	_, err := interceptor.End()
	require.NoError(t, err)

	mock.EXPECT().Prune(uint64(0)).Return(nil)
	require.NoError(t, interceptor.Prune(0))
}

func TestInterceptorDelegate(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockSpool(ctrl)
	interceptor := NewInterceptor(mock)

	require.Equal(t, mock, interceptor.Delegate())
}
