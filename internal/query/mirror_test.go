package query_test

import (
	"context"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/stretchr/testify/require"
)

func TestReadMirrorSourceHead(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Initially returns 0
	head, err := query.ReadMirrorSourceHead(s, "my-ledger")
	require.NoError(t, err)
	require.Equal(t, uint64(0), head)

	// Write source head
	batch := s.NewBatch()
	require.NoError(t, state.SetMirrorSourceHead(batch, "my-ledger", 42))
	require.NoError(t, batch.Commit())

	head, err = query.ReadMirrorSourceHead(s, "my-ledger")
	require.NoError(t, err)
	require.Equal(t, uint64(42), head)
}

func TestReadMirrorSyncProgress_Syncing(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write cursor=5, sourceHead=100
	batch := s.NewBatch()
	require.NoError(t, state.SetMirrorCursor(batch, "my-ledger", 5))
	require.NoError(t, state.SetMirrorSourceHead(batch, "my-ledger", 100))
	require.NoError(t, batch.Commit())

	progress, err := query.ReadMirrorSyncProgress(context.Background(), s, "my-ledger")
	require.NoError(t, err)
	require.Equal(t, commonpb.MirrorSyncState_MIRROR_SYNC_STATE_SYNCING, progress.State)
	require.Equal(t, uint64(5), progress.Cursor)
	require.Equal(t, uint64(100), progress.SourceLogCount)
	require.Equal(t, uint64(95), progress.RemainingLogs)
	require.Nil(t, progress.Error)
}

func TestReadMirrorSyncProgress_Following(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write cursor=100, sourceHead=100
	batch := s.NewBatch()
	require.NoError(t, state.SetMirrorCursor(batch, "my-ledger", 100))
	require.NoError(t, state.SetMirrorSourceHead(batch, "my-ledger", 100))
	require.NoError(t, batch.Commit())

	progress, err := query.ReadMirrorSyncProgress(context.Background(), s, "my-ledger")
	require.NoError(t, err)
	require.Equal(t, commonpb.MirrorSyncState_MIRROR_SYNC_STATE_FOLLOWING, progress.State)
	require.Equal(t, uint64(100), progress.Cursor)
	require.Equal(t, uint64(100), progress.SourceLogCount)
	require.Equal(t, uint64(0), progress.RemainingLogs)
}

func TestReadMirrorSyncProgress_WithError(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Write cursor and error
	batch := s.NewBatch()
	require.NoError(t, state.SetMirrorCursor(batch, "my-ledger", 10))
	require.NoError(t, state.SetMirrorSourceHead(batch, "my-ledger", 50))
	require.NoError(t, state.SetMirrorStatus(batch, "my-ledger", &commonpb.MirrorSyncError{
		Message: "connection refused",
	}))
	require.NoError(t, batch.Commit())

	progress, err := query.ReadMirrorSyncProgress(context.Background(), s, "my-ledger")
	require.NoError(t, err)
	require.Equal(t, commonpb.MirrorSyncState_MIRROR_SYNC_STATE_SYNCING, progress.State)
	require.Equal(t, uint64(40), progress.RemainingLogs)
	require.NotNil(t, progress.Error)
	require.Equal(t, "connection refused", progress.Error.Message)
}

func TestReadMirrorSyncProgress_NoData(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// No data written — should return SYNCING with zeros
	progress, err := query.ReadMirrorSyncProgress(context.Background(), s, "my-ledger")
	require.NoError(t, err)
	require.Equal(t, commonpb.MirrorSyncState_MIRROR_SYNC_STATE_SYNCING, progress.State)
	require.Equal(t, uint64(0), progress.Cursor)
	require.Equal(t, uint64(0), progress.SourceLogCount)
	require.Equal(t, uint64(0), progress.RemainingLogs)
}
