package query_test

import (
	"context"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestReadMirrorSourceHead(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)

	// Initially returns 0
	head, err := query.ReadMirrorSourceHead(s, "my-ledger")
	require.NoError(t, err)
	require.Equal(t, uint64(0), head)

	// Write source head
	batch := s.OpenWriteSession()
	require.NoError(t, state.SetMirrorSourceHead(batch, "my-ledger", 42))
	require.NoError(t, batch.Commit())

	head, err = query.ReadMirrorSourceHead(s, "my-ledger")
	require.NoError(t, err)
	require.Equal(t, uint64(42), head)
}

func TestReadMirrorSyncProgress_Syncing(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	attrs := attributes.New()

	// Applied boundary=5, sourceHead=100
	batch := s.OpenWriteSession()
	_, err := attrs.Boundary.Set(batch, []byte("my-ledger"), &raftcmdpb.LedgerBoundaries{LastMirrorV2LogId: 5})
	require.NoError(t, err)
	require.NoError(t, state.SetMirrorSourceHead(batch, "my-ledger", 100))
	require.NoError(t, batch.Commit())

	progress, err := query.ReadMirrorSyncProgress(context.Background(), s, attrs.Boundary, "my-ledger")
	require.NoError(t, err)
	require.Equal(t, commonpb.MirrorSyncState_MIRROR_SYNC_STATE_SYNCING, progress.GetState())
	require.Equal(t, uint64(5), progress.GetCursor())
	require.Equal(t, uint64(100), progress.GetSourceLogCount())
	require.Equal(t, uint64(95), progress.GetRemainingLogs())
	require.Nil(t, progress.GetError())
}

func TestReadMirrorSyncProgress_Following(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	attrs := attributes.New()

	// Applied boundary=100, sourceHead=100
	batch := s.OpenWriteSession()
	_, err := attrs.Boundary.Set(batch, []byte("my-ledger"), &raftcmdpb.LedgerBoundaries{LastMirrorV2LogId: 100})
	require.NoError(t, err)
	require.NoError(t, state.SetMirrorSourceHead(batch, "my-ledger", 100))
	require.NoError(t, batch.Commit())

	progress, err := query.ReadMirrorSyncProgress(context.Background(), s, attrs.Boundary, "my-ledger")
	require.NoError(t, err)
	require.Equal(t, commonpb.MirrorSyncState_MIRROR_SYNC_STATE_FOLLOWING, progress.GetState())
	require.Equal(t, uint64(100), progress.GetCursor())
	require.Equal(t, uint64(100), progress.GetSourceLogCount())
	require.Equal(t, uint64(0), progress.GetRemainingLogs())
}

func TestReadMirrorSyncProgress_WithError(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	attrs := attributes.New()

	// Applied boundary and error
	batch := s.OpenWriteSession()
	_, err := attrs.Boundary.Set(batch, []byte("my-ledger"), &raftcmdpb.LedgerBoundaries{LastMirrorV2LogId: 10})
	require.NoError(t, err)
	require.NoError(t, state.SetMirrorSourceHead(batch, "my-ledger", 50))
	require.NoError(t, state.SetMirrorStatus(batch, "my-ledger", &commonpb.MirrorSyncError{
		Message: "connection refused",
	}))
	require.NoError(t, batch.Commit())

	progress, err := query.ReadMirrorSyncProgress(context.Background(), s, attrs.Boundary, "my-ledger")
	require.NoError(t, err)
	require.Equal(t, commonpb.MirrorSyncState_MIRROR_SYNC_STATE_SYNCING, progress.GetState())
	require.Equal(t, uint64(40), progress.GetRemainingLogs())
	require.NotNil(t, progress.GetError())
	require.Equal(t, "connection refused", progress.GetError().GetMessage())
}

func TestReadMirrorSyncProgress_NoData(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	attrs := attributes.New()

	// No data written — should return SYNCING with zeros
	progress, err := query.ReadMirrorSyncProgress(context.Background(), s, attrs.Boundary, "my-ledger")
	require.NoError(t, err)
	require.Equal(t, commonpb.MirrorSyncState_MIRROR_SYNC_STATE_SYNCING, progress.GetState())
	require.Equal(t, uint64(0), progress.GetCursor())
	require.Equal(t, uint64(0), progress.GetSourceLogCount())
	require.Equal(t, uint64(0), progress.GetRemainingLogs())
	require.Nil(t, progress.GetError())
}

// An orphan SubPLMirrorCursor row left in an old store must not influence
// progress: nothing reads that sub-prefix any more (EN-1513).
func TestReadMirrorSyncProgress_IgnoresOrphanCursorRow(t *testing.T) {
	t.Parallel()

	s := newTestStore(t)
	attrs := attributes.New()

	batch := s.OpenWriteSession()
	_, err := attrs.Boundary.Set(batch, []byte("my-ledger"), &raftcmdpb.LedgerBoundaries{LastMirrorV2LogId: 5})
	require.NoError(t, err)
	require.NoError(t, state.SetMirrorSourceHead(batch, "my-ledger", 100))
	// Hand-write a row under the retired 0x05 sub-prefix, as an old store would have.
	orphan := dal.NewKeyBuilder().Reset().
		PutZonePrefix(dal.ZonePerLedger, 0x05).
		PutLedgerNameFixed("my-ledger").
		Build()
	require.NoError(t, batch.SetBytes(orphan, binary.BigEndian.AppendUint64(nil, 999)))
	require.NoError(t, batch.Commit())

	progress, err := query.ReadMirrorSyncProgress(context.Background(), s, attrs.Boundary, "my-ledger")
	require.NoError(t, err)
	require.Equal(t, uint64(5), progress.GetCursor(), "orphan 0x05 row must be ignored")
	require.Equal(t, commonpb.MirrorSyncState_MIRROR_SYNC_STATE_SYNCING, progress.GetState())
}
