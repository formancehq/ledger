package grpc

import (
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func stagedFinalizationServer(t *testing.T) *RestoreServiceServerImpl {
	t.Helper()

	server := NewRestoreServiceServer(t.TempDir(), "target-cluster", 1, noopLogger{})
	store, err := dal.OpenDirect(server.stagingDir(), noopLogger{})
	require.NoError(t, err)
	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, "restored-ledger", &commonpb.LedgerInfo{Name: "restored-ledger"}))
	require.NoError(t, batch.Commit())
	server.stagingStore = store
	server.downloaded = true
	server.validated = true
	t.Cleanup(server.Shutdown)

	return server
}

func requirePublishedRestore(t *testing.T, server *RestoreServiceServerImpl) {
	t.Helper()

	marker, err := node.ReadRestoredMarker(server.dataDir)
	require.NoError(t, err)
	require.NotNil(t, marker)
	require.EqualValues(t, 1, marker.LastAppliedIndex)
	checkpointID, found, err := dal.ScanLatestCheckpointID(server.dataDir)
	require.NoError(t, err)
	require.True(t, found, "normal startup must discover the published checkpoint")
	require.Zero(t, checkpointID)

	checkpoint := filepath.Join(server.dataDir, "checkpoints", "0")
	store, err := dal.OpenDirect(checkpoint, noopLogger{})
	require.NoError(t, err, "published checkpoint must reopen after the staging handle closes")
	info, err := query.GetLedgerByName(t.Context(), store, "restored-ledger")
	require.NoError(t, err)
	require.Equal(t, "restored-ledger", info.GetName())
	require.NoError(t, store.Close())
}

func TestFinalizeRestoreCompetingOwnerPreservesPublishedCheckpoint(t *testing.T) {
	t.Parallel()

	server := stagedFinalizationServer(t)
	beforeClose := make(chan struct{})
	release := make(chan struct{})
	var entered atomic.Int32
	server.beforeFinalizeClose = func() {
		if entered.Add(1) == 1 {
			close(beforeClose)
			<-release
		}
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := server.FinalizeRestore(context.Background(), &restorepb.FinalizeRestoreRequest{})
		firstDone <- err
	}()
	<-beforeClose // The first call owns the generation after preparation and metadata reads.

	_, err := server.FinalizeRestore(context.Background(), &restorepb.FinalizeRestoreRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "already running")
	require.EqualValues(t, 1, entered.Load(), "the competitor must not prepare or publish")
	marker, err := node.ReadRestoredMarker(server.dataDir)
	require.NoError(t, err)
	require.Nil(t, marker)
	server.mu.Lock()
	require.NotNil(t, server.stagingStore, "the rejected call must not close the owner's handle")
	server.mu.Unlock()

	shutdownDone := make(chan struct{})
	go func() {
		server.Shutdown()
		close(shutdownDone)
	}()
	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned before the active finalizer published")
	default:
	}

	close(release)
	require.NoError(t, <-firstDone)
	<-shutdownDone
	requirePublishedRestore(t, server)

	_, err = server.FinalizeRestore(context.Background(), &restorepb.FinalizeRestoreRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err), "shutdown rejects later calls")
	requirePublishedRestore(t, server)
}

func TestFinalizeRestoreSequentialRetryPreservesPublishedCheckpoint(t *testing.T) {
	t.Parallel()

	server := stagedFinalizationServer(t)
	_, err := server.FinalizeRestore(context.Background(), &restorepb.FinalizeRestoreRequest{})
	require.NoError(t, err)
	requirePublishedRestore(t, server)

	_, err = server.FinalizeRestore(context.Background(), &restorepb.FinalizeRestoreRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	requirePublishedRestore(t, server)
}

func TestFinalizeRestoreRejectsOverlapBeforePreparation(t *testing.T) {
	t.Parallel()

	server := stagedFinalizationServer(t)
	claimed := make(chan struct{})
	release := make(chan struct{})
	server.beforeFinalizePrepare = func() {
		close(claimed)
		<-release
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := server.FinalizeRestore(t.Context(), &restorepb.FinalizeRestoreRequest{})
		firstDone <- err
	}()
	<-claimed

	_, err := server.FinalizeRestore(t.Context(), &restorepb.FinalizeRestoreRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "already running")
	server.mu.Lock()
	require.NotNil(t, server.stagingStore)
	server.mu.Unlock()
	close(release)
	require.NoError(t, <-firstDone)
	requirePublishedRestore(t, server)
}

func TestFinalizeRestoreMarkerFailureRollsBackCheckpoint(t *testing.T) {
	t.Parallel()

	server := stagedFinalizationServer(t)
	require.NoError(t, os.Mkdir(filepath.Join(server.dataDir, "RESTORED"), 0o755))
	_, err := server.FinalizeRestore(context.Background(), &restorepb.FinalizeRestoreRequest{})
	require.ErrorContains(t, err, "renaming restored marker")
	_, err = os.Stat(filepath.Join(server.dataDir, "checkpoints", "0"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(server.stagingDir())
	require.NoError(t, err, "failed publication retains staging files for diagnosis/re-download")
	server.mu.Lock()
	require.Nil(t, server.stagingStore, "the staging handle was closed before publication")
	require.False(t, server.finalizing)
	server.mu.Unlock()
}

func TestFinalizeRestorePublishesRebuiltPostCheckpointDelta(t *testing.T) {
	t.Parallel()

	server := stagedFinalizationServer(t)
	server.mu.Lock()
	store := server.stagingStore
	server.mu.Unlock()
	batch := store.OpenWriteSession()
	logKey := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(1).Build()
	auditKey := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(1).Build()
	require.NoError(t, batch.SetProto(logKey, &commonpb.Log{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: "delta-ledger", Id: 2},
		}},
	}))
	require.NoError(t, batch.SetProto(auditKey, &auditpb.AuditEntry{
		Sequence: 1,
		Outcome: &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{
			MinLogSequence: 1, MaxLogSequence: 1,
		}},
	}))
	require.NoError(t, batch.Commit())
	require.NoError(t, backup.RebuildDelta(t.Context(), noopLogger{}, store, 0, 0))

	_, err := server.FinalizeRestore(t.Context(), &restorepb.FinalizeRestoreRequest{})
	require.NoError(t, err)
	marker, err := node.ReadRestoredMarker(server.dataDir)
	require.NoError(t, err)
	require.NotNil(t, marker)
	checkpoint, err := dal.OpenDirect(filepath.Join(server.dataDir, "checkpoints", "0"), noopLogger{})
	require.NoError(t, err)
	info, err := query.GetLedgerByName(t.Context(), checkpoint, "delta-ledger")
	require.NoError(t, err)
	require.Equal(t, "delta-ledger", info.GetName())
	require.NoError(t, checkpoint.Close())
	requirePublishedRestore(t, server)
}
