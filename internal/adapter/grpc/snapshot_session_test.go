package grpc

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func newSnapshotSessionFixture(t *testing.T, syncName string) (*dal.Store, *snapshotSessionStore, string, string) {
	t.Helper()

	store, err := dal.NewStore(
		t.TempDir(),
		noopLogger{},
		noop.NewMeterProvider().Meter("test"),
		dal.DefaultConfig(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	checkpointPath, err := store.CreateTemporaryCheckpoint(syncName)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(checkpointPath, "data.bin"), []byte("checkpoint data"), 0600))

	sessions := newSnapshotSessionStore(store, noopLogger{}, defaultSessionTTL)
	t.Cleanup(sessions.stop)

	sessionID, err := sessions.create(syncName, checkpointPath)
	require.NoError(t, err)

	return store, sessions, sessionID, checkpointPath
}

func TestSnapshotSessionStore_CloseDefersCleanupUntilActiveFetchReleases(t *testing.T) {
	t.Parallel()

	const syncName = "active-fetch"
	store, sessions, sessionID, _ := newSnapshotSessionFixture(t, syncName)

	session, ok := sessions.acquire(sessionID)
	require.True(t, ok)
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { sessions.release(session) })
	}
	t.Cleanup(release)

	// Reproduce the failing interleaving: FetchFile has acquired the session,
	// then CloseSession removes it before FetchFile opens the rooted file.
	sessions.remove(sessionID)

	var received []byte
	err := streamOneFile(session.checkpointPath, "data.bin", make([]byte, defaultChunkSize), func(resp *snapshotpb.FetchFileResponse) error {
		received = append(received, resp.GetData()...)

		return nil
	})
	require.NoError(t, err)
	require.Equal(t, []byte("checkpoint data"), received)

	_, exists := store.TemporaryCheckpointPath(syncName)
	require.True(t, exists)

	release()

	_, exists = store.TemporaryCheckpointPath(syncName)
	require.False(t, exists)
}

func TestSnapshotService_FetchFileReleasesSessionUseOnCompletion(t *testing.T) {
	t.Parallel()

	const syncName = "completed-fetch"
	store, sessions, sessionID, _ := newSnapshotSessionFixture(t, syncName)
	server := &SnapshotServiceServerImpl{logger: noopLogger{}, sessions: sessions}
	stream := newFakeServerStream[snapshotpb.FetchFileResponse](t)

	require.NoError(t, server.FetchFile(&snapshotpb.FetchFileRequest{
		SessionId: sessionID,
		Path:      "data.bin",
	}, stream))
	require.NotEmpty(t, stream.sent)

	_, err := server.CloseSession(context.Background(), &snapshotpb.CloseSessionRequest{SessionId: sessionID})
	require.NoError(t, err)

	_, exists := store.TemporaryCheckpointPath(syncName)
	require.False(t, exists)
}

func TestSnapshotService_CloseRacingActiveFetchDefersCleanup(t *testing.T) {
	t.Parallel()

	const syncName = "close-race"
	store, sessions, sessionID, _ := newSnapshotSessionFixture(t, syncName)
	server := &SnapshotServiceServerImpl{logger: noopLogger{}, sessions: sessions}

	firstSend := make(chan struct{})
	continueSend := make(chan struct{})
	var continueOnce sync.Once
	unblockSend := func() {
		continueOnce.Do(func() { close(continueSend) })
	}
	t.Cleanup(unblockSend)

	stream := NewMockServerStreamingServer[snapshotpb.FetchFileResponse](gomock.NewController(t))
	var sendCount atomic.Int32
	stream.EXPECT().Send(gomock.Any()).DoAndReturn(func(*snapshotpb.FetchFileResponse) error {
		if sendCount.Add(1) == 1 {
			close(firstSend)
			<-continueSend
		}

		return nil
	}).AnyTimes()

	fetchDone := make(chan error, 1)
	go func() {
		fetchDone <- server.FetchFile(&snapshotpb.FetchFileRequest{
			SessionId: sessionID,
			Path:      "data.bin",
		}, stream)
	}()

	<-firstSend

	_, err := server.CloseSession(context.Background(), &snapshotpb.CloseSessionRequest{SessionId: sessionID})
	require.NoError(t, err)
	_, exists := store.TemporaryCheckpointPath(syncName)
	require.True(t, exists)
	_, ok := sessions.acquire(sessionID)
	require.False(t, ok)

	unblockSend()
	require.NoError(t, <-fetchDone)

	_, exists = store.TemporaryCheckpointPath(syncName)
	require.False(t, exists)
}

func TestSnapshotSessionStore_ExpiryDefersCleanupUntilActiveFetchReleases(t *testing.T) {
	t.Parallel()

	const syncName = "expired-fetch"
	store, sessions, sessionID, _ := newSnapshotSessionFixture(t, syncName)
	session, ok := sessions.acquire(sessionID)
	require.True(t, ok)

	sessions.mu.Lock()
	session.lastAccess = time.Now().Add(-sessions.ttl - time.Second)
	sessions.mu.Unlock()
	sessions.reapExpired()

	_, ok = sessions.acquire(sessionID)
	require.False(t, ok)
	_, exists := store.TemporaryCheckpointPath(syncName)
	require.True(t, exists)

	sessions.release(session)

	_, exists = store.TemporaryCheckpointPath(syncName)
	require.False(t, exists)
}

func TestSnapshotSessionStore_CleanupWaitsForLastActiveFetch(t *testing.T) {
	t.Parallel()

	const syncName = "parallel-fetches"
	store, sessions, sessionID, _ := newSnapshotSessionFixture(t, syncName)
	first, ok := sessions.acquire(sessionID)
	require.True(t, ok)
	second, ok := sessions.acquire(sessionID)
	require.True(t, ok)

	sessions.remove(sessionID)
	sessions.release(first)

	_, exists := store.TemporaryCheckpointPath(syncName)
	require.True(t, exists)

	sessions.release(second)

	_, exists = store.TemporaryCheckpointPath(syncName)
	require.False(t, exists)
}

func TestSnapshotSessionStore_StopDefersCleanupUntilActiveFetchReleases(t *testing.T) {
	t.Parallel()

	const syncName = "shutdown-fetch"
	store, sessions, sessionID, checkpointPath := newSnapshotSessionFixture(t, syncName)
	session, ok := sessions.acquire(sessionID)
	require.True(t, ok)

	sessions.stop()

	_, ok = sessions.acquire(sessionID)
	require.False(t, ok)
	_, exists := store.TemporaryCheckpointPath(syncName)
	require.True(t, exists)
	_, err := sessions.create("after-stop", checkpointPath)
	require.ErrorContains(t, err, "stopped")

	sessions.release(session)

	_, exists = store.TemporaryCheckpointPath(syncName)
	require.False(t, exists)
}
