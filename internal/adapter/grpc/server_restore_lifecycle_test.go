package grpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type cancellationBlockingReader struct {
	ctx      context.Context
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	closed   chan struct{}
	start    sync.Once
	cancel   sync.Once
	unblock  sync.Once
	close    sync.Once
}

func newCancellationBlockingReader(ctx context.Context) *cancellationBlockingReader {
	return &cancellationBlockingReader{
		ctx:      ctx,
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
		release:  make(chan struct{}),
		closed:   make(chan struct{}),
	}
}

func (r *cancellationBlockingReader) Read(_ []byte) (int, error) {
	r.start.Do(func() { close(r.started) })
	<-r.ctx.Done()
	r.cancel.Do(func() { close(r.canceled) })
	<-r.release

	return 0, r.ctx.Err()
}

func (r *cancellationBlockingReader) Close() error {
	r.close.Do(func() { close(r.closed) })

	return nil
}

func (r *cancellationBlockingReader) allowUnwind() {
	r.unblock.Do(func() { close(r.release) })
}

func startBlockedRestoreDownload(t *testing.T) (*RestoreServiceServerImpl, *downloadJob, *cancellationBlockingReader) {
	t.Helper()

	ctrl := gomock.NewController(t)
	storage := NewMockStorage(ctrl)
	readerReady := make(chan *cancellationBlockingReader, 1)
	storage.EXPECT().
		GetFile(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ string) (io.ReadCloser, error) {
			reader := newCancellationBlockingReader(ctx)
			readerReady <- reader

			return reader, nil
		})

	server := newServerForTest(t, staticFactory(storage))
	_, err := server.StartDownloadBackup(context.Background(), &restorepb.StartDownloadBackupRequest{})
	require.NoError(t, err)

	reader := <-readerReady
	<-reader.started

	server.mu.Lock()
	job := server.job
	server.mu.Unlock()
	require.NotNil(t, job)
	t.Cleanup(func() {
		reader.allowUnwind()
		server.Shutdown()
	})

	return server, job, reader
}

func TestRestoreShutdownCancelsAndJoinsActiveDownload(t *testing.T) {
	t.Parallel()

	server, job, reader := startBlockedRestoreDownload(t)
	server.BeginShutdown()
	<-reader.canceled

	shutdownDone := make(chan struct{})
	go func() {
		server.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned before the canceled download unwound")
	default:
	}
	select {
	case <-reader.closed:
		t.Fatal("the download resource closed before its owner unwound")
	default:
	}

	_, err := server.StartDownloadBackup(context.Background(), &restorepb.StartDownloadBackupRequest{})
	require.Equal(t, codes.Unavailable, status.Code(err))

	reader.allowUnwind()
	select {
	case <-shutdownDone:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not join the canceled download")
	}

	select {
	case <-reader.closed:
	default:
		t.Fatal("download resource was not closed during unwind")
	}
	select {
	case <-job.done:
	default:
		t.Fatal("shutdown returned before the download job terminated")
	}

	server.mu.Lock()
	state := job.state
	server.mu.Unlock()
	require.Equal(t, restorepb.DownloadState_DOWNLOAD_STATE_CANCELED, state)
}

func TestRestoreDownloadOutlivesInitiatingRPC(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	storage := NewMockStorage(ctrl)
	readerReady := make(chan *cancellationBlockingReader, 1)
	storage.EXPECT().
		GetFile(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ string) (io.ReadCloser, error) {
			reader := newCancellationBlockingReader(ctx)
			readerReady <- reader

			return reader, nil
		})

	server := newServerForTest(t, staticFactory(storage))
	rpcCtx, cancelRPC := context.WithCancel(context.Background())
	started, err := server.StartDownloadBackup(rpcCtx, &restorepb.StartDownloadBackupRequest{})
	require.NoError(t, err)

	reader := <-readerReady
	<-reader.started
	t.Cleanup(func() {
		reader.allowUnwind()
		server.Shutdown()
	})
	cancelRPC()

	select {
	case <-reader.canceled:
		t.Fatal("initiating RPC cancellation reached the detached download")
	default:
	}

	cancelDone := make(chan error, 1)
	go func() {
		_, err := server.CancelDownload(context.Background(), &restorepb.CancelDownloadRequest{JobId: started.GetJobId()})
		cancelDone <- err
	}()
	<-reader.canceled
	reader.allowUnwind()

	select {
	case err := <-cancelDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("explicit cancellation did not join the download")
	}

	server.Shutdown()
}

func TestRestoreShutdownWaitsForAdmittedRequestBeforeClosingStagingStore(t *testing.T) {
	t.Parallel()

	server := NewRestoreServiceServer(t.TempDir(), "test-cluster", 1, noopLogger{})
	store, err := dal.OpenDirect(server.stagingDir(), noopLogger{})
	require.NoError(t, err)

	server.mu.Lock()
	server.stagingStore = store
	server.downloaded = true
	server.mu.Unlock()
	require.NoError(t, server.beginRequest())

	server.BeginShutdown()
	shutdownDone := make(chan struct{})
	go func() {
		server.Shutdown()
		close(shutdownDone)
	}()

	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned while an admitted restore request was active")
	default:
	}

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err, "staging store must remain valid while the request unwinds")
	require.NoError(t, handle.Close())

	server.endRequest()
	select {
	case <-shutdownDone:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not finish after the request drained")
	}

	server.mu.Lock()
	retained := server.stagingStore
	server.mu.Unlock()
	require.Nil(t, retained)

	_, err = store.NewDirectReadHandle()
	require.Error(t, err, "shutdown must close the retained staging store")

	// Repeated shutdown is a no-op: it must not double-close or panic.
	server.BeginShutdown()
	server.Shutdown()
}

func TestRestoreShutdownWithoutActiveWorkClosesStagingStore(t *testing.T) {
	t.Parallel()

	server := NewRestoreServiceServer(t.TempDir(), "test-cluster", 1, noopLogger{})
	store, err := dal.OpenDirect(server.stagingDir(), noopLogger{})
	require.NoError(t, err)

	server.mu.Lock()
	server.stagingStore = store
	server.downloaded = true
	server.mu.Unlock()

	server.Shutdown()

	server.mu.Lock()
	retained := server.stagingStore
	server.mu.Unlock()
	require.Nil(t, retained)

	_, err = store.NewDirectReadHandle()
	require.Error(t, err)
}

func TestRestoreDownloadRemainsAsynchronousAndSucceeds(t *testing.T) {
	t.Parallel()

	manifest, objects := restoreCheckpointFixture(t)
	manifestData, err := json.Marshal(manifest)
	require.NoError(t, err)

	ctrl := gomock.NewController(t)
	storage := NewMockStorage(ctrl)
	manifestRequested := make(chan struct{})
	releaseManifest := make(chan struct{})
	var requested sync.Once

	storage.EXPECT().
		GetFile(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(_ context.Context, key string) (io.ReadCloser, error) {
			if strings.HasSuffix(key, "/backups/manifest.json") {
				requested.Do(func() { close(manifestRequested) })
				<-releaseManifest

				return io.NopCloser(bytes.NewReader(manifestData)), nil
			}

			data, ok := objects[key]
			if !ok {
				return nil, fmt.Errorf("unexpected storage key %q", key)
			}

			return io.NopCloser(bytes.NewReader(data)), nil
		})

	server := newServerForTest(t, staticFactory(storage))
	t.Cleanup(server.Shutdown)
	started, err := server.StartDownloadBackup(context.Background(), &restorepb.StartDownloadBackupRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, started.GetJobId())
	<-manifestRequested

	server.mu.Lock()
	job := server.job
	server.mu.Unlock()
	select {
	case <-job.done:
		t.Fatal("StartDownloadBackup did not leave the transfer asynchronous")
	default:
	}

	close(releaseManifest)
	select {
	case <-job.done:
	case <-time.After(10 * time.Second):
		t.Fatal("successful download did not terminate")
	}

	server.mu.Lock()
	state := job.state
	store := server.stagingStore
	downloaded := server.downloaded
	server.mu.Unlock()
	require.Equal(t, restorepb.DownloadState_DOWNLOAD_STATE_SUCCEEDED, state)
	require.True(t, downloaded)
	require.NotNil(t, store)

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	require.NoError(t, handle.Close())

	server.Shutdown()
	_, err = store.NewDirectReadHandle()
	require.Error(t, err)
}

func TestCancelDownloadAndShutdownCanCompete(t *testing.T) {
	t.Parallel()

	server, job, reader := startBlockedRestoreDownload(t)
	cancelDone := make(chan error, 1)
	go func() {
		_, err := server.CancelDownload(context.Background(), &restorepb.CancelDownloadRequest{JobId: job.id})
		cancelDone <- err
	}()
	<-reader.canceled

	server.BeginShutdown()
	shutdownDone := make(chan struct{})
	go func() {
		server.Shutdown()
		close(shutdownDone)
	}()

	select {
	case err := <-cancelDone:
		require.NoError(t, err)
		t.Fatal("CancelDownload returned before the job unwound")
	default:
	}
	select {
	case <-shutdownDone:
		t.Fatal("shutdown returned before competing cancellation unwound")
	default:
	}

	reader.allowUnwind()
	select {
	case err := <-cancelDone:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("CancelDownload did not return")
	}
	select {
	case <-shutdownDone:
	case <-time.After(10 * time.Second):
		t.Fatal("shutdown did not return")
	}

	server.BeginShutdown()
	server.Shutdown()
}

func restoreCheckpointFixture(t *testing.T) (*backup.Manifest, map[string][]byte) {
	t.Helper()

	sourceDir := filepath.Join(t.TempDir(), "source")
	store, err := dal.OpenDirect(sourceDir, noopLogger{})
	require.NoError(t, err)
	require.NoError(t, store.Close())

	files := map[string]backup.CheckpointFile{}
	objects := map[string][]byte{}
	err = filepath.WalkDir(sourceDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if !entry.Type().IsRegular() {
			return nil
		}

		rel, relErr := filepath.Rel(sourceDir, path)
		if relErr != nil {
			return relErr
		}
		name := filepath.ToSlash(rel)
		key := "fixture/" + name
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}

		files[name] = backup.CheckpointFile{Size: int64(len(data)), Key: key}
		objects[key] = data

		return nil
	})
	require.NoError(t, err)
	require.NotEmpty(t, files)

	return &backup.Manifest{Checkpoint: &backup.CheckpointManifest{Files: files}}, objects
}
