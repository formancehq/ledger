package grpc

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

func TestClampParallelism(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   int
		want int
	}{
		{in: 0, want: defaultDownloadParallelism}, // zero → default
		{in: -5, want: minDownloadParallelism},    // negative → clamp low
		{in: 1, want: 1},
		{in: 64, want: 64},
		{in: 1000, want: maxDownloadParallelism}, // over max → clamp high
	}

	for _, tc := range cases {
		got := clampParallelism(tc.in)
		require.Equal(t, tc.want, got, "in=%d", tc.in)
	}
}

// newServerForTest returns a RestoreServiceServerImpl wired with a test data
// directory and the provided storage factory. parallelism = 4 keeps the worker
// pool small enough to reason about while exercising concurrency.
func newServerForTest(t *testing.T, factory storageFactory) *RestoreServiceServerImpl {
	t.Helper()

	s := NewRestoreServiceServer(t.TempDir(), "test-cluster", 4, noopLogger{})
	s.storageFactory = factory

	return s
}

// staticFactory binds a single MockStorage instance to the server's storage
// factory hook so every Start call receives the same mock.
func staticFactory(mock backup.Storage) storageFactory {
	return func(_ *restorepb.StartDownloadBackupRequest) (backup.Storage, error) {
		return mock, nil
	}
}

func TestStartDownloadBackup_RejectsConcurrent(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockStorage(ctrl)

	// gotFile signals the worker reached GetFile so the second Start observes
	// the in-progress state regardless of goroutine scheduling.
	gotFile := make(chan struct{})

	mock.EXPECT().
		GetFile(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ string) (io.ReadCloser, error) {
			close(gotFile)
			<-ctx.Done()

			return nil, ctx.Err()
		})

	s := newServerForTest(t, staticFactory(mock))

	startResp, err := s.StartDownloadBackup(context.Background(), &restorepb.StartDownloadBackupRequest{})
	require.NoError(t, err)
	require.NotEmpty(t, startResp.GetJobId())

	<-gotFile

	_, err = s.StartDownloadBackup(context.Background(), &restorepb.StartDownloadBackupRequest{})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "already in progress")

	// Cancel so the background goroutine returns and the mock controller can
	// verify expectations without racing against the deferred t.Cleanup.
	_, err = s.CancelDownload(context.Background(), &restorepb.CancelDownloadRequest{JobId: startResp.GetJobId()})
	require.NoError(t, err)
}

func TestGetDownloadStatus_UnknownJob(t *testing.T) {
	t.Parallel()

	s := newServerForTest(t, staticFactory(NewMockStorage(gomock.NewController(t))))

	_, err := s.GetDownloadStatus(context.Background(), &restorepb.GetDownloadStatusRequest{JobId: "nope"})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestCancelDownload_UnknownJob(t *testing.T) {
	t.Parallel()

	s := newServerForTest(t, staticFactory(NewMockStorage(gomock.NewController(t))))

	_, err := s.CancelDownload(context.Background(), &restorepb.CancelDownloadRequest{JobId: "nope"})
	require.Error(t, err)
	require.Equal(t, codes.NotFound, status.Code(err))
}

func TestCancelDownload_DuringManifestRead(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockStorage(ctrl)

	// Block the manifest read on the job context. Cancelling the job must
	// propagate, the goroutine must unwind, and the final state must be
	// CANCELED. The staging directory must be wiped.
	mock.EXPECT().
		GetFile(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ string) (io.ReadCloser, error) {
			<-ctx.Done()

			return nil, ctx.Err()
		})

	s := newServerForTest(t, staticFactory(mock))

	startResp, err := s.StartDownloadBackup(context.Background(), &restorepb.StartDownloadBackupRequest{})
	require.NoError(t, err)

	// Cancel returns once the job goroutine finishes (or the 5 s timeout
	// inside CancelDownload elapses — which would fail the test below).
	_, err = s.CancelDownload(context.Background(), &restorepb.CancelDownloadRequest{JobId: startResp.GetJobId()})
	require.NoError(t, err)

	statusResp, err := s.GetDownloadStatus(context.Background(), &restorepb.GetDownloadStatusRequest{JobId: startResp.GetJobId()})
	require.NoError(t, err)
	require.Equal(t, restorepb.DownloadState_DOWNLOAD_STATE_CANCELED, statusResp.GetState())
	require.NotZero(t, statusResp.GetFinishedAtUnix())

	// A second cancel is a no-op.
	_, err = s.CancelDownload(context.Background(), &restorepb.CancelDownloadRequest{JobId: startResp.GetJobId()})
	require.NoError(t, err)

	// downloading must be reset so the operator can immediately retry without
	// restarting the server.
	s.mu.Lock()
	downloading := s.downloading
	downloaded := s.downloaded
	s.mu.Unlock()
	require.False(t, downloading, "downloading flag must be reset after cancel")
	require.False(t, downloaded, "downloaded flag must stay false after cancel")
}

func TestCancelDownload_Idempotent_AfterTerminal(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockStorage(ctrl)

	// Manifest read returns invalid JSON; the job goroutine will fail at the
	// parse step and transition to FAILED. Then Cancel must be a no-op.
	mock.EXPECT().
		GetFile(gomock.Any(), gomock.Any()).
		Return(io.NopCloser(strings.NewReader("not-json")), nil)

	s := newServerForTest(t, staticFactory(mock))

	startResp, err := s.StartDownloadBackup(context.Background(), &restorepb.StartDownloadBackupRequest{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		resp, err := s.GetDownloadStatus(context.Background(), &restorepb.GetDownloadStatusRequest{JobId: startResp.GetJobId()})
		require.NoError(t, err)

		return resp.GetState() == restorepb.DownloadState_DOWNLOAD_STATE_FAILED
	}, 5*time.Second, 10*time.Millisecond)

	_, err = s.CancelDownload(context.Background(), &restorepb.CancelDownloadRequest{JobId: startResp.GetJobId()})
	require.NoError(t, err)

	// State must remain FAILED — cancel must not overwrite a terminal state.
	resp, err := s.GetDownloadStatus(context.Background(), &restorepb.GetDownloadStatusRequest{JobId: startResp.GetJobId()})
	require.NoError(t, err)
	require.Equal(t, restorepb.DownloadState_DOWNLOAD_STATE_FAILED, resp.GetState())
	require.NotEmpty(t, resp.GetErrorMessage())
}

// TestDownloadCheckpointFiles_RespectsParallelism verifies the worker pool
// reaches exactly s.parallelism concurrent fetches and never exceeds it.
// Workers rendezvous on a barrier so the test is deterministic regardless of
// scheduler timing: the first P entrants wait until all P have arrived before
// returning. Any additional worker would also block on the barrier, so if the
// errgroup admitted P+1, maxInflight would exceed P.
func TestDownloadCheckpointFiles_RespectsParallelism(t *testing.T) {
	t.Parallel()

	const (
		parallelism = 4
		fileCount   = 32
	)

	ctrl := gomock.NewController(t)
	mock := NewMockStorage(ctrl)

	var (
		inflight, maxInflight atomic.Int32
		barrierFull           = make(chan struct{})
		barrierOnce           sync.Once
	)

	mock.EXPECT().
		GetFile(gomock.Any(), gomock.Any()).
		Times(fileCount).
		DoAndReturn(func(_ context.Context, _ string) (io.ReadCloser, error) {
			cur := inflight.Add(1)
			defer inflight.Add(-1)

			for {
				prev := maxInflight.Load()
				if cur <= prev || maxInflight.CompareAndSwap(prev, cur) {
					break
				}
			}

			// Hold the first parallelism workers until all of them have arrived.
			// Once the barrier opens, every subsequent worker passes through.
			if cur <= parallelism {
				if cur == parallelism {
					barrierOnce.Do(func() { close(barrierFull) })
				}

				<-barrierFull
			}

			return io.NopCloser(strings.NewReader("payload")), nil
		})

	s := NewRestoreServiceServer(t.TempDir(), "test-cluster", parallelism, noopLogger{})

	manifest := &backup.Manifest{Checkpoint: &backup.CheckpointManifest{Files: checkpointFilesFor(fileCount)}}
	job := &downloadJob{}

	err := s.downloadCheckpointFiles(context.Background(), job, mock, manifest, s.stagingDir())
	require.NoError(t, err)
	require.EqualValues(t, fileCount, job.filesDownloaded.Load())
	require.Equal(t, int32(parallelism), maxInflight.Load(), "max concurrent fetches must equal configured parallelism")
}

// TestDownloadCheckpointFiles_ErrorAbortsOthers verifies a single failure
// causes the errgroup to cancel and stop spawning more workers.
func TestDownloadCheckpointFiles_ErrorAbortsOthers(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockStorage(ctrl)

	var calls atomic.Int32

	mock.EXPECT().
		GetFile(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, _ string) (io.ReadCloser, error) {
			calls.Add(1)
			// The first worker fails immediately; subsequent workers either
			// observe ctx.Cancel from the errgroup or never run.
			return nil, errors.New("boom")
		})

	s := NewRestoreServiceServer(t.TempDir(), "test-cluster", 4, noopLogger{})

	manifest := &backup.Manifest{Checkpoint: &backup.CheckpointManifest{Files: checkpointFilesFor(100)}}

	err := s.downloadCheckpointFiles(context.Background(), &downloadJob{}, mock, manifest, s.stagingDir())
	require.Error(t, err)
	require.Contains(t, err.Error(), "boom")
	require.Less(t, int(calls.Load()), 100, "errgroup should short-circuit before every worker runs")
}

// TestDownloadCheckpointFiles_ContextCancel verifies that cancelling the
// parent context aborts all workers and returns the sentinel cancel error.
func TestDownloadCheckpointFiles_ContextCancel(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	mock := NewMockStorage(ctrl)

	// entered closes the first time a worker enters GetFile, giving the test a
	// deterministic signal to fire cancel() without relying on a fixed sleep.
	var (
		entered     = make(chan struct{})
		enteredOnce sync.Once
	)

	mock.EXPECT().
		GetFile(gomock.Any(), gomock.Any()).
		AnyTimes().
		DoAndReturn(func(ctx context.Context, _ string) (io.ReadCloser, error) {
			enteredOnce.Do(func() { close(entered) })
			<-ctx.Done()

			return nil, ctx.Err()
		})

	s := NewRestoreServiceServer(t.TempDir(), "test-cluster", 4, noopLogger{})

	manifest := &backup.Manifest{Checkpoint: &backup.CheckpointManifest{Files: checkpointFilesFor(20)}}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		<-entered
		cancel()
	}()

	err := s.downloadCheckpointFiles(ctx, &downloadJob{}, mock, manifest, s.stagingDir())
	require.ErrorIs(t, err, errCanceled)
}

func TestStartDownloadBackup_RejectsAfterSuccess(t *testing.T) {
	t.Parallel()

	s := newServerForTest(t, staticFactory(NewMockStorage(gomock.NewController(t))))
	// Simulate a prior successful download.
	s.mu.Lock()
	s.downloaded = true
	s.mu.Unlock()

	_, err := s.StartDownloadBackup(context.Background(), &restorepb.StartDownloadBackupRequest{})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "already downloaded")
}

// checkpointFilesFor builds a manifest Files map of n entries, each with a
// content-addressed key derived from its filename (the mock storage ignores the
// key, so the exact hash is irrelevant here).
func checkpointFilesFor(n int) map[string]backup.CheckpointFile {
	files := make(map[string]backup.CheckpointFile, n)
	for i := range n {
		name := uniqueName(i)
		files[name] = backup.CheckpointFile{Size: 7, Key: "prefix/" + name + ".hash"}
	}

	return files
}

// uniqueName produces filenames that survive map iteration order but stay
// short enough for the staging tree.
func uniqueName(i int) string {
	const letters = "abcdefghijklmnop"

	return string(letters[i%len(letters)]) + "/" + string(letters[(i/len(letters))%len(letters)]) + fileNumber(i) + ".sst"
}

func fileNumber(i int) string {
	if i == 0 {
		return "0"
	}

	var digits []byte
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}

	return string(digits)
}
