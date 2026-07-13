package grpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// storageFactory builds the backup.Storage used by a download job. It is a
// field on the server so unit tests can substitute an in-memory fake without
// touching live S3.
type storageFactory func(req *restorepb.StartDownloadBackupRequest) (backup.Storage, error)

// defaultStorageFactory builds the production storage backend selected by the
// request's storage provider oneof (s3 or azure).
func defaultStorageFactory(req *restorepb.StartDownloadBackupRequest) (backup.Storage, error) {
	cfg, err := storageConfigFromProto(req.GetStorage())
	if err != nil {
		return nil, err
	}

	return backup.NewStorage(cfg)
}

// downloadJob tracks the lifecycle of a single async download started by
// StartDownloadBackup. Counters are atomic so worker goroutines can update
// them without contending with status pollers; the remaining fields are
// guarded by the parent server's mu.
type downloadJob struct {
	id     string
	cancel context.CancelFunc
	done   chan struct{} // closed when the job goroutine returns

	filesDownloaded atomic.Uint64
	bytesDownloaded atomic.Uint64
	totalFiles      atomic.Uint64
	totalBytes      atomic.Uint64
	currentFile     atomic.Pointer[string]

	// Fields below are protected by RestoreServiceServerImpl.mu.
	state      restorepb.DownloadState
	errMessage string
	startedAt  int64 // Unix seconds
	finishedAt int64 // 0 while RUNNING / PENDING
}

// errCanceled is the sentinel the job goroutine uses to distinguish a
// user-initiated cancel from an unexpected error returned by errgroup.
var errCanceled = errors.New("download canceled")

// StartDownloadBackup kicks off an asynchronous download from the configured
// backup backend and returns immediately with the job ID. The actual transfer
// happens on a goroutine detached from the calling RPC context so it survives
// any ingress / load balancer timeout. See issue #349.
func (s *RestoreServiceServerImpl) StartDownloadBackup(_ context.Context, req *restorepb.StartDownloadBackupRequest) (*restorepb.StartDownloadBackupResponse, error) {
	s.mu.Lock()

	if s.downloaded {
		s.mu.Unlock()

		return nil, status.Error(codes.FailedPrecondition, "backup already downloaded; finalize or restart to download again")
	}

	if s.downloading {
		s.mu.Unlock()

		return nil, status.Error(codes.FailedPrecondition, "another download is already in progress")
	}

	jobCtx, cancel := context.WithCancel(context.Background())

	job := &downloadJob{
		id:        uuid.NewString(),
		cancel:    cancel,
		done:      make(chan struct{}),
		state:     restorepb.DownloadState_DOWNLOAD_STATE_PENDING,
		startedAt: time.Now().Unix(),
	}

	s.downloading = true
	s.job = job
	s.mu.Unlock()

	factory := s.storageFactory
	if factory == nil {
		factory = defaultStorageFactory
	}

	go s.runDownloadJob(jobCtx, job, req, factory)

	return &restorepb.StartDownloadBackupResponse{JobId: job.id}, nil
}

// GetDownloadStatus reports the current state of the job created by
// StartDownloadBackup. Once the job reaches a terminal state (SUCCEEDED,
// FAILED, CANCELED) the response stays stable.
func (s *RestoreServiceServerImpl) GetDownloadStatus(_ context.Context, req *restorepb.GetDownloadStatusRequest) (*restorepb.GetDownloadStatusResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	job := s.job
	if job == nil || job.id != req.GetJobId() {
		return nil, status.Error(codes.NotFound, "unknown job id")
	}

	var currentFile string
	if p := job.currentFile.Load(); p != nil {
		currentFile = *p
	}

	return &restorepb.GetDownloadStatusResponse{
		State:           job.state,
		FilesDownloaded: job.filesDownloaded.Load(),
		TotalFiles:      job.totalFiles.Load(),
		BytesDownloaded: job.bytesDownloaded.Load(),
		TotalBytes:      job.totalBytes.Load(),
		CurrentFile:     currentFile,
		ErrorMessage:    job.errMessage,
		StartedAtUnix:   uint64(job.startedAt),
		FinishedAtUnix:  uint64(job.finishedAt),
	}, nil
}

// CancelDownload aborts a running download. Idempotent: calling it on a
// finished or already-cancelled job is a no-op. After a cancel, the staging
// directory is wiped so the operator can immediately retry without having to
// restart the server.
func (s *RestoreServiceServerImpl) CancelDownload(_ context.Context, req *restorepb.CancelDownloadRequest) (*restorepb.CancelDownloadResponse, error) {
	s.mu.Lock()

	job := s.job
	if job == nil || job.id != req.GetJobId() {
		s.mu.Unlock()

		return nil, status.Error(codes.NotFound, "unknown job id")
	}

	switch job.state {
	case restorepb.DownloadState_DOWNLOAD_STATE_SUCCEEDED,
		restorepb.DownloadState_DOWNLOAD_STATE_FAILED,
		restorepb.DownloadState_DOWNLOAD_STATE_CANCELED:
		s.mu.Unlock()

		return &restorepb.CancelDownloadResponse{}, nil
	}

	s.mu.Unlock()

	job.cancel()

	// Wait briefly for the goroutine to drain so callers see a CANCELED status
	// on their next poll. We cap the wait so a stuck storage client does not block
	// the RPC indefinitely; in that case the client will observe CANCELED on a
	// later poll instead.
	select {
	case <-job.done:
	case <-time.After(5 * time.Second):
	}

	return &restorepb.CancelDownloadResponse{}, nil
}

// runDownloadJob executes the long-running download phase. It owns the job's
// lifecycle from PENDING → RUNNING → {SUCCEEDED, FAILED, CANCELED} and the
// staging directory's contents.
func (s *RestoreServiceServerImpl) runDownloadJob(
	ctx context.Context,
	job *downloadJob,
	req *restorepb.StartDownloadBackupRequest,
	factory storageFactory,
) {
	defer close(job.done)

	storage, manifest, err := s.prepareDownload(ctx, req, factory)
	if err != nil {
		s.finishJob(job, err, nil)

		return
	}

	stagingStore, err := s.executeDownload(ctx, job, storage, manifest)
	s.finishJob(job, err, stagingStore)
}

// prepareDownload performs all blocking I/O that must succeed before workers
// start: build the storage client, fetch the manifest, validate it, and
// recreate the staging directory. Returns the storage and the parsed manifest;
// checkpoint files are downloaded by their content-addressed keys recorded in
// the manifest, so no prefix needs to be threaded through.
func (s *RestoreServiceServerImpl) prepareDownload(
	ctx context.Context,
	req *restorepb.StartDownloadBackupRequest,
	factory storageFactory,
) (backup.Storage, *backup.Manifest, error) {
	storage, err := factory(req)
	if err != nil {
		return nil, nil, fmt.Errorf("creating backup storage: %w", err)
	}

	bucketID := req.GetBucketId()
	if bucketID == "" {
		bucketID = s.clusterID
	}

	manifestKey := bucketID + "/backups/manifest.json"

	manifestReader, err := storage.GetFile(ctx, manifestKey)
	if err != nil {
		return nil, nil, fmt.Errorf("reading backup manifest: %w", err)
	}

	manifestData, err := io.ReadAll(io.LimitReader(manifestReader, maxRestoreManifestBytes+1))
	_ = manifestReader.Close()

	if err != nil {
		return nil, nil, fmt.Errorf("reading manifest data: %w", err)
	}

	if int64(len(manifestData)) > maxRestoreManifestBytes {
		return nil, nil, status.Errorf(codes.FailedPrecondition,
			"backup manifest exceeds %d bytes; refusing to read", maxRestoreManifestBytes)
	}

	manifestPtr, err := backup.DecodeManifest(manifestData)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing manifest: %w", err)
	}

	manifest := *manifestPtr

	hasCheckpoint := manifest.Checkpoint != nil && len(manifest.Checkpoint.Files) > 0
	if !hasCheckpoint && len(manifest.Exports) == 0 {
		return nil, nil, status.Error(codes.FailedPrecondition,
			"backup manifest contains no checkpoint files and no export segments")
	}

	if hasCheckpoint && len(manifest.Checkpoint.Files) > maxRestoreManifestFiles {
		return nil, nil, status.Errorf(codes.FailedPrecondition,
			"backup manifest declares %d files (max %d); refusing to download",
			len(manifest.Checkpoint.Files), maxRestoreManifestFiles)
	}

	stagingDir := s.stagingDir()

	if err := os.RemoveAll(stagingDir); err != nil {
		return nil, nil, fmt.Errorf("cleaning staging directory: %w", err)
	}

	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return nil, nil, fmt.Errorf("creating staging directory: %w", err)
	}

	return storage, &manifest, nil
}

// executeDownload transitions the job to RUNNING, downloads every checkpoint
// file in parallel, then opens the staging store and applies incremental
// exports. The returned store is non-nil only on success.
func (s *RestoreServiceServerImpl) executeDownload(
	ctx context.Context,
	job *downloadJob,
	storage backup.Storage,
	manifest *backup.Manifest,
) (*dal.Store, error) {
	stagingDir := s.stagingDir()

	if manifest.Checkpoint != nil {
		job.totalFiles.Store(uint64(len(manifest.Checkpoint.Files)))

		var totalBytes uint64
		for _, cf := range manifest.Checkpoint.Files {
			if cf.Size > 0 {
				totalBytes += uint64(cf.Size)
			}
		}

		job.totalBytes.Store(totalBytes)
	}

	s.mu.Lock()
	job.state = restorepb.DownloadState_DOWNLOAD_STATE_RUNNING
	s.mu.Unlock()

	if manifest.Checkpoint != nil {
		if err := s.downloadCheckpointFiles(ctx, job, storage, manifest, stagingDir); err != nil {
			return nil, err
		}
	}

	stagingStore, err := dal.OpenDirect(stagingDir, s.logger)
	if err != nil {
		return nil, fmt.Errorf("opening staging store: %w", err)
	}

	if err := backup.ApplyExportsAndRebuild(ctx, s.logger, storage, stagingStore, manifest); err != nil {
		_ = stagingStore.Close()

		return nil, fmt.Errorf("applying export segments: %w", err)
	}

	return stagingStore, nil
}

// downloadCheckpointFiles fans out checkpoint downloads across a worker pool
// sized by s.parallelism. The first error cancels the group; the remaining
// workers exit at their next ctx check.
func (s *RestoreServiceServerImpl) downloadCheckpointFiles(
	ctx context.Context,
	job *downloadJob,
	storage backup.Storage,
	manifest *backup.Manifest,
	stagingDir string,
) error {
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(s.parallelism)

	for filename, cf := range manifest.Checkpoint.Files {
		g.Go(func() error {
			if err := gctx.Err(); err != nil {
				return err
			}

			return s.downloadOneFile(gctx, job, storage, stagingDir, filename, cf.Key)
		})
	}

	if err := g.Wait(); err != nil {
		if errors.Is(err, context.Canceled) {
			return errCanceled
		}

		return err
	}

	return nil
}

// downloadOneFile resolves the destination path, fetches the object, streams
// it to disk, and bumps the job counters. Workers call this concurrently.
func (s *RestoreServiceServerImpl) downloadOneFile(
	ctx context.Context,
	job *downloadJob,
	storage backup.Storage,
	stagingDir string,
	filename string,
	storageKey string,
) error {
	destPath, err := safeStagingPath(stagingDir, filename)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid manifest entry: %v", err)
	}

	if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
		return fmt.Errorf("creating directory for %s: %w", filename, err)
	}

	name := filename
	job.currentFile.Store(&name)

	// storageKey is the content-addressed object key recorded in the manifest;
	// downloads resolve by that key, never by reconstructing prefix+filename,
	// so a restore always fetches the exact bytes the manifest committed.
	reader, err := storage.GetFile(ctx, storageKey)
	if err != nil {
		return fmt.Errorf("downloading %s: %w", filename, err)
	}

	defer func() { _ = reader.Close() }()

	outFile, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("creating file %s: %w", filename, err)
	}

	n, err := io.Copy(outFile, reader)
	_ = outFile.Close()

	if err != nil {
		return fmt.Errorf("writing file %s: %w", filename, err)
	}

	job.filesDownloaded.Add(1)
	job.bytesDownloaded.Add(uint64(n))

	return nil
}

// finishJob applies the terminal state under the lock. On success the staging
// store is published; on failure or cancel the staging directory is wiped so
// a retry starts from a clean slate. downloading is reset either way; downloaded
// is set only on success.
func (s *RestoreServiceServerImpl) finishJob(job *downloadJob, runErr error, stagingStore *dal.Store) {
	finished := time.Now().Unix()

	switch {
	case runErr == nil:
		s.mu.Lock()
		job.state = restorepb.DownloadState_DOWNLOAD_STATE_SUCCEEDED
		job.finishedAt = finished
		s.stagingStore = stagingStore
		s.downloading = false
		s.downloaded = true
		s.mu.Unlock()

		s.logger.WithFields(map[string]any{
			"filesDownloaded": job.filesDownloaded.Load(),
			"totalBytes":      job.bytesDownloaded.Load(),
		}).Infof("Backup downloaded successfully")

	case errors.Is(runErr, errCanceled) || errors.Is(runErr, context.Canceled):
		if stagingStore != nil {
			_ = stagingStore.Close()
		}

		if err := os.RemoveAll(s.stagingDir()); err != nil {
			s.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to clean staging directory after cancel")
		}

		s.mu.Lock()
		job.state = restorepb.DownloadState_DOWNLOAD_STATE_CANCELED
		job.finishedAt = finished
		s.downloading = false
		s.mu.Unlock()

		s.logger.Infof("Backup download canceled")

	default:
		if stagingStore != nil {
			_ = stagingStore.Close()
		}

		if err := os.RemoveAll(s.stagingDir()); err != nil {
			s.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to clean staging directory after failure")
		}

		s.mu.Lock()
		job.state = restorepb.DownloadState_DOWNLOAD_STATE_FAILED
		job.errMessage = runErr.Error()
		job.finishedAt = finished
		s.downloading = false
		s.mu.Unlock()

		s.logger.WithFields(map[string]any{"error": runErr}).Errorf("Backup download failed")
	}
}
