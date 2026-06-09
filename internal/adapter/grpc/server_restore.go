package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/application/check"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const (
	restoreStagingDir = "restore-staging"

	// maxRestoreManifestBytes caps how much of the manifest we read from
	// S3 into memory. A real Pebble backup manifest is JSON describing
	// one entry per file; even a 1 TB / 16 k SST backup is well under
	// a megabyte of JSON. 32 MiB keeps a huge margin without risking
	// OOM on a malicious or runaway upload.
	maxRestoreManifestBytes = 32 * 1024 * 1024

	// maxRestoreManifestFiles caps the number of file entries inside the
	// manifest. 100 k is generous (a 6 TB backup at 64 MiB/SST is ~100 k
	// files); larger uploads should be flagged at backup-time, not blindly
	// trusted in restore mode.
	maxRestoreManifestFiles = 100_000
)

// safeStagingPath joins filename onto stagingDir while refusing any input
// that would escape the staging directory. It rejects absolute paths,
// parent-directory traversals, and (defense in depth) any path that —
// after canonicalisation — resolves outside the staging root.
//
// The filename comes from a Pebble backup manifest fetched from S3.
// The manifest is signed by nothing today, so a malicious or buggy
// uploader could attempt to overwrite arbitrary files. This function
// is the only path validation in the restore download loop; it must
// reject any input that does not land inside stagingDir.
func safeStagingPath(stagingDir, filename string) (string, error) {
	if filename == "" {
		return "", errors.New("manifest entry has empty filename")
	}

	// Normalise slashes first so the OS-specific checks below are stable
	// regardless of whether the manifest used forward or backslashes.
	osName := filepath.FromSlash(filename)

	if filepath.IsAbs(osName) {
		return "", fmt.Errorf("manifest entry %q is an absolute path", filename)
	}

	cleaned := filepath.Clean(osName)

	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("manifest entry %q escapes the staging directory", filename)
	}

	dest := filepath.Join(stagingDir, cleaned)

	// Defense in depth: even after the prefix checks above, verify that
	// `dest` is actually under `stagingDir` once everything is resolved.
	// filepath.Rel returns an error or a path starting with ".." if dest
	// is not a descendant.
	rel, err := filepath.Rel(stagingDir, dest)
	if err != nil {
		return "", fmt.Errorf("computing relative path for %q: %w", filename, err)
	}

	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("manifest entry %q resolves outside the staging directory", filename)
	}

	return dest, nil
}

// RestoreServiceServerImpl implements the RestoreService gRPC server.
type RestoreServiceServerImpl struct {
	restorepb.UnimplementedRestoreServiceServer

	mu          sync.Mutex
	dataDir     string
	clusterID   string
	logger      logging.Logger
	downloading bool
	downloaded  bool

	// stagingStore is the read-write Pebble handle on the staging directory,
	// opened by DownloadBackup once the download is complete and the export
	// segments have been applied, and kept alive for the rest of the restore
	// lifecycle. ValidateRestore, PreviewRestore, and FinalizeRestore all
	// reuse it instead of re-opening the staging Pebble in the same process
	// — Pebble v2 forbids that (vfs/file_lock_unix.go: "lock held by current
	// process"), and even if it did not, repeatedly warming up the table
	// metadata for thousands of SSTs on a 1+ TB staging directory would be
	// gratuitously slow.
	stagingStore *dal.Store
}

// NewRestoreServiceServer creates a new RestoreServiceServerImpl.
func NewRestoreServiceServer(dataDir, clusterID string, logger logging.Logger) *RestoreServiceServerImpl {
	return &RestoreServiceServerImpl{
		dataDir:   dataDir,
		clusterID: clusterID,
		logger:    logger,
	}
}

func (s *RestoreServiceServerImpl) stagingDir() string {
	return filepath.Join(s.dataDir, restoreStagingDir)
}

// closeStagingStore closes the held staging handle, if any, and nils it out.
// Safe to call multiple times. Caller must hold s.mu.
func (s *RestoreServiceServerImpl) closeStagingStore() {
	if s.stagingStore == nil {
		return
	}

	if err := s.stagingStore.Close(); err != nil {
		s.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to close staging store")
	}

	s.stagingStore = nil
}

// DownloadBackup downloads backup files from S3 into the restore staging area.
func (s *RestoreServiceServerImpl) DownloadBackup(ctx context.Context, req *restorepb.DownloadBackupRequest) (*restorepb.DownloadBackupResponse, error) {
	s.mu.Lock()
	if s.downloaded {
		s.mu.Unlock()

		return nil, status.Error(codes.FailedPrecondition, "backup already downloaded; finalize or restart to download again")
	}

	if s.downloading {
		s.mu.Unlock()

		return nil, status.Error(codes.FailedPrecondition, "another download is already in progress")
	}

	s.downloading = true
	s.mu.Unlock()

	success := false

	defer func() {
		s.mu.Lock()
		if success {
			s.downloaded = true
		}
		s.downloading = false
		s.mu.Unlock()
	}()

	// Create S3 storage
	storage, err := backup.NewStorage("s3", "", req.GetS3Bucket(), req.GetS3Region(), req.GetS3Endpoint(), req.GetS3AccessKeyId(), req.GetS3SecretAccessKey())
	if err != nil {
		return nil, fmt.Errorf("creating S3 storage: %w", err)
	}

	bucketID := req.GetBucketId()
	if bucketID == "" {
		bucketID = s.clusterID
	}

	manifestKey := bucketID + "/backups/manifest.json"
	fileKeyPrefix := bucketID + "/backups/data/"

	// Read manifest
	manifestReader, err := storage.GetFile(ctx, manifestKey)
	if err != nil {
		return nil, fmt.Errorf("reading backup manifest: %w", err)
	}

	// Read the manifest behind a hard byte cap. A real manifest is well
	// under a megabyte; anything larger is either a misconfiguration on
	// the backup side or an attempt to exhaust the restore node's memory.
	// LimitReader yields an EOF at the cap+1 byte; we detect overshoot by
	// reading one extra byte and refusing if it is present.
	manifestData, err := io.ReadAll(io.LimitReader(manifestReader, maxRestoreManifestBytes+1))
	_ = manifestReader.Close()

	if err != nil {
		return nil, fmt.Errorf("reading manifest data: %w", err)
	}

	if int64(len(manifestData)) > maxRestoreManifestBytes {
		return nil, status.Errorf(codes.FailedPrecondition,
			"backup manifest exceeds %d bytes; refusing to read", maxRestoreManifestBytes)
	}

	var manifest backup.Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return nil, fmt.Errorf("parsing manifest: %w", err)
	}

	hasCheckpoint := manifest.Checkpoint != nil && len(manifest.Checkpoint.Files) > 0
	if !hasCheckpoint && len(manifest.Exports) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "backup manifest contains no checkpoint files and no export segments")
	}

	if hasCheckpoint && len(manifest.Checkpoint.Files) > maxRestoreManifestFiles {
		return nil, status.Errorf(codes.FailedPrecondition,
			"backup manifest declares %d files (max %d); refusing to download",
			len(manifest.Checkpoint.Files), maxRestoreManifestFiles)
	}

	// Prepare staging directory
	stagingDir := s.stagingDir()

	if err := os.RemoveAll(stagingDir); err != nil {
		return nil, fmt.Errorf("cleaning staging directory: %w", err)
	}

	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating staging directory: %w", err)
	}

	// Download each checkpoint file from S3 into staging.
	var (
		totalBytes      uint64
		checkpointFiles int
	)

	if hasCheckpoint {
		checkpointFiles = len(manifest.Checkpoint.Files)

		for filename := range manifest.Checkpoint.Files {
			destPath, err := safeStagingPath(stagingDir, filename)
			if err != nil {
				return nil, status.Errorf(codes.InvalidArgument, "invalid manifest entry: %v", err)
			}

			key := fileKeyPrefix + filename

			if err := os.MkdirAll(filepath.Dir(destPath), 0o755); err != nil {
				return nil, fmt.Errorf("creating directory for %s: %w", filename, err)
			}

			reader, err := storage.GetFile(ctx, key)
			if err != nil {
				return nil, fmt.Errorf("downloading %s: %w", filename, err)
			}

			outFile, err := os.Create(destPath)
			if err != nil {
				_ = reader.Close()

				return nil, fmt.Errorf("creating file %s: %w", filename, err)
			}

			n, err := io.Copy(outFile, reader)
			_ = reader.Close()
			_ = outFile.Close()

			if err != nil {
				return nil, fmt.Errorf("writing file %s: %w", filename, err)
			}

			totalBytes += uint64(n)
		}
	}

	// Open the staging Pebble in read-write mode and keep the handle alive for
	// the remainder of the restore lifecycle (Validate, Preview, Finalize all
	// reuse it). ApplyExportsAndRebuild operates on this same handle so the
	// post-download flow never has to close and re-open in the same process.
	stagingStore, err := dal.OpenDirect(stagingDir, s.logger)
	if err != nil {
		return nil, fmt.Errorf("opening staging store: %w", err)
	}

	defer func() {
		if !success {
			if closeErr := stagingStore.Close(); closeErr != nil {
				s.logger.WithFields(map[string]any{"error": closeErr}).
					Errorf("Failed to close staging store after failed download")
			}
		}
	}()

	// Apply incremental export segments on top of the checkpoint and rebuild
	// derived state. Skipping this would silently drop every log/audit entry
	// written after the last full checkpoint. Mirrors the offline bootstrap.
	if err := backup.ApplyExportsAndRebuild(ctx, s.logger, storage, stagingStore, &manifest); err != nil {
		return nil, fmt.Errorf("applying export segments: %w", err)
	}

	s.mu.Lock()
	s.stagingStore = stagingStore
	s.mu.Unlock()

	success = true

	s.logger.WithFields(map[string]any{
		"filesDownloaded": checkpointFiles,
		"exportSegments":  len(manifest.Exports),
		"totalBytes":      totalBytes,
	}).Infof("Backup downloaded from S3 successfully")

	return &restorepb.DownloadBackupResponse{
		FilesDownloaded: uint32(checkpointFiles),
		TotalBytes:      totalBytes,
	}, nil
}

// ValidateRestore runs integrity checks on the staged backup data.
func (s *RestoreServiceServerImpl) ValidateRestore(_ *restorepb.ValidateRestoreRequest, stream ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error {
	s.mu.Lock()
	downloaded := s.downloaded
	store := s.stagingStore
	s.mu.Unlock()

	if !downloaded || store == nil {
		return status.Error(codes.FailedPrecondition, "no backup downloaded; download a backup first")
	}

	attrs := attributes.New()
	checker := check.NewChecker(store, attrs, s.logger)

	return checker.Check(stream.Context(), func(event *servicepb.CheckStoreEvent) {
		var restoreEvent restorepb.ValidateRestoreEvent

		switch t := event.GetType().(type) {
		case *servicepb.CheckStoreEvent_Progress:
			restoreEvent.Type = &restorepb.ValidateRestoreEvent_Progress{
				Progress: &restorepb.ValidateRestoreProgress{
					LogsChecked: t.Progress.GetLogsChecked(),
					TotalLogs:   t.Progress.GetTotalLogs(),
				},
			}
		case *servicepb.CheckStoreEvent_Error:
			restoreEvent.Type = &restorepb.ValidateRestoreEvent_Error{
				Error: &restorepb.ValidateRestoreError{
					Message: t.Error.GetMessage(),
				},
			}
		}

		err := stream.Send(&restoreEvent)
		if err != nil {
			s.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to send validate event")
		}
	})
}

// PreviewRestore returns a summary of the staged backup data.
func (s *RestoreServiceServerImpl) PreviewRestore(ctx context.Context, _ *restorepb.PreviewRestoreRequest) (*restorepb.PreviewRestoreResponse, error) {
	s.mu.Lock()
	downloaded := s.downloaded
	store := s.stagingStore
	s.mu.Unlock()

	if !downloaded || store == nil {
		return nil, status.Error(codes.FailedPrecondition, "no backup downloaded; download a backup first")
	}

	lastAppliedIndex, err := query.ReadLastAppliedIndex(store)
	if err != nil {
		return nil, fmt.Errorf("getting last applied index: %w", err)
	}

	lastAppliedTimestamp, err := query.ReadLastAppliedTimestamp(store)
	if err != nil {
		return nil, fmt.Errorf("getting last applied timestamp: %w", err)
	}

	readHandle, handleErr := store.NewDirectReadHandle()
	if handleErr != nil {
		return nil, fmt.Errorf("creating read handle: %w", handleErr)
	}
	defer func() { _ = readHandle.Close() }()

	lastSequence, err := query.ReadLastSequence(readHandle)
	if err != nil {
		return nil, fmt.Errorf("getting last sequence: %w", err)
	}

	cursor, err := query.ReadLedgers(ctx, readHandle)
	if err != nil {
		return nil, fmt.Errorf("listing ledgers: %w", err)
	}

	defer func() { _ = cursor.Close() }()

	var ledgerNames []string

	for {
		ledger, err := cursor.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("iterating ledgers: %w", err)
		}

		ledgerNames = append(ledgerNames, ledger.GetName())
	}

	return &restorepb.PreviewRestoreResponse{
		LastAppliedIndex:     lastAppliedIndex,
		LastAppliedTimestamp: lastAppliedTimestamp,
		LastSequence:         lastSequence,
		LedgerCount:          uint32(len(ledgerNames)),
		LedgerNames:          ledgerNames,
	}, nil
}

// FinalizeRestore compacts attributes and commits the staged backup as the live data.
func (s *RestoreServiceServerImpl) FinalizeRestore(_ context.Context, _ *restorepb.FinalizeRestoreRequest) (*restorepb.FinalizeRestoreResponse, error) {
	s.mu.Lock()
	downloaded := s.downloaded
	store := s.stagingStore
	s.mu.Unlock()

	if !downloaded || store == nil {
		return nil, status.Error(codes.FailedPrecondition, "no backup downloaded; download a backup first")
	}

	stagingDir := s.stagingDir()

	// Compact attributes to index 0 and reset lastAppliedIndex.
	s.logger.Infof("Compacting backup for restore compatibility")

	if err := attributes.CompactAllForBackup(store); err != nil {
		return nil, fmt.Errorf("compacting backup attributes: %w", err)
	}

	// Read metadata after compaction. We can do this directly on the same RW
	// handle — query.ReadLast* only need a PebbleGetter, which *dal.Store
	// satisfies in both RW and RO modes.
	lastAppliedIndex, err := query.ReadLastAppliedIndex(store)
	if err != nil {
		return nil, fmt.Errorf("getting last applied index: %w", err)
	}

	lastAppliedTimestamp, err := query.ReadLastAppliedTimestamp(store)
	if err != nil {
		return nil, fmt.Errorf("getting last applied timestamp: %w", err)
	}

	// Close the staging Pebble before the filesystem move. The hard-link
	// below would otherwise race with open FDs (SSTs, LOCK file) held by the
	// running DB. After Close, the staging dir is just a tree of files we
	// own.
	s.mu.Lock()
	s.closeStagingStore()
	s.mu.Unlock()

	// Write RESTORED marker
	marker := node.RestoredMarker{
		LastAppliedIndex:     lastAppliedIndex,
		LastAppliedTimestamp: lastAppliedTimestamp,
	}

	markerData, err := json.Marshal(marker)
	if err != nil {
		return nil, fmt.Errorf("marshaling restored marker: %w", err)
	}

	markerPath := filepath.Join(s.dataDir, "RESTORED")
	if err := os.WriteFile(markerPath, markerData, 0o644); err != nil {
		return nil, fmt.Errorf("writing restored marker: %w", err)
	}

	// Move staging to checkpoint 0
	checkpointsDir := filepath.Join(s.dataDir, "checkpoints")
	if err := os.MkdirAll(checkpointsDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating checkpoints directory: %w", err)
	}

	checkpointPath := filepath.Join(checkpointsDir, "0")
	if err := os.RemoveAll(checkpointPath); err != nil {
		return nil, fmt.Errorf("removing existing checkpoint 0: %w", err)
	}

	if err := dal.HardLink(stagingDir, checkpointPath); err != nil {
		return nil, fmt.Errorf("hard linking staging to checkpoint: %w", err)
	}

	// Remove staging directory
	if err := os.RemoveAll(stagingDir); err != nil {
		s.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to remove staging directory")
	}

	s.logger.WithFields(map[string]any{
		"lastAppliedIndex":     lastAppliedIndex,
		"lastAppliedTimestamp": lastAppliedTimestamp,
	}).Infof("Restore finalized successfully. Restart the server without --restore to use restored data.")

	return &restorepb.FinalizeRestoreResponse{
		Message: fmt.Sprintf("Restore finalized (index=%d). Restart the server without --restore to use restored data.", lastAppliedIndex),
	}, nil
}

// RegisterRestoreService registers the RestoreService on a gRPC service registrar.
func RegisterRestoreService(registrar ggrpc.ServiceRegistrar, impl restorepb.RestoreServiceServer) {
	restorepb.RegisterRestoreServiceServer(registrar, impl)
}
