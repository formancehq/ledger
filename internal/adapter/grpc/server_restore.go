package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/application/check"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/backup"
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/proto/restorepb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

const restoreStagingDir = "restore-staging"

// RestoreServiceServerImpl implements the RestoreService gRPC server.
type RestoreServiceServerImpl struct {
	restorepb.UnimplementedRestoreServiceServer

	mu          sync.Mutex
	dataDir     string
	clusterID   string
	logger      logging.Logger
	downloading bool
	downloaded  bool
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
	storage, err := backup.NewStorage("s3", "", req.GetS3Bucket(), req.GetS3Region(), req.GetS3Endpoint())
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

	manifestData, err := io.ReadAll(manifestReader)
	_ = manifestReader.Close()

	if err != nil {
		return nil, fmt.Errorf("reading manifest data: %w", err)
	}

	var manifest backup.Manifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		return nil, fmt.Errorf("parsing manifest: %w", err)
	}

	if len(manifest.Files) == 0 {
		return nil, status.Error(codes.FailedPrecondition, "backup manifest contains no files")
	}

	// Prepare staging directory
	stagingDir := s.stagingDir()

	if err := os.RemoveAll(stagingDir); err != nil {
		return nil, fmt.Errorf("cleaning staging directory: %w", err)
	}

	if err := os.MkdirAll(stagingDir, 0o755); err != nil {
		return nil, fmt.Errorf("creating staging directory: %w", err)
	}

	// Download each file from S3 into staging
	var totalBytes uint64

	for filename := range manifest.Files {
		key := fileKeyPrefix + filename
		destPath := filepath.Join(stagingDir, filepath.FromSlash(filename))

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

	success = true

	s.logger.WithFields(map[string]any{
		"filesDownloaded": len(manifest.Files),
		"totalBytes":      totalBytes,
	}).Infof("Backup downloaded from S3 successfully")

	return &restorepb.DownloadBackupResponse{
		FilesDownloaded: uint32(len(manifest.Files)),
		TotalBytes:      totalBytes,
	}, nil
}

// ValidateRestore runs integrity checks on the staged backup data.
func (s *RestoreServiceServerImpl) ValidateRestore(_ *restorepb.ValidateRestoreRequest, stream ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error {
	s.mu.Lock()
	downloaded := s.downloaded
	s.mu.Unlock()

	if !downloaded {
		return status.Error(codes.FailedPrecondition, "no backup downloaded; download a backup first")
	}

	stagingDir := s.stagingDir()

	store, err := dal.OpenReadOnly(stagingDir, s.logger)
	if err != nil {
		return fmt.Errorf("opening staging store: %w", err)
	}

	defer func() { _ = store.Close() }()

	attrs := attributes.New()
	checker := check.NewChecker(store, attrs)

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
	s.mu.Unlock()

	if !downloaded {
		return nil, status.Error(codes.FailedPrecondition, "no backup downloaded; download a backup first")
	}

	stagingDir := s.stagingDir()

	store, err := dal.OpenReadOnly(stagingDir, s.logger)
	if err != nil {
		return nil, fmt.Errorf("opening staging store: %w", err)
	}

	defer func() { _ = store.Close() }()

	lastAppliedIndex, err := query.ReadLastAppliedIndex(store)
	if err != nil {
		return nil, fmt.Errorf("getting last applied index: %w", err)
	}

	lastAppliedTimestamp, err := query.ReadLastAppliedTimestamp(store)
	if err != nil {
		return nil, fmt.Errorf("getting last applied timestamp: %w", err)
	}

	lastSequence, err := query.ReadLastSequence(store)
	if err != nil {
		return nil, fmt.Errorf("getting last sequence: %w", err)
	}

	cursor, err := query.ReadLedgers(ctx, store)
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
	s.mu.Unlock()

	if !downloaded {
		return nil, status.Error(codes.FailedPrecondition, "no backup downloaded; download a backup first")
	}

	stagingDir := s.stagingDir()

	// Compact attributes to index 0 and reset lastAppliedIndex.
	s.logger.Infof("Compacting backup for restore compatibility")

	compactStore, err := dal.OpenDirect(stagingDir, s.logger)
	if err != nil {
		return nil, fmt.Errorf("opening staging for compaction: %w", err)
	}

	if err := attributes.CompactAllForBackup(compactStore); err != nil {
		_ = compactStore.Close()

		return nil, fmt.Errorf("compacting backup attributes: %w", err)
	}

	if err := compactStore.Close(); err != nil {
		return nil, fmt.Errorf("closing compacted staging: %w", err)
	}

	// Read metadata after compaction
	store, err := dal.OpenReadOnly(stagingDir, s.logger)
	if err != nil {
		return nil, fmt.Errorf("opening staging store: %w", err)
	}

	lastAppliedIndex, err := query.ReadLastAppliedIndex(store)
	if err != nil {
		_ = store.Close()

		return nil, fmt.Errorf("getting last applied index: %w", err)
	}

	lastAppliedTimestamp, err := query.ReadLastAppliedTimestamp(store)
	if err != nil {
		_ = store.Close()

		return nil, fmt.Errorf("getting last applied timestamp: %w", err)
	}

	if err := store.Close(); err != nil {
		return nil, fmt.Errorf("closing staging store: %w", err)
	}

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

	// Write CURRENT_CHECKPOINT
	cpFile := filepath.Join(s.dataDir, "CURRENT_CHECKPOINT")
	if err := os.WriteFile(cpFile, []byte("0"), 0o644); err != nil {
		return nil, fmt.Errorf("writing CURRENT_CHECKPOINT: %w", err)
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

// RegisterRestoreService registers the RestoreService on a gRPC server.
func RegisterRestoreService(server *ggrpc.Server, impl restorepb.RestoreServiceServer) {
	restorepb.RegisterRestoreServiceServer(server, impl)
}
