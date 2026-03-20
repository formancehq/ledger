package grpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/tarutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/restorepb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

const restoreStagingDir = "restore-staging"

// RestoreServiceServerImpl implements the RestoreService gRPC server.
type RestoreServiceServerImpl struct {
	restorepb.UnimplementedRestoreServiceServer

	mu        sync.Mutex
	dataDir   string
	logger    logging.Logger
	uploading bool
	uploaded  bool
}

// NewRestoreServiceServer creates a new RestoreServiceServerImpl.
func NewRestoreServiceServer(dataDir string, logger logging.Logger) *RestoreServiceServerImpl {
	return &RestoreServiceServerImpl{
		dataDir: dataDir,
		logger:  logger,
	}
}

func (s *RestoreServiceServerImpl) stagingDir() string {
	return filepath.Join(s.dataDir, restoreStagingDir)
}

// UploadBackup receives a tar archive via client streaming and extracts it into the staging directory.
func (s *RestoreServiceServerImpl) UploadBackup(stream ggrpc.ClientStreamingServer[restorepb.UploadBackupRequest, restorepb.UploadBackupResponse]) error {
	s.mu.Lock()
	if s.uploaded {
		s.mu.Unlock()

		return status.Error(codes.FailedPrecondition, "backup already uploaded; finalize or restart to upload again")
	}

	if s.uploading {
		s.mu.Unlock()

		return status.Error(codes.FailedPrecondition, "another upload is already in progress")
	}

	s.uploading = true
	s.mu.Unlock()

	// Reset uploading flag on failure (on success, uploaded is set instead)
	success := false

	defer func() {
		if !success {
			s.mu.Lock()
			s.uploading = false
			s.mu.Unlock()
		}
	}()

	stagingDir := s.stagingDir()

	// Clean and create staging directory
	err := os.RemoveAll(stagingDir)
	if err != nil {
		return fmt.Errorf("cleaning staging directory: %w", err)
	}

	err = os.MkdirAll(stagingDir, 0755)
	if err != nil {
		return fmt.Errorf("creating staging directory: %w", err)
	}

	// Set up pipe-based concurrent extraction
	pr, pw := io.Pipe()
	extractErrCh := make(chan error, 1)

	go func() {
		defer func() { _ = pr.Close() }()

		extractErrCh <- tarutil.ExtractTar(pr, stagingDir)
	}()

	var (
		totalReceived uint64
		hash          = sha256.New()
		expectedHash  string
	)

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			_ = pw.CloseWithError(err)

			return fmt.Errorf("receiving upload chunk: %w", err)
		}

		if req.GetEof() {
			expectedHash = req.GetContentSha256()

			break
		}

		if len(req.GetData()) > 0 {
			if _, err := pw.Write(req.GetData()); err != nil {
				return fmt.Errorf("writing to tar pipe: %w", err)
			}

			if _, err := hash.Write(req.GetData()); err != nil {
				return fmt.Errorf("computing hash: %w", err)
			}

			totalReceived += uint64(len(req.GetData()))
		}
	}

	// Close write end to signal EOF to tar reader
	err = pw.Close()
	if err != nil {
		return fmt.Errorf("closing pipe: %w", err)
	}

	// Wait for extraction to complete
	err = <-extractErrCh
	if err != nil {
		return fmt.Errorf("extracting tar: %w", err)
	}

	actualHash := hex.EncodeToString(hash.Sum(nil))

	// Verify hash if provided
	if expectedHash != "" && actualHash != expectedHash {
		// Clean up staging on hash mismatch
		_ = os.RemoveAll(stagingDir)

		return status.Errorf(codes.DataLoss, "SHA256 mismatch: expected %s, got %s", expectedHash, actualHash)
	}

	s.mu.Lock()
	s.uploaded = true
	s.uploading = false
	s.mu.Unlock()

	success = true

	s.logger.WithFields(map[string]any{
		"bytesReceived": totalReceived,
		"sha256":        actualHash,
	}).Infof("Backup uploaded successfully")

	return stream.SendAndClose(&restorepb.UploadBackupResponse{
		BytesReceived: totalReceived,
		Sha256:        actualHash,
	})
}

// ValidateRestore runs integrity checks on the staged backup data.
func (s *RestoreServiceServerImpl) ValidateRestore(_ *restorepb.ValidateRestoreRequest, stream ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error {
	s.mu.Lock()
	uploaded := s.uploaded
	s.mu.Unlock()

	if !uploaded {
		return status.Error(codes.FailedPrecondition, "no backup uploaded; upload a backup first")
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
		// Convert CheckStoreEvent to ValidateRestoreEvent
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
	uploaded := s.uploaded
	s.mu.Unlock()

	if !uploaded {
		return nil, status.Error(codes.FailedPrecondition, "no backup uploaded; upload a backup first")
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

// FinalizeRestore commits the staged backup as the live data.
func (s *RestoreServiceServerImpl) FinalizeRestore(_ context.Context, _ *restorepb.FinalizeRestoreRequest) (*restorepb.FinalizeRestoreResponse, error) {
	s.mu.Lock()
	uploaded := s.uploaded
	s.mu.Unlock()

	if !uploaded {
		return nil, status.Error(codes.FailedPrecondition, "no backup uploaded; upload a backup first")
	}

	stagingDir := s.stagingDir()

	// Open staging to read metadata
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
	if err := os.WriteFile(markerPath, markerData, 0644); err != nil {
		return nil, fmt.Errorf("writing restored marker: %w", err)
	}

	// Extract baseline checkpoint from staging if present.
	baselineSrc := filepath.Join(stagingDir, "_baseline")
	if info, err := os.Stat(baselineSrc); err == nil && info.IsDir() {
		baselineDst := filepath.Join(s.dataDir, "baseline", "checker")
		if err := os.MkdirAll(filepath.Dir(baselineDst), 0755); err != nil {
			return nil, fmt.Errorf("creating baseline directory: %w", err)
		}

		_ = os.RemoveAll(baselineDst)

		if err := os.Rename(baselineSrc, baselineDst); err != nil {
			return nil, fmt.Errorf("moving baseline checkpoint: %w", err)
		}

		s.logger.Infof("Restored baseline checkpoint for integrity verification")
	}

	// Move staging to checkpoint 0
	checkpointsDir := filepath.Join(s.dataDir, "checkpoints")
	if err := os.MkdirAll(checkpointsDir, 0755); err != nil {
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
	if err := os.WriteFile(cpFile, []byte("0"), 0644); err != nil {
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
