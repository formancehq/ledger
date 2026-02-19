package application

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/restorepb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/service/check"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/formancehq/ledger-v3-poc/internal/storage/tarutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	restoreStagingDir  = "restore-staging"
	restoredMarkerFile = "RESTORED"
)

// RestoredMarker is the JSON structure written to the RESTORED marker file.
type RestoredMarker struct {
	LastAppliedIndex     uint64 `json:"lastAppliedIndex"`
	LastAppliedTimestamp uint64 `json:"lastAppliedTimestamp"`
}

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
func (s *RestoreServiceServerImpl) UploadBackup(stream grpc.ClientStreamingServer[restorepb.UploadBackupRequest, restorepb.UploadBackupResponse]) error {
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
	if err := os.RemoveAll(stagingDir); err != nil {
		return fmt.Errorf("cleaning staging directory: %w", err)
	}
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
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
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = pw.CloseWithError(err)
			return fmt.Errorf("receiving upload chunk: %w", err)
		}

		if req.Eof {
			expectedHash = req.ContentSha256
			break
		}

		if len(req.Data) > 0 {
			if _, err := pw.Write(req.Data); err != nil {
				return fmt.Errorf("writing to tar pipe: %w", err)
			}
			if _, err := hash.Write(req.Data); err != nil {
				return fmt.Errorf("computing hash: %w", err)
			}
			totalReceived += uint64(len(req.Data))
		}
	}

	// Close write end to signal EOF to tar reader
	if err := pw.Close(); err != nil {
		return fmt.Errorf("closing pipe: %w", err)
	}

	// Wait for extraction to complete
	if err := <-extractErrCh; err != nil {
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
func (s *RestoreServiceServerImpl) ValidateRestore(_ *restorepb.ValidateRestoreRequest, stream grpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error {
	s.mu.Lock()
	uploaded := s.uploaded
	s.mu.Unlock()

	if !uploaded {
		return status.Error(codes.FailedPrecondition, "no backup uploaded; upload a backup first")
	}

	stagingDir := s.stagingDir()

	store, err := data.OpenReadOnly(stagingDir, s.logger)
	if err != nil {
		return fmt.Errorf("opening staging store: %w", err)
	}
	defer func() { _ = store.Close() }()

	attrs := attributes.New()
	checker := check.NewChecker(store, attrs)

	return checker.Check(stream.Context(), func(event *servicepb.CheckStoreEvent) {
		// Convert CheckStoreEvent to ValidateRestoreEvent
		var restoreEvent restorepb.ValidateRestoreEvent

		switch t := event.Type.(type) {
		case *servicepb.CheckStoreEvent_Progress:
			restoreEvent.Type = &restorepb.ValidateRestoreEvent_Progress{
				Progress: &restorepb.ValidateRestoreProgress{
					LogsChecked: t.Progress.LogsChecked,
					TotalLogs:   t.Progress.TotalLogs,
				},
			}
		case *servicepb.CheckStoreEvent_Error:
			restoreEvent.Type = &restorepb.ValidateRestoreEvent_Error{
				Error: &restorepb.ValidateRestoreError{
					Message: t.Error.Message,
				},
			}
		}

		if err := stream.Send(&restoreEvent); err != nil {
			s.logger.WithFields(map[string]any{"error": err}).Errorf("Failed to send validate event")
		}
	})
}

// PreviewRestore returns a summary of the staged backup data.
func (s *RestoreServiceServerImpl) PreviewRestore(_ context.Context, _ *restorepb.PreviewRestoreRequest) (*restorepb.PreviewRestoreResponse, error) {
	s.mu.Lock()
	uploaded := s.uploaded
	s.mu.Unlock()

	if !uploaded {
		return nil, status.Error(codes.FailedPrecondition, "no backup uploaded; upload a backup first")
	}

	stagingDir := s.stagingDir()

	store, err := data.OpenReadOnly(stagingDir, s.logger)
	if err != nil {
		return nil, fmt.Errorf("opening staging store: %w", err)
	}
	defer func() { _ = store.Close() }()

	lastAppliedIndex, err := store.GetLastAppliedIndex()
	if err != nil {
		return nil, fmt.Errorf("getting last applied index: %w", err)
	}

	lastAppliedTimestamp, err := store.GetLastAppliedTimestamp()
	if err != nil {
		return nil, fmt.Errorf("getting last applied timestamp: %w", err)
	}

	lastSequence, err := store.GetLastSequence()
	if err != nil {
		return nil, fmt.Errorf("getting last sequence: %w", err)
	}

	cursor, err := store.ListLedgers()
	if err != nil {
		return nil, fmt.Errorf("listing ledgers: %w", err)
	}
	defer func() { _ = cursor.Close() }()

	var ledgerNames []string
	for {
		ledger, err := cursor.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("iterating ledgers: %w", err)
		}
		ledgerNames = append(ledgerNames, ledger.Name)
	}

	return &restorepb.PreviewRestoreResponse{
		LastAppliedIndex:     lastAppliedIndex,
		LastAppliedTimestamp: lastAppliedTimestamp,
		LastSequence:         lastSequence,
		LedgerCount:          uint32(len(ledgerNames)),
		LedgerNames:         ledgerNames,
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
	store, err := data.OpenReadOnly(stagingDir, s.logger)
	if err != nil {
		return nil, fmt.Errorf("opening staging store: %w", err)
	}

	lastAppliedIndex, err := store.GetLastAppliedIndex()
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("getting last applied index: %w", err)
	}

	lastAppliedTimestamp, err := store.GetLastAppliedTimestamp()
	if err != nil {
		_ = store.Close()
		return nil, fmt.Errorf("getting last applied timestamp: %w", err)
	}

	if err := store.Close(); err != nil {
		return nil, fmt.Errorf("closing staging store: %w", err)
	}

	// Write RESTORED marker
	marker := RestoredMarker{
		LastAppliedIndex:     lastAppliedIndex,
		LastAppliedTimestamp: lastAppliedTimestamp,
	}
	markerData, err := json.Marshal(marker)
	if err != nil {
		return nil, fmt.Errorf("marshaling restored marker: %w", err)
	}

	markerPath := filepath.Join(s.dataDir, restoredMarkerFile)
	if err := os.WriteFile(markerPath, markerData, 0644); err != nil {
		return nil, fmt.Errorf("writing restored marker: %w", err)
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

	if err := data.HardLink(stagingDir, checkpointPath); err != nil {
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
func RegisterRestoreService(server *grpc.Server, impl restorepb.RestoreServiceServer) {
	restorepb.RegisterRestoreServiceServer(server, impl)
}
