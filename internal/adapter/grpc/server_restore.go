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
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const (
	restoreStagingDir = "restore-staging"

	// defaultDownloadParallelism is the fallback when no --restore-download-parallelism
	// flag is provided. 16 saturates typical S3 throughput without flooding the
	// node's file descriptor budget or Pebble's open-files limit.
	defaultDownloadParallelism = 16

	// minDownloadParallelism / maxDownloadParallelism clamp the configured value
	// so a misconfigured operator cannot disable parallelism entirely or open
	// hundreds of concurrent S3 connections.
	minDownloadParallelism = 1
	maxDownloadParallelism = 64

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

// clampParallelism returns a download parallelism value within [min, max],
// substituting the default when v == 0.
func clampParallelism(v int) int {
	if v == 0 {
		v = defaultDownloadParallelism
	}

	if v < minDownloadParallelism {
		return minDownloadParallelism
	}

	if v > maxDownloadParallelism {
		return maxDownloadParallelism
	}

	return v
}

// RestoreServiceServerImpl implements the RestoreService gRPC server.
//
// The download phase is decoupled from the calling RPC context: StartDownloadBackup
// kicks off a background job and returns immediately, GetDownloadStatus exposes
// the running state, and CancelDownload aborts it. This avoids ingress / load
// balancer timeouts on multi-hour transfers (issue #349).
type RestoreServiceServerImpl struct {
	restorepb.UnimplementedRestoreServiceServer

	mu          sync.Mutex
	dataDir     string
	clusterID   string
	parallelism int // effective per-job worker count, already clamped
	logger      logging.Logger
	downloading bool
	downloaded  bool

	// job holds the single active download (if any). Only one job runs at a
	// time because the staging directory is a singleton. Successive Start
	// calls reject while a job is RUNNING and after a SUCCEEDED job — the
	// caller must Finalize or restart the server.
	//
	// A terminal job (SUCCEEDED / FAILED / CANCELED) stays in this field so
	// the client can still poll its final state via GetDownloadStatus.
	job *downloadJob

	// stagingStore is the read-write Pebble handle on the staging directory,
	// opened by the download job once the data is in place and the export
	// segments have been applied, and kept alive for the rest of the restore
	// lifecycle. ValidateRestore, PreviewRestore, and FinalizeRestore all
	// reuse it instead of re-opening the staging Pebble in the same process
	// — Pebble v2 forbids that (vfs/file_lock_unix.go: "lock held by current
	// process"), and even if it did not, repeatedly warming up the table
	// metadata for thousands of SSTs on a 1+ TB staging directory would be
	// gratuitously slow.
	stagingStore *dal.Store

	// storageFactory builds the backup.Storage used by a download job.
	// Production code leaves this nil (defaults to S3); tests inject a fake
	// to avoid touching live infrastructure.
	storageFactory storageFactory
}

// NewRestoreServiceServer creates a new RestoreServiceServerImpl. parallelism
// caps concurrent S3 file downloads during the async download phase; 0 falls
// back to the default and out-of-range values are clamped to [1, 64].
func NewRestoreServiceServer(dataDir, clusterID string, parallelism int, logger logging.Logger) *RestoreServiceServerImpl {
	return &RestoreServiceServerImpl{
		dataDir:     dataDir,
		clusterID:   clusterID,
		parallelism: clampParallelism(parallelism),
		logger:      logger,
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

// ValidateRestore runs integrity checks on the staged backup data.
func (s *RestoreServiceServerImpl) ValidateRestore(_ *restorepb.ValidateRestoreRequest, stream ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error {
	s.mu.Lock()
	downloaded := s.downloaded
	store := s.stagingStore
	s.mu.Unlock()

	if !downloaded || store == nil {
		return status.Error(codes.FailedPrecondition, "no backup downloaded; download a backup first")
	}

	// Use the BACKUP's ClusterID (recorded in its PersistedConfig) to recompute
	// audit hashes, not the local server's clusterID — those may differ when a
	// backup is staged on a fresh node, and the audit chain was hashed under
	// the source cluster's key.
	persisted, err := query.ReadPersistedConfig(store)
	if err != nil {
		return status.Errorf(codes.Internal, "reading staged backup config: %v", err)
	}

	if persisted == nil {
		return status.Error(codes.FailedPrecondition, "staged backup has no persisted config; cannot validate audit chain")
	}

	attrs := attributes.New()
	// No cold reader on this path: it validates a staged backup store, so the
	// idempotency pass keeps the post-archive boundary as its verification floor.
	// nil TTL: there is no trusted runtime config for a foreign backup, so the
	// pass falls back to the backup's persisted TTL.
	checker := check.NewChecker(store, attrs, persisted.GetClusterId(), nil, nil, s.logger)

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

// FinalizeRestore prepares the staged backup (Global-zone resets) and commits it as the live data.
func (s *RestoreServiceServerImpl) FinalizeRestore(_ context.Context, _ *restorepb.FinalizeRestoreRequest) (*restorepb.FinalizeRestoreResponse, error) {
	s.mu.Lock()
	downloaded := s.downloaded
	store := s.stagingStore
	s.mu.Unlock()

	if !downloaded || store == nil {
		return nil, status.Error(codes.FailedPrecondition, "no backup downloaded; download a backup first")
	}

	stagingDir := s.stagingDir()

	// Prepare Global-zone keys (applied index pinned to the restore-genesis
	// index, cluster-local state reset) so the staged backup is restartable
	// on a fresh cluster.
	s.logger.Infof("Preparing backup for restore compatibility")

	if err := attributes.PrepareForBackup(store); err != nil {
		return nil, fmt.Errorf("preparing backup attributes: %w", err)
	}

	// Read metadata after backup preparation. We can do this directly on the same RW
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
