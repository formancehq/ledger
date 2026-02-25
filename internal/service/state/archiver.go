package state

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/infra/coldstorage"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ArchiveRequest contains the data needed to archive a period.
type ArchiveRequest struct {
	PeriodID      uint64
	StartSequence uint64 // First log sequence in the period
	CloseSequence uint64 // Last log sequence in the period (the ClosePeriod log)
}

// errNotLeader is returned by archive() when the current node is not the Raft leader.
// The retry loop uses this to wait and re-check leadership.
var errNotLeader = fmt.Errorf("not leader, skipping archive")

// ArchiveProposer is a callback to propose a ConfirmArchivePeriod order back into Raft.
type ArchiveProposer func(periodID uint64)

// Archiver runs in the background to export closed period data to cold storage
// and propose ConfirmArchivePeriod back into Raft for deterministic purge.
// It follows the same pattern as Sealer: a background goroutine reads from
// archiveRequestCh, performs I/O off the Raft critical path, then proposes.
type Archiver struct {
	logger           logging.Logger
	dataStore        *dal.Store
	coldStorage      coldstorage.ColdStorage
	archiveRequestCh chan ArchiveRequest
	proposeFn        ArchiveProposer
	isLeader         func() bool
	stopCh           chan struct{}
	doneCh           chan struct{}
	bucketID         string
}

// NewArchiver creates a new background archiver.
func NewArchiver(
	logger logging.Logger,
	dataStore *dal.Store,
	coldStorage coldstorage.ColdStorage,
	archiveRequestCh chan ArchiveRequest,
	proposeFn ArchiveProposer,
	isLeader func() bool,
	bucketID string,
) *Archiver {
	return &Archiver{
		logger:           logger.WithFields(map[string]any{"cmp": "archiver"}),
		dataStore:        dataStore,
		coldStorage:      coldStorage,
		archiveRequestCh: archiveRequestCh,
		proposeFn:        proposeFn,
		isLeader:         isLeader,
		stopCh:           make(chan struct{}),
		doneCh:           make(chan struct{}),
		bucketID:         bucketID,
	}
}

// Start launches the background archiving goroutine.
// Crash recovery is automatic via WAL replay: if the node crashes between
// ArchivePeriodLog and ConfirmArchivePeriodLog, WAL replay re-sends the
// ArchiveRequest to the channel, and the export is idempotent.
func (a *Archiver) Start() {
	go a.run()
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (a *Archiver) Stop() {
	close(a.stopCh)
	<-a.doneCh
}

func (a *Archiver) run() {
	defer close(a.doneCh)

	for {
		select {
		case <-a.stopCh:
			return
		case req := <-a.archiveRequestCh:
			a.archiveWithRetry(req)
		}
	}
}

// archiveWithRetry retries archive() with exponential backoff until it succeeds
// or the archiver is stopped. The export is idempotent (same data, same key).
// On follower nodes, the loop exits when the archive appears in cold storage
// (pushed by the leader), without calling ArchiveProposer.
func (a *Archiver) archiveWithRetry(req ArchiveRequest) {
	backoff := 100 * time.Millisecond
	const maxBackoff = 10 * time.Second

	for {
		err := a.archive(req)
		if err == nil {
			return
		}

		if errors.Is(err, errNotLeader) {
			a.logger.WithFields(map[string]any{
				"periodId": req.PeriodID,
			}).Infof("Not leader, waiting %v before re-checking", backoff)
		} else {
			a.logger.Errorf("Background archiving failed (will retry in %v): %v", backoff, err)
		}

		select {
		case <-a.stopCh:
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// archive exports period data to cold storage and proposes ConfirmArchivePeriod.
//
// The flow handles both leader and follower nodes:
//   - First, check if the archive already exists in cold storage. If it does,
//     the leader already pushed it — followers exit silently, and the leader
//     proposes ConfirmArchivePeriod (crash-recovery idempotency).
//   - If the archive does not exist yet and this node is not the leader,
//     return errNotLeader so the retry loop waits and re-checks.
//   - Only the leader builds, uploads, and proposes.
func (a *Archiver) archive(req ArchiveRequest) error {
	ctx := context.Background()
	logFields := map[string]any{
		"periodId":      req.PeriodID,
		"startSequence": req.StartSequence,
		"closeSequence": req.CloseSequence,
	}

	// Check if already archived — this lets followers detect that the leader
	// completed the upload and exit the retry loop without proposing.
	exists, err := a.coldStorage.Exists(ctx, a.bucketID, req.PeriodID)
	if err != nil {
		return fmt.Errorf("checking archive existence: %w", err)
	}
	if exists {
		if a.isLeader() {
			// Leader crash-recovery: we uploaded before crashing, propose confirm.
			a.logger.WithFields(logFields).Infof("Archive already exists, proposing ConfirmArchivePeriod")
			a.proposeFn(req.PeriodID)
		} else {
			// Follower: the leader pushed the archive, nothing left to do.
			a.logger.WithFields(logFields).Infof("Archive already exists in cold storage, done")
		}
		return nil
	}

	// Archive doesn't exist yet — only the leader should upload.
	if !a.isLeader() {
		return errNotLeader
	}

	a.logger.WithFields(logFields).Infof("Starting period archival")

	// Build the tar.gz archive in memory
	archiveData, err := a.buildArchive(req)
	if err != nil {
		return fmt.Errorf("building archive: %w", err)
	}

	// Upload to cold storage
	if err := a.coldStorage.Archive(ctx, a.bucketID, req.PeriodID, bytes.NewReader(archiveData)); err != nil {
		return fmt.Errorf("uploading archive: %w", err)
	}

	// Verify the upload
	exists, err = a.coldStorage.Exists(ctx, a.bucketID, req.PeriodID)
	if err != nil {
		return fmt.Errorf("verifying archive: %w", err)
	}
	if !exists {
		return fmt.Errorf("archive verification failed: archive not found after upload")
	}

	a.logger.WithFields(map[string]any{
		"periodId":    req.PeriodID,
		"archiveSize": len(archiveData),
	}).Infof("Period archival complete, proposing ConfirmArchivePeriod")

	// Propose ConfirmArchivePeriod back into Raft
	a.proposeFn(req.PeriodID)

	return nil
}

// periodMetadata is the JSON metadata included in the archive.
type periodMetadata struct {
	PeriodID      uint64 `json:"periodId"`
	StartSequence uint64 `json:"startSequence"`
	CloseSequence uint64 `json:"closeSequence"`
	ArchivedAt    string `json:"archivedAt"`
}

// buildArchive creates a tar.gz archive containing period metadata and a raw
// binary dump of all cold-storable Pebble KV pairs for the period.
//
// Archive layout:
//   - metadata.json  — period ID, sequence range, archival timestamp
//   - data.bin       — raw KV pairs: [keyLen:4][key][valueLen:4][value]...
func (a *Archiver) buildArchive(req ArchiveRequest) ([]byte, error) {
	var buf bytes.Buffer
	gzWriter := gzip.NewWriter(&buf)
	tarWriter := tar.NewWriter(gzWriter)

	// 1. Add period metadata JSON
	meta := periodMetadata{
		PeriodID:      req.PeriodID,
		StartSequence: req.StartSequence,
		CloseSequence: req.CloseSequence,
		ArchivedAt:    time.Now().UTC().Format(time.RFC3339),
	}
	metaJSON, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshaling period metadata: %w", err)
	}
	if err := addTarEntry(tarWriter, "metadata.json", metaJSON); err != nil {
		return nil, err
	}

	// 2. Dump all cold-storable KV pairs as raw binary
	var dataBuf bytes.Buffer
	var lenBuf [4]byte
	if err := a.dataStore.IterateColdKVPairs(req.StartSequence, req.CloseSequence, func(key, value []byte) error {
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(key)))
		dataBuf.Write(lenBuf[:])
		dataBuf.Write(key)
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(value)))
		dataBuf.Write(lenBuf[:])
		dataBuf.Write(value)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("iterating cold KV pairs: %w", err)
	}
	if dataBuf.Len() > 0 {
		if err := addTarEntry(tarWriter, "data.bin", dataBuf.Bytes()); err != nil {
			return nil, err
		}
	}

	if err := tarWriter.Close(); err != nil {
		return nil, fmt.Errorf("closing tar writer: %w", err)
	}
	if err := gzWriter.Close(); err != nil {
		return nil, fmt.Errorf("closing gzip writer: %w", err)
	}

	return buf.Bytes(), nil
}

// addTarEntry adds a file entry to a tar archive.
func addTarEntry(tw *tar.Writer, name string, data []byte) error {
	header := &tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(data)),
	}
	if err := tw.WriteHeader(header); err != nil {
		return fmt.Errorf("writing tar header for %s: %w", name, err)
	}
	if _, err := tw.Write(data); err != nil {
		return fmt.Errorf("writing tar data for %s: %w", name, err)
	}
	return nil
}
