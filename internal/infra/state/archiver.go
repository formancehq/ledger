package state

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/infra/coldstorage"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ArchiveRequest contains the data needed to archive a period.
type ArchiveRequest struct {
	PeriodID      uint64
	StartSequence uint64 // First log sequence in the period
	CloseSequence uint64 // Last log sequence in the period (the ClosePeriod log)
}

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
	w                worker.Worker
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
		w:                worker.New(),
		bucketID:         bucketID,
	}
}

// Start launches the background archiving goroutine.
// Crash recovery is automatic via WAL replay: if the node crashes between
// ArchivePeriodLog and ConfirmArchivePeriodLog, WAL replay re-sends the
// ArchiveRequest to the channel, and the export is idempotent.
func (a *Archiver) Start() {
	a.w.Run(func(stop <-chan struct{}) {
		worker.DrainChannel(stop, a.archiveRequestCh, func(req ArchiveRequest) {
			worker.RetryWithBackoff(stop, a.logger, func() error {
				return a.archive(stop, req)
			})
		})
	})
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (a *Archiver) Stop() {
	a.w.Stop()
}

// archive exports period data to cold storage and proposes ConfirmArchivePeriod.
//
// The flow handles both leader and follower nodes:
//   - First, check if the archive already exists in cold storage. If it does,
//     the leader already pushed it — followers exit silently, and the leader
//     proposes ConfirmArchivePeriod (crash-recovery idempotency).
//   - If the archive does not exist yet and this node is not the leader,
//     return worker.ErrNotLeader so the retry loop waits and re-checks.
//   - Only the leader builds, uploads, and proposes.
func (a *Archiver) archive(stop <-chan struct{}, req ArchiveRequest) error {
	// Derive a cancellable context from the worker's stop channel so that
	// cold storage I/O (Exists, Archive) is interrupted during shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()

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
		return worker.ErrNotLeader
	}

	a.logger.WithFields(logFields).Infof("Starting period archival")

	// Stream the tar.gz archive directly to cold storage via a pipe,
	// avoiding buffering the entire archive in memory.
	pr, pw := io.Pipe()

	streamErrCh := make(chan error, 1)

	go func() {
		err := a.streamArchive(req, pw)

		_ = pw.CloseWithError(err) // signals EOF (or error) to the reader side
		streamErrCh <- err
	}()

	// Upload to cold storage — reads from the pipe as streamArchive writes.
	uploadErr := a.coldStorage.Archive(ctx, a.bucketID, req.PeriodID, pr)

	// Close the reader so the writer goroutine doesn't block if upload failed early.
	_ = pr.CloseWithError(uploadErr)

	// Wait for the writer goroutine to finish.
	streamErr := <-streamErrCh

	if streamErr != nil && uploadErr != nil {
		return fmt.Errorf("streaming archive: %w (upload also failed: %w)", streamErr, uploadErr)
	}

	if streamErr != nil {
		return fmt.Errorf("streaming archive: %w", streamErr)
	}

	if uploadErr != nil {
		return fmt.Errorf("uploading archive: %w", uploadErr)
	}

	// Verify the upload
	exists, err = a.coldStorage.Exists(ctx, a.bucketID, req.PeriodID)
	if err != nil {
		return fmt.Errorf("verifying archive: %w", err)
	}

	if !exists {
		return errors.New("archive verification failed: archive not found after upload")
	}

	a.logger.WithFields(logFields).Infof("Period archival complete, proposing ConfirmArchivePeriod")

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

// streamArchive writes a tar.gz archive to w containing period metadata and a
// raw binary dump of all cold-storable Pebble KV pairs for the period.
//
// It uses a two-pass approach over IterateColdKVPairs to avoid buffering:
//   - Pass 1: compute the total data.bin size (tar needs it upfront)
//   - Pass 2: stream KV pairs directly through tar → gzip → w
//
// Archive layout:
//   - metadata.json  — period ID, sequence range, archival timestamp
//   - data.bin       — raw KV pairs: [keyLen:4][key][valueLen:4][value]...
func (a *Archiver) streamArchive(req ArchiveRequest, w io.Writer) error {
	gzWriter := gzip.NewWriter(w)
	tarWriter := tar.NewWriter(gzWriter)

	// 1. Add period metadata JSON (always small, OK to buffer)
	meta := periodMetadata{
		PeriodID:      req.PeriodID,
		StartSequence: req.StartSequence,
		CloseSequence: req.CloseSequence,
		ArchivedAt:    time.Now().UTC().Format(time.RFC3339),
	}

	metaJSON, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling period metadata: %w", err)
	}

	if err := addTarEntry(tarWriter, "metadata.json", metaJSON); err != nil {
		return err
	}

	// 2. Dump all cold-storable KV pairs as raw binary (two-pass streaming)
	// Pass 1: compute data.bin size so we can write the tar header.
	var dataSize int64

	if err := a.dataStore.IterateColdKVPairs(req.StartSequence, req.CloseSequence, func(key, value []byte) error {
		dataSize += 4 + int64(len(key)) + 4 + int64(len(value))

		return nil
	}); err != nil {
		return fmt.Errorf("computing data size: %w", err)
	}

	// Pass 2: stream KV pairs directly into the tar entry.
	if dataSize > 0 {
		err := tarWriter.WriteHeader(&tar.Header{
			Name: "data.bin",
			Mode: 0o644,
			Size: dataSize,
		})
		if err != nil {
			return fmt.Errorf("writing tar header for data.bin: %w", err)
		}

		var lenBuf [4]byte

		err = a.dataStore.IterateColdKVPairs(req.StartSequence, req.CloseSequence, func(key, value []byte) error {
			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(key)))

			if _, err := tarWriter.Write(lenBuf[:]); err != nil {
				return err
			}

			if _, err := tarWriter.Write(key); err != nil {
				return err
			}

			binary.BigEndian.PutUint32(lenBuf[:], uint32(len(value)))

			if _, err := tarWriter.Write(lenBuf[:]); err != nil {
				return err
			}

			if _, err := tarWriter.Write(value); err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			return fmt.Errorf("streaming cold KV pairs: %w", err)
		}
	}

	if err := tarWriter.Close(); err != nil {
		return fmt.Errorf("closing tar writer: %w", err)
	}

	if err := gzWriter.Close(); err != nil {
		return fmt.Errorf("closing gzip writer: %w", err)
	}

	return nil
}

// addTarEntry adds a file entry to a tar archive.
func addTarEntry(tw *tar.Writer, name string, data []byte) error {
	header := &tar.Header{
		Name: name,
		Mode: 0o644,
		Size: int64(len(data)),
	}

	err := tw.WriteHeader(header)
	if err != nil {
		return fmt.Errorf("writing tar header for %s: %w", name, err)
	}

	if _, err := tw.Write(data); err != nil {
		return fmt.Errorf("writing tar data for %s: %w", name, err)
	}

	return nil
}
