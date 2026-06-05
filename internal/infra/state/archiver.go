package state

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/cockroachdb/pebble/v2/bloom"
	"github.com/cockroachdb/pebble/v2/objstorage"
	"github.com/cockroachdb/pebble/v2/sstable"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
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
	archiveRequestCh *worker.Channel[ArchiveRequest]
	proposeFn        ArchiveProposer
	isLeader         func() bool
	reconcileFn      func(stop <-chan struct{})
	w                worker.Worker
	bucketID         string
}

// archiveReconcileInterval is the interval at which the Archiver re-checks
// for pending archive requests that may have been missed due to dropped signals.
const archiveReconcileInterval = 30 * time.Second

// NewArchiver creates a new background archiver.
// reconcileFn re-dispatches ARCHIVING periods from durable state to the channel.
func NewArchiver(
	logger logging.Logger,
	dataStore *dal.Store,
	coldStorage coldstorage.ColdStorage,
	archiveRequestCh *worker.Channel[ArchiveRequest],
	proposeFn ArchiveProposer,
	isLeader func() bool,
	bucketID string,
	reconcileFn func(stop <-chan struct{}),
) *Archiver {
	return &Archiver{
		logger:           logger.WithFields(map[string]any{"cmp": "archiver"}),
		dataStore:        dataStore,
		coldStorage:      coldStorage,
		archiveRequestCh: archiveRequestCh,
		proposeFn:        proposeFn,
		isLeader:         isLeader,
		reconcileFn:      reconcileFn,
		w:                worker.New(),
		bucketID:         bucketID,
	}
}

// Start launches the background archiving goroutine with periodic reconciliation.
func (a *Archiver) Start() {
	a.w.Run(func(stop <-chan struct{}) {
		// Periodic reconciliation: re-scan for ARCHIVING periods.
		go worker.RunTicker(stop, archiveReconcileInterval, func() {
			if a.isLeader() {
				a.reconcileFn(stop)
			}
		})

		// Main drain loop.
		worker.DrainChannel(stop, a.archiveRequestCh.Receive(), func(req ArchiveRequest) {
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
//   - Exists returns true only when both the archive data AND its persisted
//     checksum are present. On a leader, that lets us read the expected
//     checksum and verify it against the current bytes before proposing.
//     On a follower, that just means the leader is done — exit silently.
//   - A partial upload (data without checksum, e.g. crashed mid-upload) shows
//     up as Exists=false so the leader re-uploads on retry.
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

	exists, err := a.coldStorage.Exists(ctx, a.bucketID, req.PeriodID)
	if err != nil {
		return fmt.Errorf("checking archive existence: %w", err)
	}

	if exists {
		if !a.isLeader() {
			// Follower: the leader pushed the archive, nothing left to do.
			a.logger.WithFields(logFields).Infof("Archive already exists in cold storage, done")

			return nil
		}

		// Leader crash-recovery: validate the existing object against its
		// persisted checksum. A truncated-but-readable SST would still
		// produce a digest, so we MUST compare against the stored reference.
		if err := a.verifyExistingArchive(ctx, req.PeriodID); err != nil {
			return err
		}

		a.logger.WithFields(logFields).Infof("Archive integrity verified, proposing ConfirmArchivePeriod")
		a.proposeFn(req.PeriodID)

		return nil
	}

	// Archive doesn't exist yet — only the leader should upload.
	if !a.isLeader() {
		return worker.ErrNotLeader
	}

	a.logger.WithFields(logFields).Infof("Starting period archival")

	// Build SST archive to a temp file, then upload it.
	tmpPath, localChecksum, err := a.buildSSTArchive(req)
	if err != nil {
		return fmt.Errorf("building SST archive: %w", err)
	}

	defer func() { _ = os.Remove(tmpPath) }()

	// Open for reading and upload to cold storage. Archive persists the
	// checksum atomically with the data.
	sstFile, err := os.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("opening SST archive for upload: %w", err)
	}

	uploadErr := a.coldStorage.Archive(ctx, a.bucketID, req.PeriodID, sstFile, localChecksum)
	_ = sstFile.Close()

	if uploadErr != nil {
		return fmt.Errorf("uploading archive: %w", uploadErr)
	}

	// Sanity-check the upload by recomputing the remote checksum and
	// comparing with the local one. Catches backend bugs or in-flight bit
	// flips before we propose Confirm.
	remoteChecksum, err := a.coldStorage.Checksum(ctx, a.bucketID, req.PeriodID)
	if err != nil {
		return fmt.Errorf("computing remote archive checksum: %w", err)
	}

	if !bytes.Equal(localChecksum, remoteChecksum) {
		return fmt.Errorf("archive checksum mismatch for period %d after upload: local=%s remote=%s",
			req.PeriodID, hex.EncodeToString(localChecksum), hex.EncodeToString(remoteChecksum))
	}

	a.logger.WithFields(logFields).Infof("Period archival complete, proposing ConfirmArchivePeriod")
	a.proposeFn(req.PeriodID)

	return nil
}

// verifyExistingArchive reads the expected SHA-256 stored alongside the
// archive at upload time, recomputes the current SHA-256 of the data, and
// fails if they do not match. Used by the crash-recovery path before
// proposing ConfirmArchivePeriod.
func (a *Archiver) verifyExistingArchive(ctx context.Context, periodID uint64) error {
	expected, err := a.coldStorage.ExpectedChecksum(ctx, a.bucketID, periodID)
	if err != nil {
		return fmt.Errorf("reading expected checksum for period %d: %w", periodID, err)
	}

	actual, err := a.coldStorage.Checksum(ctx, a.bucketID, periodID)
	if err != nil {
		return fmt.Errorf("reading current checksum for period %d: %w", periodID, err)
	}

	if !bytes.Equal(expected, actual) {
		return fmt.Errorf("archive integrity check failed for period %d: expected=%s actual=%s",
			periodID, hex.EncodeToString(expected), hex.EncodeToString(actual))
	}

	return nil
}

// periodMetadata is the JSON metadata included in the archive. Every field
// here must be a deterministic function of the period being archived, so
// that two archive builds of the same period produce byte-identical SSTs
// (and therefore identical checksums). Adding a timestamp or any other
// non-deterministic field would break checksum verification — keep this
// struct boring.
type periodMetadata struct {
	PeriodID      uint64 `json:"periodId"`
	StartSequence uint64 `json:"startSequence"`
	CloseSequence uint64 `json:"closeSequence"`
}

// MetadataKey is the SST key used for period metadata.
// Prefix 0x00 sorts before the cold zone (0x01+) so it won't interfere with queries.
var MetadataKey = []byte{0x00, 'm', 'e', 't', 'a', 'd', 'a', 't', 'a'}

// buildSSTArchive writes a Pebble SST file to a temp file containing period metadata
// and all cold-storable KV pairs for the period in a single pass.
//
// SST key layout:
//   - [0x00]["metadata"] → JSON period metadata
//   - original Pebble keys (0x01+ prefix) → values as-is
func (a *Archiver) buildSSTArchive(req ArchiveRequest) (string, []byte, error) {
	tmpFile, err := os.CreateTemp("", "cold-archive-*.sst")
	if err != nil {
		return "", nil, fmt.Errorf("creating temp file: %w", err)
	}

	tmpPath := tmpFile.Name()

	writer := sstable.NewWriter(newFileWritable(tmpFile), sstable.WriterOptions{
		Compression:  sstable.SnappyCompression,
		FilterPolicy: bloom.FilterPolicy(10),
	})

	// 1. Write metadata (sorts first due to 0x00 prefix). periodMetadata
	// shares the same shape as ArchiveRequest; the conversion is a no-op
	// and keeps the two struct definitions in sync at compile time.
	meta := periodMetadata(req)

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		_ = writer.Close()
		_ = os.Remove(tmpPath)

		return "", nil, fmt.Errorf("marshaling period metadata: %w", err)
	}

	if err := writer.Set(MetadataKey, metaJSON); err != nil {
		_ = writer.Close()
		_ = os.Remove(tmpPath)

		return "", nil, fmt.Errorf("writing metadata to SST: %w", err)
	}

	// 2. Write all cold-storable KV pairs (already sorted from Pebble)
	if err := a.dataStore.IterateColdKVPairs(req.StartSequence, req.CloseSequence, func(key, value []byte) error {
		return writer.Set(key, value)
	}); err != nil {
		_ = writer.Close()
		_ = os.Remove(tmpPath)

		return "", nil, fmt.Errorf("writing cold KV pairs to SST: %w", err)
	}

	if err := writer.Close(); err != nil {
		_ = os.Remove(tmpPath)

		return "", nil, fmt.Errorf("closing SST writer: %w", err)
	}

	// Compute SHA-256 checksum of the completed SST file.
	checksumFile, err := os.Open(tmpPath)
	if err != nil {
		_ = os.Remove(tmpPath)

		return "", nil, fmt.Errorf("opening SST for checksum: %w", err)
	}

	checksum, err := coldstorage.ComputeSHA256(checksumFile)
	_ = checksumFile.Close()

	if err != nil {
		_ = os.Remove(tmpPath)

		return "", nil, fmt.Errorf("computing SST checksum: %w", err)
	}

	return tmpPath, checksum, nil
}

// fileWritable adapts an *os.File to the objstorage.Writable interface.
type fileWritable struct {
	f *os.File
}

func newFileWritable(f *os.File) objstorage.Writable {
	return &fileWritable{f: f}
}

func (w *fileWritable) Write(p []byte) error {
	_, err := w.f.Write(p)

	return err
}

func (w *fileWritable) Finish() error {
	if err := w.f.Sync(); err != nil {
		_ = w.f.Close()

		return err
	}

	return w.f.Close()
}

func (w *fileWritable) Abort() {
	_ = w.f.Close()
}
