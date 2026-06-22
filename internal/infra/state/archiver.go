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
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ArchiveRequest contains the data needed to archive a chapter.
//
// Logs and audit entries advance on independent sequence counters, so the
// archive must carry BOTH ranges (#312). Iterating `SubColdAudit` /
// `SubColdAuditItem` with the log range silently drops every audit entry
// whose audit sequence happens to fall outside the log window — and the
// subsequent purge still removes them from Pebble. The audit trail goes
// straight to the floor.
type ArchiveRequest struct {
	ChapterID          uint64
	StartSequence      uint64 // First log sequence in the chapter
	CloseSequence      uint64 // Last log sequence in the chapter (the CloseChapter log)
	StartAuditSequence uint64 // First audit sequence in the chapter
	CloseAuditSequence uint64 // Last audit sequence when the chapter was closed
}

// ArchiveProposer is a callback to propose a ConfirmArchiveChapter order back into Raft.
type ArchiveProposer func(chapterID uint64) error

//go:generate mockgen -typed -write_source_comment=false -write_package_comment=false -source archiver.go -destination archiver_chapter_state_generated_test.go -package state . ArchiverChapterState

// ArchiverChapterState provides the Archiver with read access to the current
// chapter state, used to gate consumption of stale archive requests after a
// follower sync — see Archiver.archive for the rationale.
// Implemented by *Machine.
type ArchiverChapterState interface {
	ArchivingChapterByID(id uint64) (*commonpb.Chapter, bool)
}

// Archiver runs in the background to export closed chapter data to cold storage
// and propose ConfirmArchiveChapter back into Raft for deterministic purge.
// It follows the same pattern as Sealer: a background goroutine reads from
// archiveRequestCh, performs I/O off the Raft critical path, then proposes.
type Archiver struct {
	logger           logging.Logger
	dataStore        dal.ColdStorageScanner
	coldStorage      coldstorage.ColdStorage
	archiveRequestCh *worker.Channel[ArchiveRequest]
	proposeFn        ArchiveProposer
	isLeader         func() bool
	chapterState     ArchiverChapterState
	reconcileFn      func(stop <-chan struct{})
	w                worker.Worker
	bucketID         string
}

// archiveReconcileInterval is the interval at which the Archiver re-checks
// for pending archive requests that may have been missed due to dropped signals.
const archiveReconcileInterval = 30 * time.Second

// NewArchiver creates a new background archiver.
// reconcileFn re-dispatches ARCHIVING chapters from durable state to the channel.
func NewArchiver(
	logger logging.Logger,
	dataStore dal.ColdStorageScanner,
	coldStorage coldstorage.ColdStorage,
	archiveRequestCh *worker.Channel[ArchiveRequest],
	proposeFn ArchiveProposer,
	isLeader func() bool,
	chapterState ArchiverChapterState,
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
		chapterState:     chapterState,
		reconcileFn:      reconcileFn,
		w:                worker.New(),
		bucketID:         bucketID,
	}
}

// Start launches the background archiving goroutine with periodic reconciliation.
func (a *Archiver) Start() {
	a.w.Run(func(stop <-chan struct{}) {
		// Periodic reconciliation: re-scan for ARCHIVING chapters.
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

// archive exports chapter data to cold storage and proposes ConfirmArchiveChapter.
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
		"chapterId":     req.ChapterID,
		"startSequence": req.StartSequence,
		"closeSequence": req.CloseSequence,
	}

	// Reject stale requests whose chapter has moved past ARCHIVING. After a
	// follower sync, the leader's checkpoint may have already promoted this
	// chapter to ARCHIVED (entries purged) — iterating the request's sequence
	// ranges would surface zero entries and we'd upload an empty SST. The
	// drain in SynchronizeWithLeader handles most of these, but the guard
	// also covers requests that race the leader's confirm via Raft (chapter
	// transitions between TrySend and consumption).
	if _, ok := a.chapterState.ArchivingChapterByID(req.ChapterID); !ok {
		a.logger.WithFields(logFields).Infof("Chapter no longer ARCHIVING (sealed/archived by leader), skipping")

		return nil
	}

	exists, err := a.coldStorage.Exists(ctx, a.bucketID, req.ChapterID)
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
		if err := a.verifyExistingArchive(ctx, req.ChapterID); err != nil {
			return err
		}

		a.logger.WithFields(logFields).Infof("Archive integrity verified, proposing ConfirmArchiveChapter")
		if err := a.proposeFn(req.ChapterID); err != nil {
			return fmt.Errorf("proposing ConfirmArchiveChapter for chapter %d: %w", req.ChapterID, err)
		}

		return nil
	}

	// Archive doesn't exist yet — only the leader should upload.
	if !a.isLeader() {
		return worker.ErrNotLeader
	}

	a.logger.WithFields(logFields).Infof("Starting chapter archival")

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

	uploadErr := a.coldStorage.Archive(ctx, a.bucketID, req.ChapterID, sstFile, localChecksum)
	_ = sstFile.Close()

	if uploadErr != nil {
		return fmt.Errorf("uploading archive: %w", uploadErr)
	}

	// Sanity-check the upload by recomputing the remote checksum and
	// comparing with the local one. Catches backend bugs or in-flight bit
	// flips before we propose Confirm.
	remoteChecksum, err := a.coldStorage.Checksum(ctx, a.bucketID, req.ChapterID)
	if err != nil {
		return fmt.Errorf("computing remote archive checksum: %w", err)
	}

	if !bytes.Equal(localChecksum, remoteChecksum) {
		return fmt.Errorf("archive checksum mismatch for chapter %d after upload: local=%s remote=%s",
			req.ChapterID, hex.EncodeToString(localChecksum), hex.EncodeToString(remoteChecksum))
	}

	a.logger.WithFields(logFields).Infof("Chapter archival complete, proposing ConfirmArchiveChapter")

	// Propose ConfirmArchiveChapter back into Raft
	if err := a.proposeFn(req.ChapterID); err != nil {
		return fmt.Errorf("proposing ConfirmArchiveChapter for chapter %d: %w", req.ChapterID, err)
	}

	return nil
}

// verifyExistingArchive reads the expected SHA-256 stored alongside the
// archive at upload time, recomputes the current SHA-256 of the data, and
// fails if they do not match. Used by the crash-recovery path before
// proposing ConfirmArchiveChapter.
func (a *Archiver) verifyExistingArchive(ctx context.Context, chapterID uint64) error {
	expected, err := a.coldStorage.ExpectedChecksum(ctx, a.bucketID, chapterID)
	if err != nil {
		return fmt.Errorf("reading expected checksum for chapter %d: %w", chapterID, err)
	}

	actual, err := a.coldStorage.Checksum(ctx, a.bucketID, chapterID)
	if err != nil {
		return fmt.Errorf("reading current checksum for chapter %d: %w", chapterID, err)
	}

	if !bytes.Equal(expected, actual) {
		return fmt.Errorf("archive integrity check failed for chapter %d: expected=%s actual=%s",
			chapterID, hex.EncodeToString(expected), hex.EncodeToString(actual))
	}

	return nil
}

// chapterMetadata is the JSON metadata included in the archive. Every field
// here must be a deterministic function of the chapter being archived, so
// that two archive builds of the same chapter produce byte-identical SSTs
// (and therefore identical checksums). Adding a timestamp or any other
// non-deterministic field would break checksum verification — keep this
// struct boring.
type chapterMetadata struct {
	ChapterID          uint64 `json:"chapterId"`
	StartSequence      uint64 `json:"startSequence"`
	CloseSequence      uint64 `json:"closeSequence"`
	StartAuditSequence uint64 `json:"startAuditSequence"`
	CloseAuditSequence uint64 `json:"closeAuditSequence"`
}

// MetadataKey is the SST key used for chapter metadata.
// Prefix 0x00 sorts before the cold zone (0x01+) so it won't interfere with queries.
var MetadataKey = []byte{0x00, 'm', 'e', 't', 'a', 'd', 'a', 't', 'a'}

// buildSSTArchive writes a Pebble SST file to a temp file containing chapter metadata
// and all cold-storable KV pairs for the chapter in a single pass.
//
// SST key layout:
//   - [0x00]["metadata"] → JSON chapter metadata
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

	// 1. Write metadata (sorts first due to 0x00 prefix). chapterMetadata
	// shares the same shape as ArchiveRequest; the conversion is a no-op
	// and keeps the two struct definitions in sync at compile time.
	meta := chapterMetadata(req)

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		_ = writer.Close()
		_ = os.Remove(tmpPath)

		return "", nil, fmt.Errorf("marshaling chapter metadata: %w", err)
	}

	if err := writer.Set(MetadataKey, metaJSON); err != nil {
		_ = writer.Close()
		_ = os.Remove(tmpPath)

		return "", nil, fmt.Errorf("writing metadata to SST: %w", err)
	}

	// 2. Write all cold-storable KV pairs (already sorted from Pebble). Logs
	// and audit have independent sequence counters; the store applies each
	// range to its own zone (#312).
	if err := a.dataStore.IterateColdKVPairs(
		req.StartSequence, req.CloseSequence,
		req.StartAuditSequence, req.CloseAuditSequence,
		func(key, value []byte) error {
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
