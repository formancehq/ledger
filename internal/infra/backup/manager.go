package backup

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Result contains statistics from a full backup run.
type Result struct {
	FilesUploaded     int
	FilesDeleted      int
	OrphansDeleted    int
	TotalFiles        int
	LastLogSequence   uint64
	LastAuditSequence uint64
	LastAppliedIndex  uint64
	Duration          time.Duration
}

// IncrementalBackupResult contains statistics from an incremental backup run.
type IncrementalBackupResult struct {
	LogEntriesExported   uint64
	AuditEntriesExported uint64
	SegmentsUploaded     int
	OrphansDeleted       int
	Duration             time.Duration
	LastLogSequence      uint64
	LastAuditSequence    uint64
}

// RunBackup performs a full checkpoint backup cycle.
// It creates a Pebble checkpoint, diffs SST files against the previous manifest,
// uploads new/changed files, cleans up stale exports, and writes an updated manifest.
//
// checkpointName is the name of the temporary checkpoint directory the
// store creates. Callers running multiple concurrent backups MUST pass
// distinct names so the underlying tmp/<name>/ directories do not
// collide on the local filesystem — the FSM mutex protects the
// destination slot, but the checkpoint directory is node-local.
func RunBackup(
	ctx context.Context,
	logger logging.Logger,
	store *dal.Store,
	storage Storage,
	bucketID string,
	checkpointName string,
) (*Result, error) {
	start := time.Now()

	manifestKey := ManifestKey(bucketID)

	// 1. Create temporary checkpoint (hard links, quasi-free)
	checkpointPath, err := store.CreateTemporaryCheckpoint(checkpointName)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint: %w", err)
	}

	defer func() {
		_ = store.RemoveTemporaryCheckpoint(checkpointName)
	}()

	// 2. List files in the checkpoint, hashing each so its stored key is
	// content-addressed (see CheckpointFile / CheckpointFileKey). The hash both
	// makes the object immutable per content and lets us skip re-uploading files
	// already present under the same key.
	checkpointFiles, err := listCheckpointFiles(bucketID, checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("listing checkpoint files: %w", err)
	}

	// 3. Read existing manifest. It is used for two things: logging the upload
	// delta, and classifying which objects the post-manifest prune (step 8)
	// removes — objects the previous manifest referenced are stale checkpoint
	// files this run superseded (normal churn, counted as FilesDeleted), objects
	// it did not are true orphans leaked by a crashed run (OrphansDeleted).
	// Correctness of the upload no longer depends on diffing sizes — the
	// content-addressed key is the diff.
	// A legacy pre-content-addressing manifest is NOT fatal here: a full backup
	// overwrites the manifest wholesale and never diffs against it, so retaking a
	// full backup with the current binary is exactly the documented recovery path
	// out of a legacy manifest. Treat it as a warning and proceed with no
	// previous-manifest keys (so its objects, which live under bare data/<name>
	// keys, all classify as orphans on prune). (The incremental path, which does
	// depend on the existing manifest, keeps the error fatal — see
	// RunIncrementalBackup.)
	prevManifest, err := ReadManifestOrEmpty(ctx, logger, storage, manifestKey)
	if err != nil {
		if errors.Is(err, ErrLegacyManifestFormat) {
			logger.Infof("Existing manifest uses the legacy pre-content-addressing format; " +
				"a full backup replaces it wholesale, so proceeding and overwriting it")

			prevManifest = &Manifest{}
		} else {
			return nil, err
		}
	}

	prevCheckpointKeys := checkpointKeySet(prevManifest)

	// 4. Determine which files still need uploading. A file whose
	// content-addressed key already exists on storage is byte-identical to what
	// is already there (that is what content-addressing guarantees), so it is
	// skipped — this is the incremental dedup, now keyed by content rather than
	// by name+size. Objects the new manifest will NOT reference are left in
	// place and removed by the post-manifest orphan prune (step 8).
	//
	// Existence is resolved with a single List of the checkpoint prefix rather
	// than one GetObject/HEAD per file: a full backup hashes hundreds/thousands
	// of SSTs and a per-file remote round trip would dominate the wall clock.
	// The List is done once and every computed content-addressed key is compared
	// against the returned set locally.
	existingKeys, err := listKeySet(ctx, storage, CheckpointPrefix(bucketID))
	if err != nil {
		return nil, fmt.Errorf("listing existing checkpoint objects: %w", err)
	}

	var toUpload []string

	for filename, cf := range checkpointFiles {
		if _, exists := existingKeys[cf.Key]; !exists {
			toUpload = append(toUpload, filename)
		}
	}

	logger.WithFields(map[string]any{
		"totalFiles": len(checkpointFiles),
		"toUpload":   len(toUpload),
	}).Infof("Backup diff computed")

	// 5. Upload the new/changed files.
	//
	// Crash-safety ordering (EN-888 / EN-1055): every object the new manifest
	// will reference is uploaded BEFORE the manifest is written, and NO stale
	// object is deleted before the new manifest is committed. Uploads are
	// content-addressed, so this NEVER overwrites an object the currently
	// published manifest still references (a changed file — e.g. a grown Pebble
	// MANIFEST — lands on a new key). Combined with deferring all deletion to
	// the post-manifest orphan prune (step 8), this makes the whole cycle
	// crash-safe: a crash at any point before WriteManifest leaves the previous
	// manifest fully restorable, and a crash after it leaves the new one
	// restorable — there is never a window where the current manifest points at
	// a half-written or deleted object.
	for _, filename := range toUpload {
		if err := uploadFile(ctx, storage, checkpointPath, checkpointFiles[filename].Key, filename); err != nil {
			return nil, err
		}
	}

	// 6. Read sequences from the checkpoint to record in manifest
	checkpointStore, err := dal.OpenReadOnly(checkpointPath, logger)
	if err != nil {
		return nil, fmt.Errorf("opening checkpoint for reading sequences: %w", err)
	}

	defer func() { _ = checkpointStore.Close() }()

	readHandle, handleErr := checkpointStore.NewDirectReadHandle()
	if handleErr != nil {
		return nil, fmt.Errorf("creating read handle: %w", handleErr)
	}

	defer func() { _ = readHandle.Close() }()

	lastAppliedIndex, err := query.ReadLastAppliedIndex(readHandle)
	if err != nil {
		return nil, fmt.Errorf("reading last applied index from checkpoint: %w", err)
	}

	lastLog, err := query.ReadLastLog(readHandle)
	if err != nil {
		return nil, fmt.Errorf("reading last log from checkpoint: %w", err)
	}

	var lastLogSeq uint64
	if lastLog != nil {
		lastLogSeq = lastLog.GetSequence()
	}

	lastAuditSeq, err := query.ReadLastAuditSequence(readHandle)
	if err != nil {
		return nil, fmt.Errorf("reading last audit sequence from checkpoint: %w", err)
	}

	// 7. Write updated manifest with new checkpoint and empty exports
	newManifest := &Manifest{
		Checkpoint: &CheckpointManifest{
			Timestamp:         time.Now().UTC().Format(time.RFC3339Nano),
			LastAppliedIndex:  lastAppliedIndex,
			LastLogSequence:   lastLogSeq,
			LastAuditSequence: lastAuditSeq,
			Files:             checkpointFiles,
		},
		Exports: nil,
	}

	if err := WriteManifest(ctx, storage, manifestKey, newManifest); err != nil {
		return nil, fmt.Errorf("writing manifest: %w", err)
	}

	// 8. Prune orphans now that the new manifest is committed. The manifest is
	// the authoritative inventory; anything under data/ or exports/ not in it is
	// dead weight — this includes both the stale checkpoint files this run
	// replaced and every export segment obsoleted by the new checkpoint, plus
	// files leaked by earlier runs that crashed before writing a manifest (the
	// diff step, which compares against the *previous manifest*, cannot reach
	// those). Running the prune strictly after WriteManifest is what makes the
	// cycle crash-safe: at no point is a still-referenced object deleted.
	expectedKeys := make(map[string]struct{}, len(checkpointFiles))
	for _, cf := range checkpointFiles {
		expectedKeys[cf.Key] = struct{}{}
	}

	deletedCheckpointKeys := pruneOrphans(ctx, logger, storage, CheckpointPrefix(bucketID), expectedKeys)
	// A full backup writes Exports: nil, so every export segment in storage is
	// now orphaned and can be removed. Export objects are never referenced by a
	// checkpoint manifest, so they always classify as orphans.
	deletedExportKeys := pruneOrphans(ctx, logger, storage, ExportPrefix(bucketID), nil)

	// Classify the checkpoint deletions: a key the PREVIOUS manifest referenced
	// is a stale checkpoint file this run superseded — that is ordinary churn and
	// is what BackupResponse.files_deleted reports. A key no manifest referenced
	// is a true orphan (leaked by a run that crashed before writing a manifest),
	// reported as orphans_deleted. Export deletions are always orphans.
	filesDeleted := 0
	orphansDeleted := len(deletedExportKeys)

	for _, key := range deletedCheckpointKeys {
		if _, wasReferenced := prevCheckpointKeys[key]; wasReferenced {
			filesDeleted++
		} else {
			orphansDeleted++
		}
	}

	duration := time.Since(start)

	logger.WithFields(map[string]any{
		"duration":          duration.String(),
		"uploaded":          len(toUpload),
		"filesDeleted":      filesDeleted,
		"orphansDeleted":    orphansDeleted,
		"total":             len(checkpointFiles),
		"lastLogSequence":   lastLogSeq,
		"lastAuditSequence": lastAuditSeq,
		"lastAppliedIndex":  lastAppliedIndex,
	}).Infof("Backup completed")

	return &Result{
		FilesUploaded: len(toUpload),
		// FilesDeleted counts stale checkpoint objects this run superseded — keys
		// the previous manifest referenced that the new manifest no longer does.
		// OrphansDeleted counts everything else the post-manifest prune removed:
		// obsolete export segments plus files leaked by earlier crashed runs.
		FilesDeleted:      filesDeleted,
		OrphansDeleted:    orphansDeleted,
		TotalFiles:        len(checkpointFiles),
		LastLogSequence:   lastLogSeq,
		LastAuditSequence: lastAuditSeq,
		LastAppliedIndex:  lastAppliedIndex,
		Duration:          duration,
	}, nil
}

// pruneOrphanExports removes every object under exports/ that is not listed in
// the manifest's export set. Used by RunIncrementalBackup both in its no-op
// path (nothing new to export, but old garbage may still be sitting there) and
// after writing a new manifest. Returns the number of objects deleted.
func pruneOrphanExports(ctx context.Context, logger logging.Logger, storage Storage, bucketID string, exports []ExportSegment) int {
	expectedKeys := make(map[string]struct{}, len(exports))
	for _, seg := range exports {
		expectedKeys[seg.Key] = struct{}{}
	}

	return len(pruneOrphans(ctx, logger, storage, ExportPrefix(bucketID), expectedKeys))
}

// pruneOrphans lists every object under prefix and deletes any whose key is not
// in expectedKeys. A nil or empty expectedKeys deletes every object under the
// prefix. It returns the keys it successfully deleted so callers can classify
// them (e.g. stale-vs-orphan). Failures are logged and skipped (a transient
// List or Delete error must never fail the surrounding backup — the manifest is
// already committed and the next run will retry).
func pruneOrphans(ctx context.Context, logger logging.Logger, storage Storage, prefix string, expectedKeys map[string]struct{}) []string {
	keys, err := storage.ListFiles(ctx, prefix)
	if err != nil {
		logger.WithFields(map[string]any{
			"prefix": prefix,
			"error":  err,
		}).Errorf("Failed to list backup objects for orphan prune (non-fatal)")

		return nil
	}

	var deleted []string

	for _, key := range keys {
		if _, kept := expectedKeys[key]; kept {
			continue
		}

		if err := storage.DeleteFile(ctx, key); err != nil {
			logger.WithFields(map[string]any{
				"key":   key,
				"error": err,
			}).Errorf("Failed to delete orphan backup file (non-fatal)")

			continue
		}

		deleted = append(deleted, key)
	}

	return deleted
}

// checkpointKeySet returns the set of content-addressed checkpoint object keys a
// manifest references. Used to classify prune deletions: a deleted key present
// here was a live checkpoint file the manifest owned. A nil or checkpoint-less
// manifest yields an empty set.
func checkpointKeySet(manifest *Manifest) map[string]struct{} {
	if manifest == nil || manifest.Checkpoint == nil {
		return map[string]struct{}{}
	}

	keys := make(map[string]struct{}, len(manifest.Checkpoint.Files))
	for _, cf := range manifest.Checkpoint.Files {
		keys[cf.Key] = struct{}{}
	}

	return keys
}

// listKeySet lists every object under prefix once and returns their keys as a
// set for local membership tests. This replaces a per-file remote existence
// probe (one GetObject/HEAD each) with a single List when many keys must be
// checked against storage.
func listKeySet(ctx context.Context, storage Storage, prefix string) (map[string]struct{}, error) {
	keys, err := storage.ListFiles(ctx, prefix)
	if err != nil {
		return nil, err
	}

	set := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		set[key] = struct{}{}
	}

	return set, nil
}

// RunIncrementalBackup exports new log and audit entries since the last backup.
// It reads the manifest to determine the starting sequences, streams new entries
// as KV stream segments to S3, and updates the manifest.
//
// maxSegmentBytes caps the on-storage size of each export segment; a range
// larger than that splits into multiple segments. 0 selects the default
// (maxExportSegmentBytes).
func RunIncrementalBackup(
	ctx context.Context,
	logger logging.Logger,
	store *dal.Store,
	storage Storage,
	bucketID string,
	maxSegmentBytes int64,
) (*IncrementalBackupResult, error) {
	start := time.Now()

	if maxSegmentBytes <= 0 {
		maxSegmentBytes = maxExportSegmentBytes
	}

	manifestKey := ManifestKey(bucketID)

	// 1. Read existing manifest (empty if first run)
	manifest, err := ReadManifestOrEmpty(ctx, logger, storage, manifestKey)
	if err != nil {
		return nil, err
	}

	// 2. Take a point-in-time snapshot for consistent reads
	readHandle, err := store.NewReadHandle()
	if err != nil {
		return nil, fmt.Errorf("creating read handle: %w", err)
	}

	defer func() { _ = readHandle.Close() }()

	// 3. Determine starting sequences
	afterLogSeq := manifest.LastExportLogSequence()
	afterAuditSeq := manifest.LastExportAuditSequence()

	// 4. Read current last sequences
	currentLastLog, err := query.ReadLastLog(readHandle)
	if err != nil {
		return nil, fmt.Errorf("reading current last log: %w", err)
	}

	var currentLogSeq uint64
	if currentLastLog != nil {
		currentLogSeq = currentLastLog.GetSequence()
	}

	currentAuditSeq, err := query.ReadLastAuditSequence(readHandle)
	if err != nil {
		return nil, fmt.Errorf("reading current last audit sequence: %w", err)
	}

	// Ensure monotonicity: after a RestoreCheckpoint (leadership change +
	// snapshot from new leader), Pebble may have a lower cold-zone sequence
	// than what the manifest already recorded from a previous export.
	// Never regress below the manifest.
	currentLogSeq = max(currentLogSeq, afterLogSeq)
	currentAuditSeq = max(currentAuditSeq, afterAuditSeq)

	// 5. Check if there's anything new
	if currentLogSeq <= afterLogSeq && currentAuditSeq <= afterAuditSeq {
		logger.Infof("No new entries to export")

		// Still prune orphan exports — dangling segments are independent of
		// whether new entries arrived since the previous run.
		orphansDeleted := pruneOrphanExports(ctx, logger, storage, bucketID, manifest.Exports)

		return &IncrementalBackupResult{
			Duration:          time.Since(start),
			LastLogSequence:   afterLogSeq,
			LastAuditSequence: afterAuditSeq,
			OrphansDeleted:    orphansDeleted,
		}, nil
	}

	var (
		logEntriesExported   uint64
		auditEntriesExported uint64
		segmentsUploaded     int
	)

	// 6. Export new log entries
	if currentLogSeq > afterLogSeq {
		segs, count, err := exportEntries(
			ctx, storage, readHandle,
			dal.ZoneCold, dal.SubColdLog, afterLogSeq, currentLogSeq, "log",
			func(part int) string { return ExportLogSegmentKey(bucketID, afterLogSeq+1, currentLogSeq, part) },
			maxSegmentBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("exporting log entries: %w", err)
		}

		logEntriesExported = count
		manifest.Exports = append(manifest.Exports, segs...)
		segmentsUploaded += len(segs)
	}

	// 7. Export new audit entries
	if currentAuditSeq > afterAuditSeq {
		segs, count, err := exportEntries(
			ctx, storage, readHandle,
			dal.ZoneCold, dal.SubColdAudit, afterAuditSeq, currentAuditSeq, "audit",
			func(part int) string { return ExportAuditSegmentKey(bucketID, afterAuditSeq+1, currentAuditSeq, part) },
			maxSegmentBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("exporting audit entries: %w", err)
		}

		auditEntriesExported = count
		manifest.Exports = append(manifest.Exports, segs...)
		segmentsUploaded += len(segs)

		// Export the audit items (per-order detail) for the same range.
		// On success proposals the audit hash covers the per-item payloads,
		// so a restored backup missing them cannot reconstruct the chain.
		// Failure proposals write an AuditEntry with zero items (see
		// state.machine.go writeAuditEntry(failureEntry, nil, ...) and
		// state.batch.go appendAuditItems), and their hash is bound to the
		// header alone. An incremental range consisting of only failures
		// therefore has audit count > 0 but auditItem count == 0 —
		// exportEntries then returns no segments, so appending its result
		// adds nothing and we never reference a key that does not exist on
		// storage (subsequent ApplyExports would fail on GetFile). Same
		// guard as the appliedProposal branch below.
		itemSegs, _, err := exportEntries(
			ctx, storage, readHandle,
			dal.ZoneCold, dal.SubColdAuditItem, afterAuditSeq, currentAuditSeq, "auditItem",
			func(part int) string {
				return ExportAuditItemSegmentKey(bucketID, afterAuditSeq+1, currentAuditSeq, part)
			},
			maxSegmentBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("exporting audit items: %w", err)
		}

		manifest.Exports = append(manifest.Exports, itemSegs...)
		segmentsUploaded += len(itemSegs)

		// Export the AppliedProposal entries for the same range. Sequences
		// are 1:1 with AuditEntry on the success path; ranges that contain
		// only failed proposals carry NO AppliedProposal entries. exportEntries
		// then returns no segments — we must not add a manifest entry
		// referencing a key that does not exist on storage, or a subsequent
		// ApplyExports will fail on GetFile.
		appliedSegs, _, err := exportEntries(
			ctx, storage, readHandle,
			dal.ZoneCold, dal.SubColdAppliedProposal, afterAuditSeq, currentAuditSeq, "appliedProposal",
			func(part int) string {
				return ExportAppliedProposalSegmentKey(bucketID, afterAuditSeq+1, currentAuditSeq, part)
			},
			maxSegmentBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("exporting applied proposals: %w", err)
		}

		manifest.Exports = append(manifest.Exports, appliedSegs...)
		segmentsUploaded += len(appliedSegs)
	}

	// 8. Write updated manifest
	if err := WriteManifest(ctx, storage, manifestKey, manifest); err != nil {
		return nil, fmt.Errorf("writing manifest: %w", err)
	}

	// 9. Prune orphan export segments — anything under exports/ that the manifest
	// no longer references (typically leaked by earlier failed incremental runs).
	// Checkpoint files under data/ are owned by the full backup and untouched here.
	orphansDeleted := pruneOrphanExports(ctx, logger, storage, bucketID, manifest.Exports)

	duration := time.Since(start)

	logger.WithFields(map[string]any{
		"duration":             duration.String(),
		"logEntriesExported":   logEntriesExported,
		"auditEntriesExported": auditEntriesExported,
		"segmentsUploaded":     segmentsUploaded,
		"orphansDeleted":       orphansDeleted,
		"lastLogSequence":      currentLogSeq,
		"lastAuditSequence":    currentAuditSeq,
	}).Infof("Incremental backup completed")

	return &IncrementalBackupResult{
		LogEntriesExported:   logEntriesExported,
		AuditEntriesExported: auditEntriesExported,
		SegmentsUploaded:     segmentsUploaded,
		OrphansDeleted:       orphansDeleted,
		Duration:             duration,
		LastLogSequence:      currentLogSeq,
		LastAuditSequence:    currentAuditSeq,
	}, nil
}

// maxExportSegmentBytes bounds the on-storage size of a single export object.
// A range whose serialized entries exceed this is split into multiple segments
// at sequence boundaries. This keeps each S3 object well under the multipart
// ceiling, shrinks the retry blast radius, and lets a restore apply parts
// independently. It is not the S3 single-PutObject limit — the S3 backend
// uploads via multipart, so any single part could in principle reach 5 TB.
const maxExportSegmentBytes = 4 << 30 // 4 GiB

// exportEntries streams entries for a given prefix from (afterSeq, endSeq] into
// one or more KV stream segments uploaded to storage, starting a new segment
// whenever the current one reaches maxSegmentBytes (splitting only at sequence
// boundaries, so a sequence's keys never straddle two segments). It returns the
// segments written — empty when the range holds no entries — and the total
// number of entries exported. Each segment is streamed through an io.Pipe so
// memory stays bounded regardless of range size.
func exportEntries(
	ctx context.Context,
	storage Storage,
	reader dal.PebbleReader,
	zone, sub byte,
	afterSeq, endSeq uint64,
	segType string,
	keyFn func(part int) string,
	maxSegmentBytes int64,
) ([]ExportSegment, uint64, error) {
	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(zone, sub)

	if afterSeq > 0 {
		kb.PutUint64(afterSeq + 1)
	}

	lowerBound := kb.Build()

	kb2 := dal.NewKeyBuilder()
	kb2.PutZonePrefix(zone, sub).PutUint64(endSeq + 1)
	upperBound := kb2.Build()

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, 0, fmt.Errorf("creating iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	iter.First()
	if err := iter.Error(); err != nil {
		return nil, 0, fmt.Errorf("iterating %s: %w", segType, err)
	}

	var (
		segments   []ExportSegment
		totalCount uint64
		part       int
	)

	for iter.Valid() {
		startSeq := seqFromKey(iter.Key())
		key := keyFn(part)

		endSeqPart, count, size, err := uploadSegmentPart(ctx, storage, key, iter, maxSegmentBytes)
		if err != nil {
			return nil, 0, err
		}

		// A part that consumed no entry cannot advance the iterator, so the loop
		// would spin forever. iter.Valid() above guarantees at least one entry,
		// so a zero count is impossible by design and must fail loudly.
		if count == 0 {
			return nil, 0, fmt.Errorf("invariant: export segment %s consumed no entries", key)
		}

		segments = append(segments, ExportSegment{
			Type:     segType,
			StartSeq: startSeq,
			EndSeq:   endSeqPart,
			Key:      key,
			Size:     size,
		})
		totalCount += count
		part++
	}

	return segments, totalCount, nil
}

// uploadSegmentPart streams one KV segment from the iterator's current position
// through an io.Pipe into storage.PutFile. It writes entries until the segment
// reaches maxSegmentBytes at a sequence boundary or the iterator is exhausted,
// leaving the iterator positioned at the first entry of the next segment (or
// invalid). It returns the last sequence written, the entry count, and the
// segment's on-storage byte size.
func uploadSegmentPart(
	ctx context.Context,
	storage Storage,
	key string,
	iter *pebble.Iterator,
	maxSegmentBytes int64,
) (endSeq, count uint64, size int64, err error) {
	pr, pw := io.Pipe()

	type result struct {
		endSeq, count uint64
		size          int64
		err           error
	}

	resCh := make(chan result, 1)

	go func() {
		cw := &countingWriter{w: pw}
		writer := NewKVStreamWriter(cw)

		var r result

		r.err = func() error {
			if err := writer.WriteHeader(); err != nil {
				return err
			}

			for iter.Valid() {
				value, err := iter.ValueAndErr()
				if err != nil {
					return fmt.Errorf("reading value: %w", err)
				}

				k := iter.Key()
				if err := writer.WriteEntry(k, value); err != nil {
					return err
				}

				r.count++
				r.endSeq = seqFromKey(k)

				iter.Next()

				// Split once the size cap is reached, but only when the next
				// entry begins a new sequence — a sequence's keys (audit items
				// share one) must never straddle two segments.
				if cw.n >= maxSegmentBytes && iter.Valid() && seqFromKey(iter.Key()) != r.endSeq {
					break
				}
			}

			if err := iter.Error(); err != nil {
				return fmt.Errorf("iterating: %w", err)
			}

			return writer.WriteFooter()
		}()

		r.size = cw.n
		_ = pw.CloseWithError(r.err)
		resCh <- r
	}()

	uploadErr := storage.PutFile(ctx, key, pr, -1)
	if uploadErr != nil {
		// Unblock the writer goroutine if the uploader stopped reading early.
		_ = pr.CloseWithError(uploadErr)
	}

	r := <-resCh

	if r.err != nil {
		return 0, 0, 0, fmt.Errorf("writing segment %s: %w", key, r.err)
	}

	if uploadErr != nil {
		return 0, 0, 0, fmt.Errorf("uploading segment %s: %w", key, uploadErr)
	}

	return r.endSeq, r.count, r.size, nil
}

// seqFromKey extracts the 8-byte big-endian sequence that follows the 2-byte
// [zone][sub] prefix of a cold-zone key. Every key exportEntries iterates is
// built with PutZonePrefix followed by PutUint64, so it is always long enough.
func seqFromKey(key []byte) uint64 {
	return binary.BigEndian.Uint64(key[2:10])
}

// countingWriter counts the bytes written through it, used to size and split
// export segments as they stream.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)

	return n, err
}

func uploadFile(ctx context.Context, storage Storage, checkpointPath, key, filename string) error {
	localPath := filepath.Join(checkpointPath, filepath.FromSlash(filename))

	file, err := os.Open(localPath)
	if err != nil {
		return fmt.Errorf("opening %s for upload: %w", filename, err)
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()

		return fmt.Errorf("stat %s: %w", filename, err)
	}

	err = storage.PutFile(ctx, key, file, info.Size())
	_ = file.Close()

	if err != nil {
		return fmt.Errorf("uploading %s: %w", filename, err)
	}

	return nil
}

// listCheckpointFiles walks the checkpoint directory and returns, per local
// filename, its size and content-addressed storage key. Each file is hashed
// (sha256 over its bytes) so its key embeds the content: a file whose bytes
// change between checkpoints — notably Pebble's same-named MANIFEST-NNNNNN that
// grows in place — maps to a different key and is uploaded as a new object
// instead of overwriting the one the currently published manifest references.
func listCheckpointFiles(bucketID, dir string) (map[string]CheckpointFile, error) {
	files := make(map[string]CheckpointFile)

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}

		hash, err := hashFile(path)
		if err != nil {
			return fmt.Errorf("hashing %s: %w", relPath, err)
		}

		// Normalize to forward slashes for consistent keys across platforms.
		name := filepath.ToSlash(relPath)
		files[name] = CheckpointFile{
			Size: info.Size(),
			Key:  CheckpointFileKey(bucketID, name, hash),
		}

		return nil
	})

	return files, err
}

// hashFile returns the hex-encoded sha256 of a file's contents, streamed so
// memory stays bounded regardless of file size.
func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}

	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
