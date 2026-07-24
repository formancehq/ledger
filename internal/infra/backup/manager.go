package backup

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

	// 2. Read the existing manifest FIRST. It is used for three things: reusing
	// prior per-file hashes so unchanged immutable files are not re-hashed
	// (step 4), logging the upload delta, and classifying which objects the
	// post-manifest prune (step 8) removes — objects the previous manifest
	// referenced are stale checkpoint files this run superseded (normal churn,
	// counted as FilesDeleted), objects it did not are true orphans leaked by a
	// crashed run (OrphansDeleted). Correctness of the upload no longer depends
	// on diffing sizes — the content-addressed key is the diff.
	// A legacy pre-content-addressing manifest is NOT fatal here: a full backup
	// overwrites the manifest wholesale and never diffs against it, so retaking a
	// full backup with the current binary is exactly the documented recovery path
	// out of a legacy manifest. Treat it as a warning and proceed with no
	// previous-manifest keys (so its objects, which live under bare data/<name>
	// keys, all classify as orphans on prune, and no hash reuse triggers). (The
	// incremental path, which depends on the existing manifest, keeps the error
	// fatal — see RunIncrementalBackup.)
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
	prevExportKeys := exportKeySet(prevManifest)

	// 3. List the objects already present under the checkpoint prefix with a
	// single List (not a HEAD per file: a full backup has thousands of SSTs and
	// a per-file round trip would dominate wall clock). This set is used twice:
	// as the guard that lets a prior file's hash be reused (a reused key must
	// still be present remotely — see listCheckpointFiles), and as the upload
	// dedup (a file whose content-addressed key already exists is byte-identical
	// and skipped).
	existingKeys, err := listKeySet(ctx, storage, CheckpointPrefix(bucketID))
	if err != nil {
		return nil, fmt.Errorf("listing existing checkpoint objects: %w", err)
	}

	// 4. Enumerate the checkpoint files, computing each file's content-addressed
	// key. Unchanged immutable files (.sst/.blob still present remotely under
	// the prior manifest's key with the same size+mtime) reuse that key instead
	// of being re-hashed — the dominant cost on a large store where almost every
	// SST is unchanged between runs.
	checkpointFiles, err := listCheckpointFiles(ctx, bucketID, checkpointPath, prevManifest, existingKeys)
	if err != nil {
		return nil, fmt.Errorf("listing checkpoint files: %w", err)
	}

	// 5. Determine which files still need uploading — those whose
	// content-addressed key is not already on storage. Objects the new manifest
	// will NOT reference are left in place and removed by the post-manifest
	// orphan prune (step 8).
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
		if err := uploadFile(ctx, logger, storage, checkpointPath, checkpointFiles[filename].Key, filename); err != nil {
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

	if err := writeManifestWithRetry(ctx, storage, manifestKey, newManifest, logger); err != nil {
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
	// removed here. When this full backup follows one or more incrementals, the
	// previous committed manifest referenced those export segments as live
	// objects now rolled up into the new checkpoint — deleting them is ordinary
	// supersede churn (files_deleted), not a failed-run leftover (orphans_deleted).
	deletedExportKeys := pruneOrphans(ctx, logger, storage, ExportPrefix(bucketID), nil)

	// Classify every deletion: a key the PREVIOUS manifest referenced — as a
	// checkpoint file OR an export segment — is something this run superseded,
	// ordinary churn reported as BackupResponse.files_deleted. A key no manifest
	// referenced is a true orphan (leaked by a run that crashed before writing a
	// manifest), reported as orphans_deleted.
	filesDeleted := 0
	orphansDeleted := 0

	for _, key := range deletedCheckpointKeys {
		if _, wasReferenced := prevCheckpointKeys[key]; wasReferenced {
			filesDeleted++
		} else {
			orphansDeleted++
		}
	}

	for _, key := range deletedExportKeys {
		if _, wasReferenced := prevExportKeys[key]; wasReferenced {
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
		// FilesDeleted counts objects this run superseded — checkpoint files and
		// export segments the previous manifest referenced that the new manifest
		// no longer does (normal roll-up churn). OrphansDeleted counts everything
		// else the post-manifest prune removed: files leaked by earlier runs that
		// crashed before writing a manifest.
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

// exportKeySet returns the set of export-segment object keys a manifest
// references. Used to classify prune deletions: a deleted export key present
// here was a live segment the previous manifest owned that a new full
// checkpoint superseded — normal roll-up churn, not an orphan. A nil or
// export-less manifest yields an empty set.
func exportKeySet(manifest *Manifest) map[string]struct{} {
	if manifest == nil {
		return map[string]struct{}{}
	}

	keys := make(map[string]struct{}, len(manifest.Exports))
	for _, seg := range manifest.Exports {
		keys[seg.Key] = struct{}{}
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

	// An incremental backup is only meaningful layered on top of a full
	// checkpoint. A checkpoint-less manifest — a fresh/empty destination, or an
	// export-only manifest — carries none of the Global-zone persisted config,
	// last-applied index, or timestamp that restore needs to reconstruct a store,
	// so publishing export segments against it would leave an artifact restore
	// cannot use (restore validation fails on the missing persisted config). Fail
	// fast here, BEFORE taking a snapshot or uploading any segment, so no export
	// artifact is published for an unrestorable base.
	if manifest.Checkpoint == nil {
		return nil, ErrNoFullCheckpoint
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
			ctx, logger, storage, readHandle,
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
			ctx, logger, storage, readHandle,
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
			ctx, logger, storage, readHandle,
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
			ctx, logger, storage, readHandle,
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
	if err := writeManifestWithRetry(ctx, storage, manifestKey, manifest, logger); err != nil {
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
	logger logging.Logger,
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

		endSeqPart, count, size, err := uploadSegmentPart(ctx, logger, storage, key, iter, maxSegmentBytes)
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
// through an io.Pipe into storage.PutFile, with bounded upload retry. It writes
// entries until the segment reaches maxSegmentBytes at a sequence boundary or
// the iterator is exhausted, leaving the iterator positioned at the first entry
// of the next segment (or invalid). It returns the last sequence written, the
// entry count, and the segment's on-storage byte size.
//
// Retry replays the segment from its start key: a pipe body is single-use, so a
// failed upload must re-stream. The iterator is re-seeked at the top of each
// attempt, which is safe because streamSegment joins its writer goroutine before
// returning — Pebble iterators are not safe for concurrent use.
func uploadSegmentPart(
	ctx context.Context,
	logger logging.Logger,
	storage Storage,
	key string,
	iter *pebble.Iterator,
	maxSegmentBytes int64,
) (endSeq, count uint64, size int64, err error) {
	// iter is Valid here (exportEntries guarantees it). Capture the segment's
	// first key so each retry attempt can replay the same range.
	startKey := bytes.Clone(iter.Key())

	var (
		endSeqOut, countOut uint64
		sizeOut             int64
	)

	retryErr := retryUpload(ctx, key, logger, func() error {
		iter.SeekGE(startKey)
		if err := iter.Error(); err != nil {
			return fmt.Errorf("seeking segment start: %w", err)
		}

		if !iter.Valid() || !bytes.Equal(iter.Key(), startKey) {
			return fmt.Errorf("invariant: iterator lost segment start on retry of %s", key)
		}

		e, c, s, streamErr := streamSegment(ctx, storage, key, iter, maxSegmentBytes)
		endSeqOut, countOut, sizeOut = e, c, s

		return streamErr
	})
	if retryErr != nil {
		return 0, 0, 0, retryErr
	}

	return endSeqOut, countOut, sizeOut, nil
}

// streamSegment performs one attempt of uploadSegmentPart: it streams KV
// entries from the iterator's current position through an io.Pipe into
// storage.PutFile, and joins the writer goroutine before returning so the
// iterator is safe to re-seek and reuse afterwards.
func streamSegment(
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

func uploadFile(ctx context.Context, logger logging.Logger, storage Storage, checkpointPath, key, filename string) error {
	localPath := filepath.Join(checkpointPath, filepath.FromSlash(filename))

	// Stat once for the size hint. The body itself is (re-)opened per attempt
	// by putWithRetry so a retried upload always streams from the start of the
	// file — an *os.File consumed by a failed multipart upload is single-use.
	info, err := os.Stat(localPath)
	if err != nil {
		return fmt.Errorf("stat %s: %w", filename, err)
	}

	err = putWithRetry(ctx, storage, key, info.Size(), logger, func() (io.Reader, func(), error) {
		f, openErr := os.Open(localPath)
		if openErr != nil {
			return nil, func() {}, fmt.Errorf("opening %s for upload: %w", filename, openErr)
		}

		return f, func() { _ = f.Close() }, nil
	})
	if err != nil {
		return fmt.Errorf("uploading %s: %w", filename, err)
	}

	return nil
}

// listCheckpointFiles walks the checkpoint directory and returns, per local
// filename, its size, mtime, and content-addressed storage key. A file's key
// embeds a sha256 of its bytes so it is immutable per content: a file whose
// bytes change between checkpoints — notably Pebble's same-named MANIFEST-NNNNNN
// that grows in place — maps to a different key and is uploaded as a new object
// instead of overwriting the one the currently published manifest references.
//
// Hashing every file is the dominant cost on a large store, so an unchanged
// immutable file (.sst/.blob still present remotely under the prior manifest's
// key with the same size+mtime) reuses that key instead of being re-hashed —
// see reusePriorKey. prev/existingKeys may be empty (first backup, legacy
// manifest), in which case every file is hashed.
func listCheckpointFiles(ctx context.Context, bucketID, dir string, prev *Manifest, existingKeys map[string]struct{}) (map[string]CheckpointFile, error) {
	files := make(map[string]CheckpointFile)

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Abort promptly on cancellation: hashing a multi-TiB store is an
		// otherwise-uninterruptible phase that holds the destination lock.
		if err := ctx.Err(); err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(dir, path)
		if err != nil {
			return err
		}

		// Normalize to forward slashes for consistent keys across platforms.
		name := filepath.ToSlash(relPath)

		if key, ok := reusePriorKey(name, info, prev, existingKeys); ok {
			files[name] = CheckpointFile{
				Size:            info.Size(),
				Key:             key,
				ModTimeUnixNano: info.ModTime().UnixNano(),
			}

			return nil
		}

		hash, err := hashFileFn(ctx, path)
		if err != nil {
			return fmt.Errorf("hashing %s: %w", relPath, err)
		}

		files[name] = CheckpointFile{
			Size:            info.Size(),
			Key:             CheckpointFileKey(bucketID, name, hash),
			ModTimeUnixNano: info.ModTime().UnixNano(),
		}

		return nil
	})

	return files, err
}

// reusePriorKey reports whether name's prior content-addressed key can be
// reused (skipping the sha256 re-hash), returning it when so. Reuse is sound
// only for a file byte-identical to the previous backup, which requires ALL of:
//
//   - immutable-by-name: a Pebble .sst or .blob, never rewritten in place and
//     whose file number is never reused within a store lineage. Metadata files
//     (MANIFEST-*, OPTIONS-*, CURRENT, marker.*) change under a stable name and
//     are always re-hashed;
//   - the previous manifest recorded the same name with the same size AND the
//     same mtime. mtime is the lineage discriminator: RestoreCheckpoint swaps in
//     freshly-written files, so a restored file with a colliding number+size
//     gets a new mtime and is re-hashed rather than falsely reused;
//   - the reused key is still present on storage. Otherwise a missing object
//     would be re-uploaded under a stale hash-bearing key that no longer matches
//     current bytes, corrupting content-addressing — fall through to a re-hash.
func reusePriorKey(name string, info os.FileInfo, prev *Manifest, existingKeys map[string]struct{}) (string, bool) {
	if prev == nil || prev.Checkpoint == nil {
		return "", false
	}

	if !isImmutableCheckpointFile(name) {
		return "", false
	}

	pf, ok := prev.Checkpoint.Files[name]
	if !ok {
		return "", false
	}

	if pf.Size != info.Size() || pf.ModTimeUnixNano != info.ModTime().UnixNano() {
		return "", false
	}

	if _, present := existingKeys[pf.Key]; !present {
		return "", false
	}

	return pf.Key, true
}

// isImmutableCheckpointFile reports whether a checkpoint file is one Pebble
// never mutates in place (an SST or a value-separation blob), and is therefore
// safe to identify by name+size+mtime for hash reuse.
func isImmutableCheckpointFile(name string) bool {
	return strings.HasSuffix(name, ".sst") || strings.HasSuffix(name, ".blob")
}

// hashFileFn is the hasher listCheckpointFiles uses, indirected through a
// package variable so tests can count invocations to assert re-hash skipping.
var hashFileFn = hashFile

// hashFile returns the hex-encoded sha256 of a file's contents, streamed so
// memory stays bounded regardless of file size. The read is wrapped so a long
// hash of a multi-GB file aborts promptly on context cancellation.
func hashFile(ctx context.Context, path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}

	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, &ctxReader{ctx: ctx, r: f}); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// ctxReader aborts a streaming read when ctx is cancelled: io.Copy would
// otherwise run a multi-GB hash to completion, ignoring a cancelled backup.
type ctxReader struct {
	ctx context.Context
	r   io.Reader
}

func (c *ctxReader) Read(p []byte) (int, error) {
	if err := c.ctx.Err(); err != nil {
		return 0, err
	}

	return c.r.Read(p)
}
