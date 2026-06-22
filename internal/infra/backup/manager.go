package backup

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

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

	// 2. List files in checkpoint
	localFiles, err := listCheckpointFiles(checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("listing checkpoint files: %w", err)
	}

	// 3. Read existing manifest
	existingManifest, err := ReadManifestOrEmpty(ctx, logger, storage, manifestKey)
	if err != nil {
		return nil, err
	}

	// 4. Compute diff against previous checkpoint files
	var previousFiles map[string]int64
	if existingManifest.Checkpoint != nil {
		previousFiles = existingManifest.Checkpoint.Files
	} else {
		previousFiles = make(map[string]int64)
	}

	toUpload, toDelete := diffFiles(localFiles, previousFiles)

	logger.WithFields(map[string]any{
		"totalFiles": len(localFiles),
		"toUpload":   len(toUpload),
		"toDelete":   len(toDelete),
	}).Infof("Backup diff computed")

	// 5. Upload new/changed files
	for _, filename := range toUpload {
		if err := uploadFile(ctx, storage, checkpointPath, CheckpointFileKey(bucketID, filename), filename); err != nil {
			return nil, err
		}
	}

	// 6. Delete stale checkpoint files from storage
	for _, filename := range toDelete {
		if err := storage.DeleteFile(ctx, CheckpointFileKey(bucketID, filename)); err != nil {
			logger.WithFields(map[string]any{
				"file":  filename,
				"error": err,
			}).Errorf("Failed to delete stale backup file (non-fatal)")
		}
	}

	// 7. Clean up old exports (they are obsolete after a new checkpoint)
	for _, seg := range existingManifest.Exports {
		if err := storage.DeleteFile(ctx, seg.Key); err != nil {
			logger.WithFields(map[string]any{
				"segment": seg.Key,
				"error":   err,
			}).Errorf("Failed to delete stale export segment (non-fatal)")
		}
	}

	// 8. Read sequences from the checkpoint to record in manifest
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

	// 9. Write updated manifest with new checkpoint and empty exports
	newManifest := &Manifest{
		Checkpoint: &CheckpointManifest{
			Timestamp:         time.Now().UTC().Format(time.RFC3339Nano),
			LastAppliedIndex:  lastAppliedIndex,
			LastLogSequence:   lastLogSeq,
			LastAuditSequence: lastAuditSeq,
			Files:             localFiles,
		},
		Exports: nil,
	}

	if err := WriteManifest(ctx, storage, manifestKey, newManifest); err != nil {
		return nil, fmt.Errorf("writing manifest: %w", err)
	}

	// 10. Prune orphans left behind by earlier failed runs. The manifest is the
	// authoritative inventory; anything under data/ or exports/ not in it is dead
	// weight. Runs that crashed before writing a manifest leak files the diff
	// step (which compares against the *previous manifest*) cannot reach.
	expectedKeys := make(map[string]struct{}, len(localFiles))
	for filename := range localFiles {
		expectedKeys[CheckpointFileKey(bucketID, filename)] = struct{}{}
	}

	orphansDeleted := pruneOrphans(ctx, logger, storage, CheckpointPrefix(bucketID), expectedKeys)
	// A full backup writes Exports: nil, so every export segment in storage is
	// now orphaned and can be removed.
	orphansDeleted += pruneOrphans(ctx, logger, storage, ExportPrefix(bucketID), nil)

	duration := time.Since(start)

	logger.WithFields(map[string]any{
		"duration":          duration.String(),
		"uploaded":          len(toUpload),
		"deleted":           len(toDelete),
		"orphansDeleted":    orphansDeleted,
		"total":             len(localFiles),
		"lastLogSequence":   lastLogSeq,
		"lastAuditSequence": lastAuditSeq,
		"lastAppliedIndex":  lastAppliedIndex,
	}).Infof("Backup completed")

	return &Result{
		FilesUploaded:     len(toUpload),
		FilesDeleted:      len(toDelete),
		OrphansDeleted:    orphansDeleted,
		TotalFiles:        len(localFiles),
		LastLogSequence:   lastLogSeq,
		LastAuditSequence: lastAuditSeq,
		LastAppliedIndex:  lastAppliedIndex,
		Duration:          duration,
	}, nil
}

// pruneOrphanExports removes every object under exports/ that is not listed in
// the manifest's export set. Used by RunIncrementalBackup both in its no-op
// path (nothing new to export, but old garbage may still be sitting there) and
// after writing a new manifest.
func pruneOrphanExports(ctx context.Context, logger logging.Logger, storage Storage, bucketID string, exports []ExportSegment) int {
	expectedKeys := make(map[string]struct{}, len(exports))
	for _, seg := range exports {
		expectedKeys[seg.Key] = struct{}{}
	}

	return pruneOrphans(ctx, logger, storage, ExportPrefix(bucketID), expectedKeys)
}

// pruneOrphans lists every object under prefix and deletes any whose key is not
// in expectedKeys. A nil or empty expectedKeys deletes every object under the
// prefix. Failures are logged and counted as non-orphan (a transient List or
// Delete error must never fail the surrounding backup — the manifest is already
// committed and the next run will retry).
func pruneOrphans(ctx context.Context, logger logging.Logger, storage Storage, prefix string, expectedKeys map[string]struct{}) int {
	keys, err := storage.ListFiles(ctx, prefix)
	if err != nil {
		logger.WithFields(map[string]any{
			"prefix": prefix,
			"error":  err,
		}).Errorf("Failed to list backup objects for orphan prune (non-fatal)")

		return 0
	}

	deleted := 0

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

		deleted++
	}

	return deleted
}

// RunIncrementalBackup exports new log and audit entries since the last backup.
// It reads the manifest to determine the starting sequences, streams new entries
// as KV stream segments to S3, and updates the manifest.
func RunIncrementalBackup(
	ctx context.Context,
	logger logging.Logger,
	store *dal.Store,
	storage Storage,
	bucketID string,
) (*IncrementalBackupResult, error) {
	start := time.Now()

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
		count, err := exportEntries(
			ctx, storage, readHandle, bucketID,
			dal.ZoneCold, dal.SubColdLog, afterLogSeq, currentLogSeq,
			ExportLogSegmentKey(bucketID, afterLogSeq+1, currentLogSeq),
		)
		if err != nil {
			return nil, fmt.Errorf("exporting log entries: %w", err)
		}

		logEntriesExported = count

		manifest.Exports = append(manifest.Exports, ExportSegment{
			Type:     "log",
			StartSeq: afterLogSeq + 1,
			EndSeq:   currentLogSeq,
			Key:      ExportLogSegmentKey(bucketID, afterLogSeq+1, currentLogSeq),
		})

		segmentsUploaded++
	}

	// 7. Export new audit entries
	if currentAuditSeq > afterAuditSeq {
		count, err := exportEntries(
			ctx, storage, readHandle, bucketID,
			dal.ZoneCold, dal.SubColdAudit, afterAuditSeq, currentAuditSeq,
			ExportAuditSegmentKey(bucketID, afterAuditSeq+1, currentAuditSeq),
		)
		if err != nil {
			return nil, fmt.Errorf("exporting audit entries: %w", err)
		}

		auditEntriesExported = count

		manifest.Exports = append(manifest.Exports, ExportSegment{
			Type:     "audit",
			StartSeq: afterAuditSeq + 1,
			EndSeq:   currentAuditSeq,
			Key:      ExportAuditSegmentKey(bucketID, afterAuditSeq+1, currentAuditSeq),
		})

		segmentsUploaded++

		// Export the audit items (per-order detail) for the same range.
		// The audit hash is computed over these orders, so a restored
		// incremental backup that lacks them cannot reconstruct the hash
		// chain (the checker fails at the first restored audit sequence).
		// Items share the audit sequence range but live in a separate
		// subzone; their composite [seq][order_idx] keys fall within the
		// same [seq+1, endSeq+1) prefix bounds exportEntries uses.
		if _, err := exportEntries(
			ctx, storage, readHandle, bucketID,
			dal.ZoneCold, dal.SubColdAuditItem, afterAuditSeq, currentAuditSeq,
			ExportAuditItemSegmentKey(bucketID, afterAuditSeq+1, currentAuditSeq),
		); err != nil {
			return nil, fmt.Errorf("exporting audit items: %w", err)
		}

		manifest.Exports = append(manifest.Exports, ExportSegment{
			Type:     "auditItem",
			StartSeq: afterAuditSeq + 1,
			EndSeq:   currentAuditSeq,
			Key:      ExportAuditItemSegmentKey(bucketID, afterAuditSeq+1, currentAuditSeq),
		})

		segmentsUploaded++
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

// exportEntries streams entries for a given prefix from (afterSeq, endSeq] into
// a KV stream segment uploaded to storage. Returns the number of entries exported.
func exportEntries(
	ctx context.Context,
	storage Storage,
	reader dal.PebbleReader,
	_ string,
	zone, sub byte,
	afterSeq, endSeq uint64,
	segmentKey string,
) (uint64, error) {
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
		return 0, fmt.Errorf("creating iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	// Buffer the segment in memory then upload.
	// For very large exports, this could be switched to a streaming upload.
	var buf bytes.Buffer
	writer := NewKVStreamWriter(&buf)

	if err := writer.WriteHeader(); err != nil {
		return 0, err
	}

	var count uint64

	for iter.First(); iter.Valid(); iter.Next() {
		value, err := iter.ValueAndErr()
		if err != nil {
			return 0, fmt.Errorf("reading value: %w", err)
		}

		if err := writer.WriteEntry(iter.Key(), value); err != nil {
			return 0, err
		}

		count++
	}

	if err := writer.WriteFooter(); err != nil {
		return 0, err
	}

	if count > 0 {
		if err := storage.PutFile(ctx, segmentKey, bytes.NewReader(buf.Bytes()), int64(buf.Len())); err != nil {
			return 0, fmt.Errorf("uploading segment %s: %w", segmentKey, err)
		}
	}

	return count, nil
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

// listCheckpointFiles walks the checkpoint directory and returns all files with their sizes.
func listCheckpointFiles(dir string) (map[string]int64, error) {
	files := make(map[string]int64)

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

		// Normalize to forward slashes for consistent keys across platforms
		files[filepath.ToSlash(relPath)] = info.Size()

		return nil
	})

	return files, err
}

// diffFiles computes which files need to be uploaded and deleted.
// A file needs uploading if it's new or its size changed.
// A file needs deletion if it's no longer in the checkpoint.
func diffFiles(current, previous map[string]int64) (toUpload, toDelete []string) {
	for filename, size := range current {
		prevSize, exists := previous[filename]
		if !exists || prevSize != size {
			toUpload = append(toUpload, filename)
		}
	}

	for filename := range previous {
		if _, exists := current[filename]; !exists {
			toDelete = append(toDelete, filename)
		}
	}

	return toUpload, toDelete
}
