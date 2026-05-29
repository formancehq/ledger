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
	Duration             time.Duration
	LastLogSequence      uint64
	LastAuditSequence    uint64
}

// RunBackup performs a full checkpoint backup cycle.
// It creates a Pebble checkpoint, diffs SST files against the previous manifest,
// uploads new/changed files, cleans up stale exports, and writes an updated manifest.
func RunBackup(
	ctx context.Context,
	logger logging.Logger,
	store *dal.Store,
	storage Storage,
	bucketID string,
) (*Result, error) {
	start := time.Now()

	manifestKey := ManifestKey(bucketID)

	// 1. Create temporary checkpoint (hard links, quasi-free)
	checkpointPath, err := store.CreateTemporaryCheckpoint("backup")
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint: %w", err)
	}

	defer func() {
		_ = store.RemoveTemporaryCheckpoint("backup")
	}()

	// 2. List files in checkpoint
	localFiles, err := listCheckpointFiles(checkpointPath)
	if err != nil {
		return nil, fmt.Errorf("listing checkpoint files: %w", err)
	}

	// 3. Read existing manifest
	existingManifest := ReadManifestOrEmpty(ctx, logger, storage, manifestKey)

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

	readHandle, handleErr := checkpointStore.NewDirectReadHandle()
	if handleErr != nil {
		_ = checkpointStore.Close()

		return nil, fmt.Errorf("creating read handle: %w", handleErr)
	}

	lastAppliedIndex, _ := query.ReadLastAppliedIndex(readHandle)

	lastLog, _ := query.ReadLastLog(readHandle)

	var lastLogSeq uint64
	if lastLog != nil {
		lastLogSeq = lastLog.GetSequence()
	}

	lastAuditSeq, _ := query.ReadLastAuditSequence(readHandle)

	_ = readHandle.Close()
	_ = checkpointStore.Close()

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

	duration := time.Since(start)

	logger.WithFields(map[string]any{
		"duration":          duration.String(),
		"uploaded":          len(toUpload),
		"deleted":           len(toDelete),
		"total":             len(localFiles),
		"lastLogSequence":   lastLogSeq,
		"lastAuditSequence": lastAuditSeq,
		"lastAppliedIndex":  lastAppliedIndex,
	}).Infof("Backup completed")

	return &Result{
		FilesUploaded:     len(toUpload),
		FilesDeleted:      len(toDelete),
		TotalFiles:        len(localFiles),
		LastLogSequence:   lastLogSeq,
		LastAuditSequence: lastAuditSeq,
		LastAppliedIndex:  lastAppliedIndex,
		Duration:          duration,
	}, nil
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
	manifest := ReadManifestOrEmpty(ctx, logger, storage, manifestKey)

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
	currentLastLog, _ := query.ReadLastLog(readHandle)

	var currentLogSeq uint64
	if currentLastLog != nil {
		currentLogSeq = currentLastLog.GetSequence()
	}

	currentAuditSeq, _ := query.ReadLastAuditSequence(readHandle)

	// Ensure monotonicity: after a RestoreCheckpoint (leadership change +
	// snapshot from new leader), Pebble may have a lower cold-zone sequence
	// than what the manifest already recorded from a previous export.
	// Never regress below the manifest.
	currentLogSeq = max(currentLogSeq, afterLogSeq)
	currentAuditSeq = max(currentAuditSeq, afterAuditSeq)

	// 5. Check if there's anything new
	if currentLogSeq <= afterLogSeq && currentAuditSeq <= afterAuditSeq {
		logger.Infof("No new entries to export")

		return &IncrementalBackupResult{
			Duration:          time.Since(start),
			LastLogSequence:   afterLogSeq,
			LastAuditSequence: afterAuditSeq,
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
	}

	// 8. Write updated manifest
	if err := WriteManifest(ctx, storage, manifestKey, manifest); err != nil {
		return nil, fmt.Errorf("writing manifest: %w", err)
	}

	duration := time.Since(start)

	logger.WithFields(map[string]any{
		"duration":             duration.String(),
		"logEntriesExported":   logEntriesExported,
		"auditEntriesExported": auditEntriesExported,
		"segmentsUploaded":     segmentsUploaded,
		"lastLogSequence":      currentLogSeq,
		"lastAuditSequence":    currentAuditSeq,
	}).Infof("Incremental backup completed")

	return &IncrementalBackupResult{
		LogEntriesExported:   logEntriesExported,
		AuditEntriesExported: auditEntriesExported,
		SegmentsUploaded:     segmentsUploaded,
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
