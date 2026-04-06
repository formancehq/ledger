package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// Manifest describes the current state of a backup.
type Manifest struct {
	Timestamp string           `json:"timestamp"`
	Files     map[string]int64 `json:"files"` // filename -> size in bytes
}

// Result contains statistics from a backup run.
type Result struct {
	FilesUploaded int
	FilesDeleted  int
	TotalFiles    int
	Duration      time.Duration
}

// RunIncrementalBackup performs a single incremental backup cycle.
// It creates a Pebble checkpoint, diffs against the previous manifest,
// uploads new files, deletes stale ones, and writes an updated manifest.
func RunIncrementalBackup(
	ctx context.Context,
	logger logging.Logger,
	store *dal.Store,
	storage Storage,
	bucketID string,
) (*Result, error) {
	start := time.Now()

	manifestKey := bucketID + "/backups/manifest.json"
	fileKeyPrefix := bucketID + "/backups/data/"

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

	// 3. Read existing manifest (nil on first backup)
	existingManifest, err := readManifest(ctx, storage, manifestKey)
	if err != nil {
		logger.Infof("No existing manifest found, performing full backup")

		existingManifest = &Manifest{Files: make(map[string]int64)}
	}

	// 4. Compute diff — SST files are immutable, so same name = same content
	toUpload, toDelete := diffFiles(localFiles, existingManifest.Files)

	logger.WithFields(map[string]any{
		"totalFiles": len(localFiles),
		"toUpload":   len(toUpload),
		"toDelete":   len(toDelete),
	}).Infof("Backup diff computed")

	// 5. Upload new/changed files
	for _, filename := range toUpload {
		if err := uploadFile(ctx, storage, checkpointPath, fileKeyPrefix+filename, filename); err != nil {
			return nil, err
		}
	}

	// 6. Delete stale files from backup storage
	for _, filename := range toDelete {
		if err := storage.DeleteFile(ctx, fileKeyPrefix+filename); err != nil {
			logger.WithFields(map[string]any{
				"file":  filename,
				"error": err,
			}).Errorf("Failed to delete stale backup file (non-fatal)")
		}
	}

	// 7. Write updated manifest (last, for atomicity)
	newManifest := &Manifest{
		Timestamp: time.Now().UTC().Format(time.RFC3339Nano),
		Files:     localFiles,
	}

	if err := writeManifest(ctx, storage, manifestKey, newManifest); err != nil {
		return nil, fmt.Errorf("writing manifest: %w", err)
	}

	duration := time.Since(start)

	logger.WithFields(map[string]any{
		"duration": duration.String(),
		"uploaded": len(toUpload),
		"deleted":  len(toDelete),
		"total":    len(localFiles),
	}).Infof("Incremental backup completed")

	return &Result{
		FilesUploaded: len(toUpload),
		FilesDeleted:  len(toDelete),
		TotalFiles:    len(localFiles),
		Duration:      duration,
	}, nil
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

func readManifest(ctx context.Context, storage Storage, key string) (*Manifest, error) {
	reader, err := storage.GetFile(ctx, key)
	if err != nil {
		return nil, err
	}

	defer func() { _ = reader.Close() }()

	var manifest Manifest
	if err := json.NewDecoder(reader).Decode(&manifest); err != nil {
		return nil, fmt.Errorf("decoding manifest: %w", err)
	}

	return &manifest, nil
}

func writeManifest(ctx context.Context, storage Storage, key string, manifest *Manifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling manifest: %w", err)
	}

	return storage.PutFile(ctx, key, bytes.NewReader(data), int64(len(data)))
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
