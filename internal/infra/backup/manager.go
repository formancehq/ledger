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

	"github.com/formancehq/ledger-v3-poc/internal/domain/processing"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/worker"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// Manifest describes the current state of a backup.
type Manifest struct {
	Timestamp string           `json:"timestamp"`
	Files     map[string]int64 `json:"files"` // filename -> size in bytes
}

// Manager runs scheduled incremental backups in the background.
// Only the Raft leader performs backups. It follows the same cron-based
// scheduling pattern as PeriodScheduler.
type Manager struct {
	logger   logging.Logger
	store    *dal.Store
	storage  Storage
	bucketID string
	schedule string
	isLeader func() bool
	w        worker.Worker
}

// NewManager creates a new backup Manager.
func NewManager(
	logger logging.Logger,
	store *dal.Store,
	storage Storage,
	bucketID string,
	schedule string,
	isLeader func() bool,
) *Manager {
	return &Manager{
		logger:   logger.WithFields(map[string]any{"cmp": "backup-manager"}),
		store:    store,
		storage:  storage,
		bucketID: bucketID,
		schedule: schedule,
		isLeader: isLeader,
		w:        worker.New(),
	}
}

// Start launches the background backup scheduler goroutine.
func (m *Manager) Start() {
	m.w.Run(m.loop)
}

// Stop signals the background goroutine to stop and waits for it to finish.
func (m *Manager) Stop() {
	m.w.Stop()
}

func (m *Manager) manifestKey() string {
	return m.bucketID + "/backups/manifest.json"
}

func (m *Manager) fileKey(filename string) string {
	return m.bucketID + "/backups/data/" + filename
}

func (m *Manager) loop(stop <-chan struct{}) {
	var timer *time.Timer

	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()

	resetTimer := func() <-chan time.Time {
		if timer != nil {
			timer.Stop()
		}

		if m.schedule == "" {
			timer = nil

			return nil
		}

		schedule, err := processing.CronParser.Parse(m.schedule)
		if err != nil {
			m.logger.WithFields(map[string]any{
				"cron":  m.schedule,
				"error": err,
			}).Errorf("Invalid backup schedule cron expression, disabling scheduler")

			timer = nil

			return nil
		}

		nextFire := schedule.Next(time.Now())
		delay := max(time.Until(nextFire), 0)

		m.logger.WithFields(map[string]any{
			"cron":     m.schedule,
			"nextFire": nextFire.Format(time.RFC3339),
		}).Infof("Backup scheduler armed")

		timer = time.NewTimer(delay)

		return timer.C
	}

	timerCh := resetTimer()

	for {
		select {
		case <-stop:
			return
		case <-timerCh:
			if m.isLeader() {
				m.logger.Infof("Backup scheduler firing: starting incremental backup")

				worker.RetryWithBackoff(stop, m.logger, func() error {
					return m.runBackup(stop)
				})
			}

			timerCh = resetTimer()
		}
	}
}

// runBackup performs a single incremental backup cycle.
func (m *Manager) runBackup(stop <-chan struct{}) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		select {
		case <-stop:
			cancel()
		case <-ctx.Done():
		}
	}()

	if !m.isLeader() {
		return worker.ErrNotLeader
	}

	start := time.Now()

	// 1. Create temporary checkpoint (hard links, quasi-free)
	checkpointPath, err := m.store.CreateTemporaryCheckpoint("backup")
	if err != nil {
		return fmt.Errorf("creating checkpoint: %w", err)
	}

	defer func() {
		_ = m.store.RemoveTemporaryCheckpoint("backup")
	}()

	// 2. List files in checkpoint
	localFiles, err := listCheckpointFiles(checkpointPath)
	if err != nil {
		return fmt.Errorf("listing checkpoint files: %w", err)
	}

	// 3. Read existing manifest (nil on first backup)
	existingManifest, err := m.readManifest(ctx)
	if err != nil {
		m.logger.Infof("No existing manifest found, performing full backup")

		existingManifest = &Manifest{Files: make(map[string]int64)}
	}

	// 4. Compute diff — SST files are immutable, so same name = same content
	toUpload, toDelete := diffFiles(localFiles, existingManifest.Files)

	m.logger.WithFields(map[string]any{
		"totalFiles": len(localFiles),
		"toUpload":   len(toUpload),
		"toDelete":   len(toDelete),
	}).Infof("Backup diff computed")

	// 5. Upload new/changed files
	for _, filename := range toUpload {
		if err := m.uploadFile(ctx, checkpointPath, filename); err != nil {
			return err
		}
	}

	// 6. Delete stale files from backup storage
	for _, filename := range toDelete {
		if err := m.storage.DeleteFile(ctx, m.fileKey(filename)); err != nil {
			m.logger.WithFields(map[string]any{
				"file":  filename,
				"error": err,
			}).Errorf("Failed to delete stale backup file (non-fatal)")
		}
	}

	// 7. Write updated manifest (last, for atomicity)
	newManifest := &Manifest{
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Files:     localFiles,
	}

	if err := m.writeManifest(ctx, newManifest); err != nil {
		return fmt.Errorf("writing manifest: %w", err)
	}

	m.logger.WithFields(map[string]any{
		"duration": time.Since(start).String(),
		"uploaded": len(toUpload),
		"deleted":  len(toDelete),
		"total":    len(localFiles),
	}).Infof("Incremental backup completed")

	return nil
}

func (m *Manager) uploadFile(ctx context.Context, checkpointPath, filename string) error {
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

	err = m.storage.PutFile(ctx, m.fileKey(filename), file, info.Size())
	_ = file.Close()

	if err != nil {
		return fmt.Errorf("uploading %s: %w", filename, err)
	}

	return nil
}

func (m *Manager) readManifest(ctx context.Context) (*Manifest, error) {
	reader, err := m.storage.GetFile(ctx, m.manifestKey())
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

func (m *Manager) writeManifest(ctx context.Context, manifest *Manifest) error {
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling manifest: %w", err)
	}

	return m.storage.PutFile(ctx, m.manifestKey(), bytes.NewReader(data), int64(len(data)))
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
