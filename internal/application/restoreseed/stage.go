// Package restoreseed stages a portable read index beside the primary restore.
// Only the generation named by the final RESTORED marker may become a live index.
package restoreseed

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/google/uuid"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/backup"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

const stageDir = "restore-readindex-staging"
const generationFile = ".restore-generation"
const ownerFile = ".restore-staging-owner"
const ownerTag = "ledger-read-index-restore-v1"

func seedPath(dataDir string) string { return filepath.Join(dataDir, stageDir, "readindex") }

// Discard removes the reserved staging directory of a failed or canceled restore.
func Discard(dataDir string) error {
	dir := filepath.Join(dataDir, stageDir)
	info, err := os.Lstat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || !hasTag(dir, ownerFile, ownerTag) {
		return fmt.Errorf("refusing to remove unowned read-index staging path %s", dir)
	}

	return os.RemoveAll(dir)
}

// Install the ownership sentinel before publishing the staging directory name.
// A crash while reserving can leave only an empty/sentinel-only temporary
// directory, never a multi-terabyte tree that cannot be safely reclaimed.
func reserveDirectory(dataDir, name, tagName, tagValue, subdir string) (retErr error) {
	temp, err := os.MkdirTemp(dataDir, ".restore-reservation-")
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, os.RemoveAll(temp)) }()
	if err := os.WriteFile(filepath.Join(temp, tagName), []byte(tagValue), 0o600); err != nil {
		return err
	}
	owner, err := os.Open(filepath.Join(temp, tagName))
	if err != nil {
		return err
	}
	if err := errors.Join(owner.Sync(), owner.Close()); err != nil {
		return err
	}
	if subdir != "" {
		if err := os.Mkdir(filepath.Join(temp, subdir), 0o750); err != nil {
			return err
		}
	}
	if err := dal.FsyncDir(temp); err != nil {
		return err
	}
	if err := os.Rename(temp, filepath.Join(dataDir, name)); err != nil {
		return err
	}

	return dal.FsyncDir(dataDir)
}

// StageCold prepares an owned, empty read store for an older backup or marker.
func StageCold(ctx context.Context, dataDir string) (string, error) {
	return Stage(ctx, dataDir, "", nil, nil, nil, backup.ReadIndexDownloadOptions{}, nil)
}

// Stage verifies the physical seed against the unmodified primary checkpoint,
// before applying any incremental exports. Missing/unknown seed versions use
// a fresh read store; corruption of a supported seed fails the restore.
func Stage(ctx context.Context, dataDir, bucketID string, storage backup.Storage, checkpoint *backup.CheckpointManifest, primary dal.PebbleGetter, options backup.ReadIndexDownloadOptions, logger logging.Logger) (_ string, retErr error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := Discard(dataDir); err != nil {
		return "", fmt.Errorf("discarding stale read-index staging: %w", err)
	}
	dir := seedPath(dataDir)
	if err := reserveDirectory(dataDir, stageDir, ownerFile, ownerTag, "readindex"); err != nil {
		return "", err
	}
	defer func() {
		if retErr != nil {
			retErr = errors.Join(retErr, Discard(dataDir))
		}
	}()
	root, err := os.OpenRoot(dir)
	if err != nil {
		return "", err
	}
	defer func() { retErr = errors.Join(retErr, root.Close()) }()
	if checkpoint != nil && checkpoint.ReadIndex != nil {
		if checkpoint.ReadIndex.Version == backup.ReadIndexVersion {
			if len(checkpoint.Files) == 0 {
				return "", errors.New("read-index seed has no primary checkpoint files")
			}
			if err := backup.DownloadReadIndex(ctx, storage, bucketID, checkpoint, root, options); err != nil {
				return "", fmt.Errorf("downloading read-index seed: %w", err)
			}
			if err := ctx.Err(); err != nil {
				return "", err
			}
			if err := readstore.PrepareRestoreSeed(dir, logger, primary, checkpoint.LastLogSequence, checkpoint.LastAuditSequence); err != nil {
				return "", fmt.Errorf("preparing read-index seed: %w", err)
			}
		} else {
			logger.Infof("Read-index backup version %d is unsupported; rebuilding indexes from primary history", checkpoint.ReadIndex.Version)
		}
	}
	id, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}
	if err := root.WriteFile(generationFile, []byte(id.String()), 0o600); err != nil {
		return "", err
	}
	// The marker commit must not outlive the files whose names it vouches for.
	if err := syncOrCopyTree(ctx, root, nil); err != nil {
		return "", fmt.Errorf("syncing prepared read-index seed: %w", err)
	}
	if err := dal.FsyncDir(dataDir); err != nil {
		return "", err
	}

	return id.String(), ctx.Err()
}
