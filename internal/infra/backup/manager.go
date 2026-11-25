package backup

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
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

// RunBackup performs a full checkpoint backup cycle.
// It flushes memtables, creates a Pebble checkpoint, uploads missing
// content-addressed files, publishes a manifest, and prunes stale objects.
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
	key, err := query.ReadAuditKey(store)
	if err != nil {
		return nil, fmt.Errorf("reading audit key before full backup: %w", err)
	}
	if key == nil {
		return nil, errors.New("full backup requires a committed audit key")
	}

	manifestKey := ManifestKey(bucketID)

	// 1. Flush committed memtables for a portable backup, then create a
	// temporary checkpoint. Follower sync also uses CreateTemporaryCheckpoint
	// but does not require this extra flush.
	if err := store.Flush(); err != nil {
		return nil, fmt.Errorf("flushing store before backup checkpoint: %w", err)
	}
	// Concurrent writes after the flush remain safe: WithFlushedWAL includes
	// the Pebble WAL in the checkpoint when required, and the upload includes
	// every checkpoint file.
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
	prevManifest, err := ReadManifestOrEmpty(ctx, logger, storage, manifestKey)
	if err != nil {
		return nil, err
	}

	prevCheckpointKeys := checkpointKeySet(prevManifest)
	prevExportKeys := exportKeySet(prevManifest)

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

	deletedCheckpointKeys := pruneOrphans(ctx, logger, storage, checkpointKeyspace(bucketID), expectedKeys)
	// A full backup writes Exports: nil, so every export segment in storage is
	// removed here. When this full backup follows one or more incrementals, the
	// previous committed manifest referenced those export segments as live
	// objects now rolled up into the new checkpoint — deleting them is ordinary
	// supersede churn (files_deleted), not a failed-run leftover (orphans_deleted).
	deletedExportKeys := pruneOrphans(ctx, logger, storage, exportKeyspace(bucketID), nil)

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

// keyspace is one object namespace a backup chain owns: the listing root, and
// the test for whether a listed key was written by this chain rather than by
// another chain whose bucketID nests under this root. The two travel together
// so a prune can never be pointed at a root without the matching ownership
// test — see the recognisers in segment.go for why that matters.
type keyspace struct {
	root  string
	owned func(key string) bool
}

// checkpointKeyspace is the chain's checkpoint objects under data/.
func checkpointKeyspace(bucketID string) keyspace {
	return keyspace{
		root:  CheckpointPrefix(bucketID),
		owned: func(key string) bool { return ownsCheckpointKey(bucketID, key) },
	}
}

// exportKeyspace identifies old RC export objects under exports/ for cleanup.
func exportKeyspace(bucketID string) keyspace {
	return keyspace{
		root:  ExportPrefix(bucketID),
		owned: func(key string) bool { return ownsExportKey(bucketID, key) },
	}
}

// pruneOrphans lists every object under space.root and deletes those this chain
// owns whose key is not in expectedKeys. A nil or empty expectedKeys deletes
// every object this chain owns under the root. Keys the chain does not own are
// left untouched: a listing by prefix also returns a nested chain's live
// objects, and deleting those would destroy that backup.
//
// It returns the keys it successfully deleted so callers can classify them
// (e.g. stale-vs-orphan). Failures are logged and skipped (a transient List or
// Delete error must never fail the surrounding backup — the manifest is already
// committed and the next run will retry).
func pruneOrphans(ctx context.Context, logger logging.Logger, storage Storage, space keyspace, expectedKeys map[string]struct{}) []string {
	keys, err := storage.ListFiles(ctx, space.root)
	if err != nil {
		logger.WithFields(map[string]any{
			"prefix": space.root,
			"error":  err,
		}).Errorf("Failed to list backup objects for orphan prune (non-fatal)")

		return nil
	}

	var (
		deleted []string
		foreign int
	)

	for _, key := range keys {
		// Another chain's object, reachable here only because its bucketID
		// nests under this root. Never ours to delete.
		if !space.owned(key) {
			foreign++

			continue
		}

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

	if foreign > 0 {
		logger.WithFields(map[string]any{
			"prefix": space.root,
			"count":  foreign,
		}).Infof("Left objects of another backup chain untouched under this prefix; a backup bucket-id nests under this one")
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
