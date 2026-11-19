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
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
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
	return RunBackupWithReadIndex(ctx, logger, store, storage, bucketID, checkpointName, "")
}

// RunBackupWithReadIndex publishes a primary checkpoint with an optional physical
// read-index seed. The caller owns the immutable read checkpoint. It must take
// that checkpoint first, so every row the indexer consumed belongs to the
// primary checkpoint captured here (or an earlier prefix of it).
func RunBackupWithReadIndex(
	ctx context.Context, logger logging.Logger, store *dal.Store, storage Storage,
	bucketID, checkpointName, readIndexCheckpointPath string,
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

	// Freeze the primary after the caller has captured the read index.
	checkpointPath, err := store.CreateTemporaryCheckpoint(checkpointName)
	if err != nil {
		return nil, fmt.Errorf("creating checkpoint: %w", err)
	}

	defer func() {
		_ = store.RemoveTemporaryCheckpoint(checkpointName)
	}()

	// Classify objects retired by this backup separately from orphans left
	// behind by a previous failed publication.
	prevManifest, err := ReadManifestOrEmpty(ctx, logger, storage, manifestKey)
	if err != nil {
		return nil, err
	}

	prevCheckpointKeys := checkpointKeySet(prevManifest)
	prevExportKeys := exportKeySet(prevManifest)

	// Determine which files still need uploading. A file whose
	// content-addressed key already exists on storage is byte-identical to what
	// is already there (that is what content-addressing guarantees), so it is
	// skipped — this is the incremental dedup, now keyed by content rather than
	// by name+size. Objects the new manifest will NOT reference are left in
	// place and removed by the post-manifest orphan prune.
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

	checkpoint, err := inspectBackupCheckpoint(logger, checkpointPath, readIndexCheckpointPath)
	if err != nil {
		return nil, err
	}

	// Count both immutable inventories before hashing or uploading either one.
	// A multi-TB full that cannot fit the restore file limit fails on directory
	// entries alone. Preserve released primary-only behavior when no seed exists.
	var maxFiles int
	var readIndexNames []string
	if readIndexCheckpointPath != "" {
		maxFiles = MaxRestoreManifestFiles
		readIndexNames, err = checkpointNames(ctx, readIndexCheckpointPath, true, maxFiles)
		if err != nil {
			return nil, fmt.Errorf("listing read index checkpoint: %w", err)
		}
	}
	primaryNames, err := checkpointNames(ctx, checkpointPath, false, maxFiles)
	if err != nil {
		return nil, fmt.Errorf("listing primary checkpoint: %w", err)
	}

	// All objects are immutable and uploaded before the manifest swap. Both
	// inventories share the same content-addressed keys, so identical files
	// need only one upload even if both checkpoints contain them.
	checkpointFiles, uploaded, err := uploadCheckpoint(ctx, storage, bucketID, checkpointPath, primaryNames, existingKeys)
	if err != nil {
		return nil, fmt.Errorf("uploading primary checkpoint: %w", err)
	}
	checkpoint.Files = checkpointFiles
	totalFiles := len(checkpointFiles)
	if readIndexCheckpointPath != "" {
		files, n, err := uploadCheckpoint(ctx, storage, bucketID, readIndexCheckpointPath, readIndexNames, existingKeys)
		if err != nil {
			return nil, fmt.Errorf("uploading read index checkpoint: %w", err)
		}
		checkpoint.ReadIndex = &ReadIndexManifest{
			Version: ReadIndexVersion, Files: files,
			InventorySHA256: readIndexBinding(checkpoint.Files, files),
		}
		uploaded += n
		totalFiles += len(files)
	}

	// Write updated manifest with new checkpoint and empty exports
	checkpoint.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	newManifest := &Manifest{Checkpoint: checkpoint}

	if err := WriteManifest(ctx, storage, manifestKey, newManifest); err != nil {
		return nil, fmt.Errorf("writing manifest: %w", err)
	}

	// Prune orphans now that the new manifest is committed. The manifest is
	// the authoritative inventory; anything under data/ or exports/ not in it is
	// dead weight — this includes both the stale checkpoint files this run
	// replaced and every export segment obsoleted by the new checkpoint, plus
	// files leaked by earlier runs that crashed before writing a manifest (the
	// diff step, which compares against the *previous manifest*, cannot reach
	// those). Running the prune strictly after WriteManifest is what makes the
	// cycle crash-safe: at no point is a still-referenced object deleted.
	deletedCheckpointKeys := pruneOrphans(ctx, logger, storage, checkpointKeyspace(bucketID), checkpointKeySet(newManifest))
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
		"uploaded":          uploaded,
		"filesDeleted":      filesDeleted,
		"orphansDeleted":    orphansDeleted,
		"total":             totalFiles,
		"lastLogSequence":   checkpoint.LastLogSequence,
		"lastAuditSequence": checkpoint.LastAuditSequence,
		"lastAppliedIndex":  checkpoint.LastAppliedIndex,
	}).Infof("Backup completed")

	return &Result{
		FilesUploaded: uploaded,
		// FilesDeleted counts objects this run superseded — checkpoint files and
		// export segments the previous manifest referenced that the new manifest
		// no longer does (normal roll-up churn). OrphansDeleted counts everything
		// else the post-manifest prune removed: files leaked by earlier runs that
		// crashed before writing a manifest.
		FilesDeleted:      filesDeleted,
		OrphansDeleted:    orphansDeleted,
		TotalFiles:        totalFiles,
		LastLogSequence:   checkpoint.LastLogSequence,
		LastAuditSequence: checkpoint.LastAuditSequence,
		LastAppliedIndex:  checkpoint.LastAppliedIndex,
		Duration:          duration,
	}, nil
}

// inspectBackupCheckpoint releases its two Pebble handles before the full's
// potentially long upload. The two directories remain frozen throughout.
func inspectBackupCheckpoint(logger logging.Logger, checkpointPath, readIndexCheckpointPath string) (_ *CheckpointManifest, retErr error) {
	checkpointStore, err := dal.OpenReadOnly(checkpointPath, logger)
	if err != nil {
		return nil, fmt.Errorf("opening checkpoint for reading sequences: %w", err)
	}

	defer func() { retErr = errors.Join(retErr, checkpointStore.Close()) }()

	readHandle, handleErr := checkpointStore.NewDirectReadHandle()
	if handleErr != nil {
		return nil, fmt.Errorf("creating read handle: %w", handleErr)
	}

	defer func() { retErr = errors.Join(retErr, readHandle.Close()) }()

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

	// Validate the captured read progress against the frozen primary before any
	// remote upload. A failed capture never replaces the previous backup.
	if readIndexCheckpointPath != "" {
		if err := readstore.ValidateBackupSeed(readIndexCheckpointPath, logger, readHandle, lastLogSeq, lastAuditSeq); err != nil {
			return nil, fmt.Errorf("validating read index checkpoint: %w", err)
		}
	}

	return &CheckpointManifest{
		LastAppliedIndex:  lastAppliedIndex,
		LastLogSequence:   lastLogSeq,
		LastAuditSequence: lastAuditSeq,
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

	return len(pruneOrphans(ctx, logger, storage, exportKeyspace(bucketID), expectedKeys))
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

// exportKeyspace is the chain's incremental export segments under exports/.
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

	if manifest.Checkpoint.ReadIndex != nil {
		for _, cf := range manifest.Checkpoint.ReadIndex.Files {
			keys[cf.Key] = struct{}{}
		}
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
	key, err := query.ReadAuditKey(store)
	if err != nil {
		return nil, fmt.Errorf("reading audit key before incremental backup: %w", err)
	}
	if key == nil {
		return nil, errors.New("incremental backup requires a committed audit key")
	}

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

	// Capture operational policy from this exact history snapshot. Never replace
	// a newer manifest policy with one observed before a snapshot rollback.
	policy, err := query.ReadClusterPolicy(readHandle)
	if err != nil {
		return nil, fmt.Errorf("reading backup cluster policy: %w", err)
	}
	appliedIndex, err := query.ReadLastAppliedIndex(readHandle)
	if err != nil {
		return nil, fmt.Errorf("reading backup applied index: %w", err)
	}
	policyChanged := false
	policyFloor := manifest.Checkpoint.LastAppliedIndex
	if manifest.ClusterPolicy != nil {
		policyFloor = max(policyFloor, manifest.ClusterPolicy.AppliedIndex)
	}
	if policy != nil && appliedIndex >= policyFloor && currentLogSeq >= afterLogSeq && currentAuditSeq >= afterAuditSeq {
		data := policy.MarshalDeterministicVT(nil)
		if manifest.ClusterPolicy == nil || appliedIndex > manifest.ClusterPolicy.AppliedIndex || !bytes.Equal(manifest.ClusterPolicy.Data, data) {
			manifest.ClusterPolicy = &ClusterPolicySnapshot{AppliedIndex: appliedIndex, Data: data}
			policyChanged = true
		}
	}

	// Ensure monotonicity: after a RestoreCheckpoint (leadership change +
	// snapshot from new leader), Pebble may have a lower history-zone sequence
	// than what the manifest already recorded from a previous export.
	// Never regress below the manifest.
	currentLogSeq = max(currentLogSeq, afterLogSeq)
	currentAuditSeq = max(currentAuditSeq, afterAuditSeq)

	// 5. Check if there's anything new
	if currentLogSeq <= afterLogSeq && currentAuditSeq <= afterAuditSeq {
		logger.Infof("No new entries to export")
		if policyChanged {
			if err := WriteManifest(ctx, storage, manifestKey, manifest); err != nil {
				return nil, fmt.Errorf("writing operational policy manifest: %w", err)
			}
		}

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
			dal.ZoneHistory, dal.SubHistoryLog, afterLogSeq, currentLogSeq, "log",
			func(part int) string { return ExportLogSegmentKey(bucketID, afterLogSeq+1, currentLogSeq, part) },
			maxSegmentBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("exporting log entries: %w", err)
		}

		logEntriesExported += count
		manifest.Exports = append(manifest.Exports, segs...)
		segmentsUploaded += len(segs)
	}

	// 7. Export new audit entries
	if currentAuditSeq > afterAuditSeq {
		segs, count, err := exportEntries(
			ctx, storage, readHandle,
			dal.ZoneHistory, dal.SubHistoryAudit, afterAuditSeq, currentAuditSeq, "audit",
			func(part int) string { return ExportAuditSegmentKey(bucketID, afterAuditSeq+1, currentAuditSeq, part) },
			maxSegmentBytes,
		)
		if err != nil {
			return nil, fmt.Errorf("exporting audit entries: %w", err)
		}

		auditEntriesExported += count
		manifest.Exports = append(manifest.Exports, segs...)
		segmentsUploaded += len(segs)

		// Export the audit items (per-order detail) for the same range.
		// The audit hash covers the per-item payloads on EVERY outcome, so a
		// restored backup missing them cannot reconstruct the chain. A failure
		// proposal writes one AuditItem per order too — writeAuditEntry
		// (state/machine.go) reaches buildAuditItems(serializedOrders, nil)
		// (state/audit.go), which only leaves LogSequence at 0 — so a
		// failure-only range is NOT an empty auditItem range. Nor is any other
		// range: an order-less proposal returns before the audit-entry path, so
		// every audit_seq in this range carries at least one item. The
		// empty-range handling is kept as defence in depth against a corrupt or
		// partially-restored source store: exportEntries then returns no
		// segments, so appending its result adds nothing and we never reference
		// a key that does not exist on storage (a subsequent ApplyExports would
		// fail on GetFile). The guard is structural, same as the
		// appliedProposal branch below — where an empty range is the normal
		// failure-only case rather than a corruption signal.
		itemSegs, _, err := exportEntries(
			ctx, storage, readHandle,
			dal.ZoneHistory, dal.SubHistoryAuditItem, afterAuditSeq, currentAuditSeq, "auditItem",
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
			dal.ZoneHistory, dal.SubHistoryAppliedProposal, afterAuditSeq, currentAuditSeq, "appliedProposal",
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

	// 10. Prune orphan export segments — anything under exports/ that the manifest
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

	// endSeq+1 wraps at MaxUint64 into a bound BELOW the lower one, which Pebble
	// reads as an empty range: the segment would export nothing and the delta
	// would silently lose every row it was meant to carry (invariant #11). The
	// prefix successor is the bound that includes the last addressable entry.
	var upperBound []byte

	if endSeq == math.MaxUint64 {
		upperBound = dal.ZonePrefixUpperBound(zone, sub)
	} else {
		kb2 := dal.NewKeyBuilder()
		kb2.PutZonePrefix(zone, sub).PutUint64(endSeq + 1)
		upperBound = kb2.Build()
	}

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

// checkpointNames enumerates immutable local files without reading their
// contents. A zero limit retains the primary-only backup's existing behavior.
func checkpointNames(ctx context.Context, dir string, readIndex bool, limit int) ([]string, error) {
	var names []string
	err := fs.WalkDir(os.DirFS(dir), ".", func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() || (readIndex && !validReadIndexFilename(name)) {
			return fmt.Errorf("invalid checkpoint file %q", name)
		}
		if limit > 0 && len(names) == limit {
			return fmt.Errorf("checkpoint exceeds restore limit of %d files per store", limit)
		}
		names = append(names, name)

		return nil
	})

	return names, err
}

// uploadCheckpoint uploads immutable files from a local Pebble checkpoint. It
// extends existingKeys after each successful Put so a second checkpoint can
// reuse identical objects. Failure may leave unreferenced objects; only a
// subsequent full's post-publication prune may remove them.
func uploadCheckpoint(ctx context.Context, storage Storage, bucketID, dir string, names []string, existingKeys map[string]struct{}) (map[string]CheckpointFile, int, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = root.Close() }()

	files := make(map[string]CheckpointFile, len(names))
	uploaded := 0
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return nil, uploaded, err
		}
		cf, err := uploadCheckpointFile(ctx, storage, bucketID, root, name, existingKeys)
		if err != nil {
			return nil, uploaded, err
		}
		files[name] = cf
		if _, present := existingKeys[cf.Key]; !present {
			existingKeys[cf.Key] = struct{}{}
			uploaded++
		}
	}

	return files, uploaded, nil
}

func uploadCheckpointFile(ctx context.Context, storage Storage, bucketID string, root *os.Root, name string, existingKeys map[string]struct{}) (CheckpointFile, error) {
	f, err := root.Open(filepath.FromSlash(name))
	if err != nil {
		return CheckpointFile{}, err
	}
	defer func() { _ = f.Close() }() // read-only immutable source
	h := sha256.New()
	size, err := io.Copy(h, f)
	if err != nil {
		return CheckpointFile{}, fmt.Errorf("hashing %s: %w", name, err)
	}
	cf := CheckpointFile{Key: CheckpointFileKey(bucketID, name, hex.EncodeToString(h.Sum(nil))), Size: size}
	if _, present := existingKeys[cf.Key]; !present {
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return CheckpointFile{}, err
		}
		if err := storage.PutFile(ctx, cf.Key, f, size); err != nil {
			return CheckpointFile{}, fmt.Errorf("uploading %s: %w", name, err)
		}
	}

	return cf, nil
}
