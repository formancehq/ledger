package indexbuilder

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// checkpointHorizon distinguishes a live checkpoint's source Raft horizon
// from a restored checkpoint, whose source number is unordered here. A
// checkpoint already deleted in the fixed snapshot also waits for the full
// destination target before any certificate is published.
func checkpointHorizon(handle *dal.ReadHandle, id, appliedIndex, targetAppliedIndex uint64) (uint64, bool, error) {
	checkpointState, err := query.ReadQueryCheckpoint(handle, id)
	if err != nil {
		return 0, false, fmt.Errorf("reading query checkpoint %d restore provenance: %w", id, err)
	}
	restored := checkpointState == nil || checkpointState.GetRestoredFromBackup()
	if restored {
		return targetAppliedIndex, true, nil
	}

	return appliedIndex, false, nil
}

func (b *Builder) materializePendingCheckpoint(ctx context.Context) error {
	pending := b.pendingCheckpointMaterialization
	if pending.id == 0 {
		return nil
	}

	for {
		err := b.readStore.WaitForAuditRaftProgress(ctx, pending.horizon)
		if errors.Is(err, readstore.ErrAuditProjectionUnavailable) ||
			errors.Is(err, readstore.ErrAuditProjectionFailed) {
			auditState := "disabled"
			if errors.Is(err, readstore.ErrAuditProjectionFailed) {
				auditState = "failed"
			}
			b.logger.WithFields(map[string]any{
				"checkpointID": pending.id,
				"horizon":      pending.horizon,
				"auditState":   auditState,
			}).Infof("Query checkpoint remains unavailable because the audit projection cannot certify it")
			b.pendingCheckpointMaterialization = pendingCheckpointMaterialization{}

			return nil
		}
		if err != nil {
			return fmt.Errorf("waiting for audit projection before query checkpoint %d: %w", pending.id, err)
		}

		_, _, auditGeneration := b.readStore.AuditProjectionStateWithGeneration()
		if err := b.createReadIndexCheckpoint(pending.id, auditGeneration); errors.Is(err, readstore.ErrAuditProjectionUnavailable) {
			b.logger.WithFields(map[string]any{
				"checkpointID": pending.id,
				"horizon":      pending.horizon,
			}).Infof("Audit projection changed during query checkpoint materialization; retrying")

			continue
		} else if err != nil {
			return err
		}

		b.pendingCheckpointMaterialization = pendingCheckpointMaterialization{}

		return nil
	}
}

// checkpointLogFields is a CreatedQueryCheckpoint or DeletedQueryCheckpoint log
// reduced to what the builder acts on. Exactly one of createID/deleteID is set.
// The log cursor reuses a single message, so a log met ahead of its turn is kept
// as these fields rather than as the message itself.
type checkpointLogFields struct {
	sequence     uint64
	createID     uint64
	deleteID     uint64
	appliedIndex uint64
}

// checkpointLinkRetries bounds the retries of a read-index checkpoint whose
// SST hard-link raced a concurrent compaction ("link ... no such file or
// directory"). Pebble hard-links live SSTs last; if one is compacted away
// between the manifest snapshot and the link, the whole checkpoint call fails.
// Recreating the checkpoint re-snapshots the (now stable) LSM and succeeds.
const checkpointLinkRetries = 5

// createReadIndexCheckpoint materializes a physical Pebble checkpoint of the read
// index for the given query checkpoint ID, atomically and crash-safely:
//
//  1. Build the checkpoint into a sibling temp directory (never the final path).
//  2. fsync the temp directory so its content is durable.
//  3. Atomically rename it into the final location.
//  4. Write the readiness marker LAST, only after the rename succeeded.
//
// A crash at any point before step 4 leaves either no final directory or a
// final directory without a marker — both are treated as "not materialized" by
// readers (openCheckpointStores). A half-written temp directory is never visible
// under the final path. Called only from the inline indexing path, at the moment
// the builder crosses the CreatedQueryCheckpoint log (so the snapshot is exactly
// MaxSequence). The indexed cursor passes that log only once the marker exists,
// so a restarted builder crosses it again and lands here: an unmarked residue is
// discarded and rebuilt, a marked directory is left as is.
func (b *Builder) createReadIndexCheckpoint(checkpointID, auditGeneration uint64) (err error) {
	finalDir := b.pebbleStore.QueryCheckpointReadIndexDir(checkpointID)

	// Already materialized on this replica (redundant call). Nothing to do.
	if dal.CheckpointDirReady(finalDir) {
		return nil
	}

	// Any leftover final directory here is a prior attempt that crashed before
	// writing the marker (never trusted — see the type doc): discard it.
	if err := os.RemoveAll(finalDir); err != nil {
		return fmt.Errorf("clearing stale read index checkpoint %d: %w", checkpointID, err)
	}

	tmpDir := finalDir + ".tmp"

	// pebble.Checkpoint fails with ErrExist if the target already exists, so the
	// temp dir must not linger from a previous crashed attempt.
	if err := os.RemoveAll(tmpDir); err != nil {
		return fmt.Errorf("clearing stale temp checkpoint %d: %w", checkpointID, err)
	}

	// Any error path after this point leaves a partial temp directory behind;
	// clean it up. After a successful rename tmpDir no longer exists, so this is
	// a harmless no-op on the happy path.
	defer func() {
		if err != nil {
			_ = os.RemoveAll(tmpDir)
		}
	}()

	for attempt := range checkpointLinkRetries {
		err = b.readStore.CreateCheckpoint(tmpDir)
		if err == nil {
			break
		}

		// A concurrent compaction can delete an SST between the manifest
		// snapshot and the hard-link, surfacing as ErrNotExist. This is
		// transient — retry after clearing the partial temp directory so the
		// next CreateCheckpoint starts from a clean slate.
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("creating read index checkpoint %d: %w", checkpointID, err)
		}

		// Clear the partial temp dir between attempts (not on the last one —
		// the deferred cleanup handles the give-up case).
		if attempt < checkpointLinkRetries-1 {
			b.logger.WithFields(map[string]any{
				"checkpointID": checkpointID,
				"attempt":      attempt + 1,
				"error":        err,
			}).Infof("Read index checkpoint hard-link raced compaction, retrying")

			if rmErr := os.RemoveAll(tmpDir); rmErr != nil {
				return fmt.Errorf("cleaning partial temp checkpoint %d: %w", checkpointID, rmErr)
			}
		}
	}

	if err != nil {
		return fmt.Errorf("creating read index checkpoint %d after %d attempts: %w", checkpointID, checkpointLinkRetries, err)
	}

	// fsync the fully-built temp directory before the rename so its content is
	// durable independent of the rename.
	if err = dal.FsyncDir(tmpDir); err != nil {
		return fmt.Errorf("fsync temp checkpoint %d: %w", checkpointID, err)
	}

	// Atomic rename into the final location: a reader never sees a partial dir.
	if err = os.Rename(tmpDir, finalDir); err != nil {
		return fmt.Errorf("renaming checkpoint %d into place: %w", checkpointID, err)
	}

	// fsync the parent so the rename is durable before we vouch for it.
	if err = dal.FsyncDir(filepath.Dir(finalDir)); err != nil {
		return fmt.Errorf("fsync checkpoint %d parent: %w", checkpointID, err)
	}

	// Write the readiness marker LAST: readers (openCheckpointStores) only treat
	// the checkpoint as ready once this exists, guaranteeing a fully materialized,
	// atomically-renamed directory. (tmpDir is already renamed away, so the
	// deferred cleanup is a no-op if this fails; the markerless finalDir is then
	// treated as not-ready and rebuilt on the next attempt.)
	ready, err := b.readStore.MarkCheckpointReadyAtAuditGeneration(finalDir, auditGeneration)
	if err != nil {
		return fmt.Errorf("marking read index checkpoint %d ready: %w", checkpointID, err)
	}
	if !ready {
		return readstore.ErrAuditProjectionUnavailable
	}

	b.logger.WithFields(map[string]any{
		"checkpointID": checkpointID,
	}).Infof("Created read index query checkpoint")

	return nil
}

// deleteReadIndexCheckpoint drives the same whole-checkpoint cleanup as the
// FSM. The shared DAL lease gate makes this duplicate trigger idempotent and
// prevents either component from being removed under an acquired reader.
func (b *Builder) deleteReadIndexCheckpoint(checkpointID uint64) {
	if err := b.pebbleStore.DeleteQueryCheckpointFiles(checkpointID); err != nil {
		b.logger.WithFields(map[string]any{
			"error":        err,
			"checkpointID": checkpointID,
		}).Infof("Failed to delete read index checkpoint files (may not exist)")
	}
}
