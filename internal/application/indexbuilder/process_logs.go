package indexbuilder

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// processLogs reads logs from Pebble starting after the given cursor,
// indexes them in batches of indexBatchSize. Logs are consumed on the fly
// (no intermediate slice) so the proto object can be GC'd immediately.
// When deadline is non-zero, processing stops after the deadline to yield
// time to other work (e.g. backfills).
//
// Since index handlers now receive previous metadata values directly from
// the log (no Pebble reads needed), processing uses a 2-pass design:
//   - Pass 1: iterate Pebble logs, dispatch to handlers that buffer writes
//     into WriteBatch.
//   - Pass 2 (only if needed): create an indexed batch, Init, write progress,
//     then Flush (which commits the batch).
//
// When a batch produces no index writes, the Pebble batch is skipped
// entirely. Progress is persisted once at the end, reducing fsyncs to O(1).
func (b *Builder) processLogs(ctx context.Context, cursor uint64, deadline time.Time) (uint64, error) {
	handle, err := b.pebbleStore.NewDirectReadHandle()
	if err != nil {
		return cursor, fmt.Errorf("creating read handle for log processing: %w", err)
	}

	defer func() { _ = handle.Close() }()

	// Per-batch schema resolver: memoizes LedgerInfo lookups for the
	// duration of this processLogs call. Hot-path encode sites read from
	// it via b.coerceForLedger.
	b.batchSchema = newSchemaResolver(handle, b.attrs)
	defer func() { b.batchSchema = nil }()

	logsCursor, err := query.ReadLogsSince(ctx, handle, cursor, dal.WithReuse(), dal.WithResetFunc(resetLogForReuse))
	if err != nil {
		return cursor, err
	}

	defer func() { _ = logsCursor.Close() }()

	// Open the AppliedProposal iterator for transient-account filtering.
	proposals, err := newAppliedProposalSync(ctx, handle, b.lastAppliedProposalSeq)
	if err != nil {
		return cursor, fmt.Errorf("creating applied proposal sync: %w", err)
	}

	defer func() { _ = proposals.close() }()

	// Track whether we advanced the cursor without persisting it yet.
	needsPersist := false
	startCursor := cursor
	lastProgressLog := time.Now()
	persistAppliedProposalProgress := func(batch *dal.WriteSession, lastProcessedSeq uint64) error {
		proposals.advanceBefore(lastProcessedSeq + 1)
		// Surface any non-EOF iterator error the AppliedProposal cursor
		// saw during this advance. Letting it slide would mean the
		// transient set we just consumed could be incomplete, and the
		// indexer would persist account->tx mappings for volumes that
		// should have been skipped.
		if err := proposals.err(); err != nil {
			return fmt.Errorf("applied proposal cursor failed: %w", err)
		}
		if seq := proposals.resumeSequence(); seq > b.lastAppliedProposalSeq {
			if err := b.readStore.WriteAppliedProposalProgress(batch, seq); err != nil {
				return err
			}

			b.lastAppliedProposalSeq = seq
		}

		return nil
	}

	for {
		var (
			batchCount              int
			lastSeq                 uint64
			eof                     bool
			pendingCheckpointCreate uint64
			pendingCheckpointDelete uint64
		)

		// Create a batch up front so write methods have a valid target.
		batch := b.readStore.NewBatch()
		b.initBatch(batch)

		// Iterate logs from Pebble and buffer index writes into the batch.
		for batchCount < b.batchSize {
			log, err := logsCursor.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					eof = true

					break
				}

				_ = batch.Cancel()

				return cursor, err
			}

			lastSeq = log.GetSequence()
			batchCount++

			if log.GetPayload() == nil {
				continue
			}

			// Handle ledger deletion: remove all read indexes for the deleted ledger.
			if dl, ok := log.GetPayload().GetType().(*commonpb.LogPayload_DeleteLedger); ok {
				if dl.DeleteLedger != nil {
					name := dl.DeleteLedger.GetName()
					if err := readstore.DeleteLedgerIndexes(batch, name); err != nil {
						_ = batch.Cancel()

						return cursor, err
					}

					b.markLedgerDeletedInBatch(name)
					// Live delete: evict the in-memory version state too so a
					// same-name recreate is treated as genuinely new. (The
					// backfill replay path deliberately does NOT do this — see
					// dropLedgerVersionState.)
					b.dropLedgerVersionState(name)
				}

				continue
			}

			// Handle query checkpoint creation: break batch so we can commit
			// pending writes, then create a physical Pebble checkpoint of the
			// read index at this exact point.
			if cqc, ok := log.GetPayload().GetType().(*commonpb.LogPayload_CreatedQueryCheckpoint); ok {
				pendingCheckpointCreate = cqc.CreatedQueryCheckpoint.GetCheckpointId()

				break
			}

			// Handle query checkpoint deletion: break batch so we can commit
			// pending writes, then remove the physical checkpoint files.
			if dqc, ok := log.GetPayload().GetType().(*commonpb.LogPayload_DeletedQueryCheckpoint); ok {
				pendingCheckpointDelete = dqc.DeletedQueryCheckpoint.GetCheckpointId()

				break
			}

			applyLog, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
			if !ok {
				continue
			}

			ledgerName := applyLog.Apply.GetLedgerName()
			ledgerLog := applyLog.Apply.GetLog()

			if ledgerLog == nil || ledgerLog.GetData() == nil {
				continue
			}

			cfg := b.ledgerConfig(ledgerName)
			var excludedVolumes map[domain.AccountAssetKey]struct{}
			if cfg.indexesPostingDerived() {
				excludedVolumes = proposals.excludedForLog(lastSeq, ledgerName, ledgerLog)
			}

			// Per-ledger log index is always maintained — every log gets a
			// LedgerLogKey(ledgerName, logID) → globalSeq entry. The controller's
			// ListLogs path relies on this being unconditional.
			if err := b.wb.WriteLedgerLogIndex(b.kb, ledgerName, ledgerLog.GetId(), log.GetSequence()); err != nil {
				_ = batch.Cancel()

				return cursor, err
			}

			// Index log date for date range filtering (opt-in via log date builtin index).
			if cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE) {
				if err := b.wb.WriteLedgerLogDateIndex(b.kb, ledgerName, ledgerLog.GetDate().GetData(), ledgerLog.GetId()); err != nil {
					_ = batch.Cancel()

					return cursor, err
				}
			}

			if err := b.indexPayload(b.kb, cfg, ledgerName, ledgerLog.GetData().GetPayload(), excludedVolumes); err != nil {
				_ = batch.Cancel()

				return cursor, err
			}
		}

		if batchCount == 0 {
			_ = batch.Cancel()

			break
		}

		// Commit the batch if there are index writes or a checkpoint action pending.
		hasCheckpointAction := pendingCheckpointCreate > 0 || pendingCheckpointDelete > 0
		if !b.wb.Empty() || hasCheckpointAction {
			// Write progress into the same batch before Flush commits it.
			if err := b.readStore.WriteProgress(batch, lastSeq); err != nil {
				_ = batch.Cancel()

				return cursor, err
			}

			// Persist only AppliedProposal entries whose log range is fully behind lastSeq.
			if err := persistAppliedProposalProgress(batch, lastSeq); err != nil {
				_ = batch.Cancel()

				return cursor, err
			}

			if err := b.wb.Flush(); err != nil {
				b.logger.WithFields(map[string]any{
					"batchSize": batchCount,
					"lastSeq":   lastSeq,
					"error":     err,
				}).Errorf("Error processing batch")

				return cursor, err
			}

			needsPersist = false
		} else {
			_ = batch.Cancel()
			b.wb.Reset()
			needsPersist = true
		}

		// Materialize a query checkpoint inline, at the exact moment the builder
		// crosses the CreatedQueryCheckpoint log — so the live read index
		// reflects precisely MaxSequence (the checkpoint's point-in-time). The
		// materialization is atomic (temp dir + fsync + rename + .ready marker
		// last), so a reader never observes a partial checkpoint. There is no
		// reconciler: historical reconstruction to a past MaxSequence is
		// infeasible (chapter log purge) and unnecessary (this inline point is
		// already exactly point-in-time). A node that crashes between rename and
		// marker, or that purged the logs before reaching this point, will never
		// have a marker for this checkpoint; the checkpoint stays registered, so
		// reads there return the retryable ErrCheckpointNotReady (Unavailable)
		// until the client deletes and recreates it (see openCheckpointStores).
		if cpID := pendingCheckpointCreate; cpID > 0 {
			if err := b.createReadIndexCheckpoint(cpID); err != nil {
				return cursor, err
			}
		}

		cursor = lastSeq
		b.lastIndexedSeq.Store(cursor)
		b.logsIndexed.Add(uint64(batchCount))
		b.readStore.NotifyProgress()

		if cpID := pendingCheckpointDelete; cpID > 0 {
			b.deleteReadIndexCheckpoint(cpID)
		}

		// Sample pebble last sequence from the cached atomic (written by the FSM
		// before signalling LogCommitted). This avoids opening a Pebble iterator
		// and deserializing a protobuf just to read a counter.
		if cached := b.notifications.LastSequence.Load(); cached > 0 {
			b.pebbleLastSeq.Store(cached)
		}

		// Periodic progress logging for long catch-up runs.
		if now := time.Now(); now.Sub(lastProgressLog) >= 10*time.Second {
			b.logger.WithFields(map[string]any{
				"cursor":  cursor,
				"from":    startCursor,
				"indexed": cursor - startCursor,
			}).Infof("processLogs progress")

			lastProgressLog = now
		}

		if eof {
			break
		}

		// Yield to backfills when a deadline is set.
		if !deadline.IsZero() && time.Now().After(deadline) {
			break
		}
	}

	// Persist progress once if we advanced the cursor without any index writes.
	// This reduces fsyncs from O(logs/batchSize) to O(1) when no indexes are active.
	if needsPersist {
		batch := b.readStore.NewBatch()
		if err := b.readStore.WriteProgress(batch, cursor); err != nil {
			_ = batch.Cancel()

			return cursor, fmt.Errorf("writing progress: %w", err)
		}

		if err := persistAppliedProposalProgress(batch, cursor); err != nil {
			_ = batch.Cancel()

			return cursor, fmt.Errorf("writing applied proposal progress: %w", err)
		}

		if err := batch.Commit(); err != nil {
			_ = batch.Cancel()

			return cursor, fmt.Errorf("committing progress: %w", err)
		}
	}

	return cursor, nil
}

// indexPayload dispatches a ledger log payload to the appropriate index handler.
func (b *Builder) indexPayload(
	kb *dal.KeyBuilder,
	cfg *ledgerIndexConfig,
	ledgerName string,
	payload any,
	excludedVolumes map[domain.AccountAssetKey]struct{},
) error {
	switch p := payload.(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		return b.indexCreatedTransaction(kb, cfg, ledgerName, p.CreatedTransaction, excludedVolumes)
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		return b.indexRevertedTransaction(kb, cfg, ledgerName, p.RevertedTransaction, excludedVolumes)
	case *commonpb.LedgerLogPayload_SavedMetadata:
		return b.indexSavedMetadata(kb, cfg, ledgerName, p.SavedMetadata)
	case *commonpb.LedgerLogPayload_DeletedMetadata:
		return b.indexDeletedMetadata(kb, cfg, ledgerName, p.DeletedMetadata)
	case *commonpb.LedgerLogPayload_SetMetadataFieldType:
		// Defer the rewrite to a background task instead of scanning
		// the reverse map inline during the hot path. addSchemaRewriteTask
		// also bumps pending_version in the current batch — propagate
		// any persistence error so the batch aborts.
		return b.addSchemaRewriteTask(cfg, ledgerName, p.SetMetadataFieldType)
	case *commonpb.LedgerLogPayload_RemovedMetadataFieldType:
		return b.handleRemovedMetadataFieldType(kb, cfg, ledgerName, p.RemovedMetadataFieldType)
	case *commonpb.LedgerLogPayload_CreateIndex:
		b.handleCreatedIndexLog(ledgerName, p.CreateIndex)
	case *commonpb.LedgerLogPayload_DropIndex:
		b.handleDroppedIndexLog(ledgerName, p.DropIndex)
	}

	return nil
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
// MaxSequence). There is no background reconciler.
func (b *Builder) createReadIndexCheckpoint(checkpointID uint64) (err error) {
	finalDir := b.pebbleStore.QueryCheckpointReadIndexDir(checkpointID)

	// Already materialized on this replica (redundant call). Nothing to do.
	if readstore.CheckpointDirReady(finalDir) {
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
	if err = readstore.FsyncDir(tmpDir); err != nil {
		return fmt.Errorf("fsync temp checkpoint %d: %w", checkpointID, err)
	}

	// Atomic rename into the final location: a reader never sees a partial dir.
	if err = os.Rename(tmpDir, finalDir); err != nil {
		return fmt.Errorf("renaming checkpoint %d into place: %w", checkpointID, err)
	}

	// fsync the parent so the rename is durable before we vouch for it.
	if err = readstore.FsyncDir(filepath.Dir(finalDir)); err != nil {
		return fmt.Errorf("fsync checkpoint %d parent: %w", checkpointID, err)
	}

	// Write the readiness marker LAST: readers (openCheckpointStores) only treat
	// the checkpoint as ready once this exists, guaranteeing a fully materialized,
	// atomically-renamed directory. (tmpDir is already renamed away, so the
	// deferred cleanup is a no-op if this fails; the markerless finalDir is then
	// treated as not-ready and rebuilt on the next attempt.)
	if err = readstore.MarkCheckpointReady(finalDir); err != nil {
		return fmt.Errorf("marking read index checkpoint %d ready: %w", checkpointID, err)
	}

	b.logger.WithFields(map[string]any{
		"checkpointID": checkpointID,
	}).Infof("Created read index query checkpoint")

	return nil
}

// deleteReadIndexCheckpoint removes the physical read index checkpoint files.
func (b *Builder) deleteReadIndexCheckpoint(checkpointID uint64) {
	destDir := b.pebbleStore.QueryCheckpointReadIndexDir(checkpointID)
	if err := os.RemoveAll(destDir); err != nil {
		b.logger.WithFields(map[string]any{
			"error":        err,
			"checkpointID": checkpointID,
		}).Infof("Failed to delete read index checkpoint files (may not exist)")
	}
}

// indexLogEntry dispatches a single log entry to the appropriate index handler.
// It does NOT call WriteProgress — the caller batches that.
// cfg is the index configuration to use for this log entry (may differ from
// b.indexConfig during backfill, where a temporary config is used).
func (b *Builder) indexLogEntry(cfg *ledgerIndexConfig, log *commonpb.Log, proposals *appliedProposalSync) error {
	if log.GetPayload() == nil {
		return nil
	}

	// Handle ledger deletion: remove all read indexes for the deleted ledger.
	if dl, ok := log.GetPayload().GetType().(*commonpb.LogPayload_DeleteLedger); ok {
		if dl.DeleteLedger != nil && b.wb.Batch() != nil {
			name := dl.DeleteLedger.GetName()
			if err := readstore.DeleteLedgerIndexes(b.wb.Batch(), name); err != nil {
				return err
			}

			b.markLedgerDeletedInBatch(name)
		}

		return nil
	}

	applyLog, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
	if !ok {
		return nil
	}

	ledgerName := applyLog.Apply.GetLedgerName()
	ledgerLog := applyLog.Apply.GetLog()
	if ledgerLog == nil || ledgerLog.GetData() == nil {
		return nil
	}

	excludedVolumes := proposals.excludedForLog(log.GetSequence(), ledgerName, ledgerLog)

	// Per-ledger log index is always maintained — see the live-path comment.
	if err := b.wb.WriteLedgerLogIndex(b.kb, ledgerName, ledgerLog.GetId(), log.GetSequence()); err != nil {
		return err
	}

	// Index log date for date range filtering (opt-in via log date builtin index).
	if cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE) {
		if err := b.wb.WriteLedgerLogDateIndex(b.kb, ledgerName, ledgerLog.GetDate().GetData(), ledgerLog.GetId()); err != nil {
			return err
		}
	}

	switch p := ledgerLog.GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		return b.indexCreatedTransaction(b.kb, cfg, ledgerName, p.CreatedTransaction, excludedVolumes)
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		return b.indexRevertedTransaction(b.kb, cfg, ledgerName, p.RevertedTransaction, excludedVolumes)
	case *commonpb.LedgerLogPayload_SavedMetadata:
		return b.indexSavedMetadata(b.kb, cfg, ledgerName, p.SavedMetadata)
	case *commonpb.LedgerLogPayload_DeletedMetadata:
		return b.indexDeletedMetadata(b.kb, cfg, ledgerName, p.DeletedMetadata)
	case *commonpb.LedgerLogPayload_SetMetadataFieldType:
		// Schema changes scan the reverse map with iterators — flush buffered
		// writes first so the iterators see a consistent state, then create a
		// new indexed batch for the rewrite.
		if err := b.wb.Flush(); err != nil {
			return err
		}

		batch := b.readStore.NewBatch()
		b.initBatch(batch) // re-init with a new batch after flush

		return b.indexSetMetadataFieldType(cfg, b.kb, ledgerName, p.SetMetadataFieldType)
	case *commonpb.LedgerLogPayload_CreateIndex:
		b.handleCreatedIndexLog(ledgerName, p.CreateIndex)
	case *commonpb.LedgerLogPayload_DropIndex:
		b.handleDroppedIndexLog(ledgerName, p.DropIndex)
	}

	return nil
}

// indexCreatedTransaction handles CreatedTransaction logs by indexing:
// - transaction existence
// - account existence (for all accounts in postings + account_metadata)
// - account metadata (from account_metadata)
// - transaction metadata (from transaction.metadata)
// - account→transaction mapping.
func (b *Builder) indexCreatedTransaction(
	kb *dal.KeyBuilder,
	cfg *ledgerIndexConfig,
	ledger string,
	ct *commonpb.CreatedTransaction,
	excludedVolumes map[domain.AccountAssetKey]struct{},
) error {
	if ct.GetTransaction() == nil {
		return nil
	}

	txn := ct.GetTransaction()

	wb := b.wb

	// Collect unique accounts from postings (reuse builder's map)
	indexAny := cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS)
	indexSource := cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS)
	indexDestination := cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS)

	clear(b.accounts)

	for _, posting := range txn.GetPostings() {
		b.accounts[posting.GetSource()] = struct{}{}
		b.accounts[posting.GetDestination()] = struct{}{}

		if err := b.indexPostingAddressMappings(
			kb, cfg, ledger, txn.GetId(), posting.GetSource(), posting.GetDestination(), posting.GetAsset(), posting.GetColor(),
			indexAny, indexSource, indexDestination, excludedVolumes,
		); err != nil {
			return err
		}
	}

	// Account metadata from account_metadata map. Look up the previous
	// encoding via the reverse-map (overlay + committed state) instead of
	// the log's PreviousValues so a same-batch create-then-overwrite
	// resolves correctly even when the schema rewrite is mid-flight.
	for account, metadataMap := range ct.GetAccountMetadata() {
		if metadataMap != nil {
			for key, value := range metadataMap.GetValues() {
				if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key) {
					continue
				}

				coerced, err := b.coerceForLedger(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, key, value)
				if err != nil {
					return err
				}
				newEncoded := readstore.EncodeMetadataValue(nil, coerced)

				if err := b.dualWriteMetadataIndex(
					kb,
					ledger, readstore.NamespaceAccount, key,
					commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					newEncoded, []byte(account),
					func(version uint32) []byte {
						return readstore.AccountReverseMapKeyV(kb, ledger, account, key, version)
					},
				); err != nil {
					return err
				}
			}
		}
	}

	// Transaction metadata. On live processLogs the rmap is empty for a
	// freshly-created tx, but a backfill replay (e.g. cursor reset on
	// retype) can land on a tx whose forward entries already exist under
	// the prior declared_type. Look up the existing encoded value so we
	// delete the stale forward key on the way in.
	if len(txn.GetMetadata()) > 0 {
		txIDBytes := make([]byte, 0, 8)

		txIDBytes = readstore.EncodeTxID(txIDBytes, txn.GetId())
		txID := txn.GetId()
		for key, value := range txn.GetMetadata() {
			if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key) {
				continue
			}

			coerced, err := b.coerceForLedger(ledger, commonpb.TargetType_TARGET_TYPE_TRANSACTION, key, value)
			if err != nil {
				return err
			}
			newEncoded := readstore.EncodeMetadataValue(nil, coerced)

			if err := b.dualWriteMetadataIndex(
				kb,
				ledger, readstore.NamespaceTransaction, key,
				commonpb.TargetType_TARGET_TYPE_TRANSACTION,
				newEncoded, txIDBytes,
				func(version uint32) []byte {
					return readstore.TransactionReverseMapKeyV(kb, ledger, txID, key, version)
				},
			); err != nil {
				return err
			}
		}
	}

	// Builtin indexes
	if cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE) && txn.GetReference() != "" {
		if err := wb.WriteTransactionReferenceIndex(kb, ledger, txn.GetReference(), txn.GetId()); err != nil {
			return err
		}
	}

	if cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP) && txn.GetTimestamp() != nil {
		if err := wb.WriteTransactionTimestampIndex(kb, ledger, txn.GetTimestamp().GetData(), txn.GetId()); err != nil {
			return err
		}
	}

	if cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT) && txn.GetInsertedAt() != nil {
		if err := wb.WriteTransactionInsertedAtIndex(kb, ledger, txn.GetInsertedAt().GetData(), txn.GetId()); err != nil {
			return err
		}
	}

	return nil
}

// indexRevertedTransaction handles RevertedTransaction logs by indexing:
// - revert transaction existence
// - account existence for revert postings
// - account→transaction mapping for revert postings.
func (b *Builder) indexRevertedTransaction(
	kb *dal.KeyBuilder,
	cfg *ledgerIndexConfig,
	ledger string,
	rt *commonpb.RevertedTransaction,
	excludedVolumes map[domain.AccountAssetKey]struct{},
) error {
	if rt.GetRevertTransaction() == nil {
		return nil
	}

	revertTxn := rt.GetRevertTransaction()
	wb := b.wb

	// Account→tx mapping for revert postings (reuse builder's map)
	indexAny := cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS)
	indexSource := cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS)
	indexDestination := cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS)

	clear(b.accounts)

	for _, posting := range revertTxn.GetPostings() {
		b.accounts[posting.GetSource()] = struct{}{}
		b.accounts[posting.GetDestination()] = struct{}{}

		if err := b.indexPostingAddressMappings(
			kb, cfg, ledger, revertTxn.GetId(), posting.GetSource(), posting.GetDestination(), posting.GetAsset(), posting.GetColor(),
			indexAny, indexSource, indexDestination, excludedVolumes,
		); err != nil {
			return err
		}
	}

	// Transaction metadata for the revert transaction. Same rationale as
	// indexCreatedTransaction: a backfill replay (cursor reset on retype)
	// can land on a tx whose forward entries already exist under the
	// prior declared_type, so look up the existing encoded value to
	// delete the stale forward key on the way in.
	if len(revertTxn.GetMetadata()) > 0 {
		txIDBytes := make([]byte, 0, 8)

		txIDBytes = readstore.EncodeTxID(txIDBytes, revertTxn.GetId())
		txID := revertTxn.GetId()
		for key, value := range revertTxn.GetMetadata() {
			if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key) {
				continue
			}

			coerced, err := b.coerceForLedger(ledger, commonpb.TargetType_TARGET_TYPE_TRANSACTION, key, value)
			if err != nil {
				return err
			}
			newEncoded := readstore.EncodeMetadataValue(nil, coerced)

			if err := b.dualWriteMetadataIndex(
				kb,
				ledger, readstore.NamespaceTransaction, key,
				commonpb.TargetType_TARGET_TYPE_TRANSACTION,
				newEncoded, txIDBytes,
				func(version uint32) []byte {
					return readstore.TransactionReverseMapKeyV(kb, ledger, txID, key, version)
				},
			); err != nil {
				return err
			}
		}
	}

	// Builtin indexes (no reference on revert transactions)
	if cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP) && revertTxn.GetTimestamp() != nil {
		if err := wb.WriteTransactionTimestampIndex(kb, ledger, revertTxn.GetTimestamp().GetData(), revertTxn.GetId()); err != nil {
			return err
		}
	}

	if cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT) && revertTxn.GetInsertedAt() != nil {
		if err := wb.WriteTransactionInsertedAtIndex(kb, ledger, revertTxn.GetInsertedAt().GetData(), revertTxn.GetId()); err != nil {
			return err
		}
	}

	// reverted_at indexes the original transaction (the one being reverted) by
	// the time it was reverted — the compensating transaction's timestamp.
	if cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT) && revertTxn.GetTimestamp() != nil {
		if err := wb.WriteTransactionRevertedAtIndex(kb, ledger, revertTxn.GetTimestamp().GetData(), rt.GetRevertedTransactionId()); err != nil {
			return err
		}
	}

	return nil
}

func (b *Builder) indexPostingAddressMappings(
	kb *dal.KeyBuilder,
	cfg *ledgerIndexConfig,
	ledger string,
	txID uint64,
	source string,
	destination string,
	asset string,
	color string,
	indexAny bool,
	indexSource bool,
	indexDestination bool,
	excludedVolumes map[domain.AccountAssetKey]struct{},
) error {
	wb := b.wb
	sourceExcluded := isExcluded(excludedVolumes, source, asset, color)
	destinationExcluded := isExcluded(excludedVolumes, destination, asset, color)

	// Account has-asset index: record every (account, assetBase, precision) a
	// posting touches, for both source and destination, skipping excluded
	// (transient/purged) volumes. Routed through the shared posting walk so
	// created and reverted transactions are both covered.
	if cfg.isAccountBuiltinIndexed(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET) {
		assetBase, precision := domain.ParseAssetPrecision(asset)

		if !sourceExcluded {
			if err := b.writeAccountByAssetDedup(kb, ledger, source, assetBase, precision); err != nil {
				return err
			}
		}

		if !destinationExcluded {
			if err := b.writeAccountByAssetDedup(kb, ledger, destination, assetBase, precision); err != nil {
				return err
			}
		}
	}

	if indexAny {
		if !sourceExcluded {
			if err := wb.WriteAccountTxMapping(kb, ledger, source, txID); err != nil {
				return err
			}
		}

		if !destinationExcluded {
			if err := wb.WriteAccountTxMapping(kb, ledger, destination, txID); err != nil {
				return err
			}
		}
	}

	// Role-specific mappings skip transient and purged ephemeral volumes —
	// matched per (account, asset) tuple so a multi-asset account keeps its
	// mappings for the assets that survived this proposal.
	if indexSource && !sourceExcluded {
		if err := wb.WriteSourceAccountTxMapping(kb, ledger, source, txID); err != nil {
			return err
		}
	}

	if indexDestination && !destinationExcluded {
		if err := wb.WriteDestinationAccountTxMapping(kb, ledger, destination, txID); err != nil {
			return err
		}
	}

	return nil
}

// writeAccountByAssetDedup writes an account-by-asset entry unless it already
// exists. The entry is presence-only so the Put is idempotent; the Get is purely
// to cut write amplification (the indexbuilder runs off the FSM hot path, so the
// extra read is acceptable). Dedup spans the in-flight batch (seenAcctAsset) and
// committed state (readstoreKeyExists).
func (b *Builder) writeAccountByAssetDedup(kb *dal.KeyBuilder, ledger, account, assetBase string, precision uint8) error {
	// AccountByAssetKey uses Build(), which returns a fresh copy and resets the
	// builder — so key does not alias the builder buffer and stays valid after
	// the WriteAccountByAssetIndex call below rebuilds the same key.
	key := readstore.AccountByAssetKey(kb, ledger, assetBase, precision, account)
	sk := string(key)

	if _, ok := b.seenAcctAsset[sk]; ok {
		return nil
	}

	b.seenAcctAsset[sk] = struct{}{}

	// If this ledger's indexes were range-deleted earlier in the same batch,
	// the committed-state read is stale: readstoreKeyExists reads committed
	// Pebble directly and cannot see the pending range delete, so it would
	// report the about-to-be-deleted row as present and suppress this Put —
	// which the range delete then wipes at commit, dropping the row. Force the
	// idempotent Put instead; queued after the range delete (the recreated
	// ledger's logs have higher sequence), it wins at commit.
	if _, deleted := b.deletedThisBatch[ledger]; !deleted {
		exists, err := b.readstoreKeyExists(key)
		if err != nil {
			return fmt.Errorf("account-by-asset dedup get: %w", err)
		}

		if exists {
			return nil
		}
	}

	return b.wb.WriteAccountByAssetIndex(kb, ledger, account, assetBase, precision)
}

// markLedgerDeletedInBatch records that ledger's read indexes were
// range-deleted in the in-flight batch (DeleteLedger) and drops the
// account-by-asset dedup set, which the deletion invalidates: any seenAcctAsset
// entry written before the delete now points at a row the range delete will
// remove. Subsequent dedup checks for this ledger skip committed-state reads
// (see deletedThisBatch in writeAccountByAssetDedup).
func (b *Builder) markLedgerDeletedInBatch(name string) {
	b.deletedThisBatch[name] = struct{}{}
	b.seenAcctAsset = make(map[string]struct{})
}

// dropLedgerVersionState evicts every in-memory per-index version state for a
// ledger — the whole-ledger counterpart of dropVersionState. It mirrors the
// persisted wipe DeleteLedgerIndexes performs on the SubInternalIndexVersion
// prefix, so a same-name recreate in the same process starts from a clean
// CurrentVersion == 0: otherwise the handleCreatedIndexLog readiness guard would
// read the dead generation's CurrentVersion != 0, skip seeding fresh state and
// scheduling the backfill, and strand the new index behind ErrIndexBuilding.
//
// This lives on the LIVE DeleteLedger apply path only (processLogs), NOT in
// markLedgerDeletedInBatch: the backfill replay path also calls that helper for
// a historical delete of the task ledger, where the in-progress version state
// tracks the RECREATED generation the backfill is building and must survive so
// completeBackfill can promote it.
func (b *Builder) dropLedgerVersionState(name string) {
	delete(b.indexVersions, name)
}

// readstoreKeyExists reports whether key is present in committed read-store
// state. A point Get on the underlying Pebble DB: (false, nil) on not-found,
// (true, nil) on hit. Mirrors reverseMapValue's committed-read path.
func (b *Builder) readstoreKeyExists(key []byte) (bool, error) {
	_, closer, err := b.readStore.DB().Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	_ = closer.Close()

	return true, nil
}

// reverseMapValue returns the encoded value the index currently holds for an
// entity+key (its reverse-map entry) — the authoritative target to delete on an
// overwrite. It reads the uncommitted-batch overlay first, then committed state.
// Returns nil when the entity has no current entry. Unlike the log's previous
// value, this matches the index's actual encoding even while a schema rewrite
// is re-encoding entries in the background.
func (b *Builder) reverseMapValue(reverseKey []byte) ([]byte, error) {
	if v, ok := b.wb.ReverseMapOverlay(reverseKey); ok {
		return v, nil
	}

	val, closer, err := b.readStore.DB().Get(reverseKey)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("reading reverse map: %w", err)
	}

	out := cloneBytes(val)
	_ = closer.Close()

	return out, nil
}

// indexSavedMetadata handles SavedMetadata logs.
func (b *Builder) indexSavedMetadata(
	kb *dal.KeyBuilder,
	cfg *ledgerIndexConfig,
	ledger string,
	sm *commonpb.SavedMetadata,
) error {
	if sm.GetTarget() == nil || len(sm.GetMetadata()) == 0 {
		return nil
	}

	switch t := sm.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		account := t.Account.GetAddr()

		for key, value := range sm.GetMetadata() {
			if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key) {
				continue
			}

			coerced, err := b.coerceForLedger(ledger, commonpb.TargetType_TARGET_TYPE_ACCOUNT, key, value)
			if err != nil {
				return err
			}
			newEncoded := readstore.EncodeMetadataValue(nil, coerced)

			if err := b.dualWriteMetadataIndex(
				kb,
				ledger, readstore.NamespaceAccount, key,
				commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				newEncoded, []byte(account),
				func(version uint32) []byte {
					return readstore.AccountReverseMapKeyV(kb, ledger, account, key, version)
				},
			); err != nil {
				return err
			}
		}
	case *commonpb.Target_TransactionId:
		txID := t.TransactionId
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, txID)

		for key, value := range sm.GetMetadata() {
			if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key) {
				continue
			}

			coerced, err := b.coerceForLedger(ledger, commonpb.TargetType_TARGET_TYPE_TRANSACTION, key, value)
			if err != nil {
				return err
			}
			newEncoded := readstore.EncodeMetadataValue(nil, coerced)

			if err := b.dualWriteMetadataIndex(
				kb,
				ledger, readstore.NamespaceTransaction, key,
				commonpb.TargetType_TARGET_TYPE_TRANSACTION,
				newEncoded, txIDBytes,
				func(version uint32) []byte {
					return readstore.TransactionReverseMapKeyV(kb, ledger, txID, key, version)
				},
			); err != nil {
				return err
			}
		}
	}

	return nil
}

// indexDeletedMetadata handles DeletedMetadata logs.
func (b *Builder) indexDeletedMetadata(
	kb *dal.KeyBuilder,
	cfg *ledgerIndexConfig,
	ledger string,
	dm *commonpb.DeletedMetadata,
) error {
	if dm.GetTarget() == nil {
		return nil
	}

	switch t := dm.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_ACCOUNT, dm.GetKey()) {
			return nil
		}

		account := t.Account.GetAddr()
		metaKey := dm.GetKey()

		return b.dualDeleteMetadataEntry(
			kb,
			ledger, readstore.NamespaceAccount, metaKey,
			commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			[]byte(account),
			func(version uint32) []byte {
				return readstore.AccountReverseMapKeyV(kb, ledger, account, metaKey, version)
			},
		)
	case *commonpb.Target_TransactionId:
		if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, dm.GetKey()) {
			return nil
		}

		txID := t.TransactionId
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, txID)
		metaKey := dm.GetKey()

		return b.dualDeleteMetadataEntry(
			kb,
			ledger, readstore.NamespaceTransaction, metaKey,
			commonpb.TargetType_TARGET_TYPE_TRANSACTION,
			txIDBytes,
			func(version uint32) []byte {
				return readstore.TransactionReverseMapKeyV(kb, ledger, txID, metaKey, version)
			},
		)
	}

	return nil
}

// indexSetMetadataFieldType handles schema change logs by re-encoding all
// inverted index entries for the affected key using the new type.
//
// Strategy: iterate the reverse map to find all entities that have this metadata key,
// then for each entity: delete the old forward index entry, convert the value,
// insert the new forward index entry, and update the reverse map.
//
// The WriteBatch must already be initialised with an indexed batch before calling
// this function. The caller is responsible for flushing the batch afterward.
func (b *Builder) indexSetMetadataFieldType(
	cfg *ledgerIndexConfig,
	kb *dal.KeyBuilder,
	ledger string,
	smft *commonpb.SetMetadataFieldTypeLog,
) error {
	// Only re-encode if this metadata key is indexed.
	if !cfg.isMetadataIndexed(smft.GetTargetType(), smft.GetKey()) {
		return nil
	}

	var ns string

	switch smft.GetTargetType() {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		ns = readstore.NamespaceAccount
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		ns = readstore.NamespaceTransaction
	default:
		return nil
	}

	// Build the reverse map prefix for scanning: [0x03][ledger\x00][ns:]
	rmapPrefix := readstore.ReverseMapPrefix(kb, ledger, ns)
	upper := readstore.IncrementBytes(rmapPrefix)

	// Use a Pebble snapshot so the scan sees committed data (not the
	// in-flight batch writes). The WriteBatch operates on an indexed batch,
	// but iterators from the batch would see partially applied state.
	snap := b.readStore.NewSnapshot()
	defer func() { _ = snap.Close() }()

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: rmapPrefix,
		UpperBound: upper,
	})
	if err != nil {
		return err
	}

	defer func() { _ = iter.Close() }()

	type rmapEntry struct {
		rmapKey  []byte // full reverse map key
		entityID []byte // account address or txID bytes
		oldValue []byte // old MetadataValue encoded via EncodeMetadataValue (sortable format)
	}

	var entries []rmapEntry

	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()

		// Strip the prefix byte (0x03) to get the portion after the prefix
		// that the extract helpers expect.
		suffixAfterByte := k[1:] // skip PrefixReverseMap byte
		metaKey := extractMetadataKeyFromReverseMap(suffixAfterByte, rmapPrefix[1:], ns)

		if metaKey != smft.GetKey() {
			continue
		}

		v, verr := iter.ValueAndErr()
		if verr != nil {
			return verr
		}

		entries = append(entries, rmapEntry{
			rmapKey:  cloneBytes(k),
			entityID: extractEntityIDFromReverseMap(suffixAfterByte, rmapPrefix[1:], ns),
			oldValue: cloneBytes(v),
		})
	}

	// For each entity: delete old forward index, convert, insert new forward index, update reverse map.
	// ReplaceMetadataIndex handles all four steps atomically within the batch.
	for _, e := range entries {
		// The reverse map stores values in the sortable EncodeMetadataValue
		// format, not protobuf — decode with the matching decoder. A failure
		// here means the stored bytes are corrupt; we treat it as fatal so
		// the schema-change task is retried rather than silently skipping
		// entries (which would leave inconsistent indexes).
		oldMV, _, err := readstore.DecodeValue(e.oldValue)
		if err != nil {
			return fmt.Errorf("decoding reverse map value for key %q: %w", smft.GetKey(), err)
		}

		oldEncoded := e.oldValue

		// Convert to new type.
		newMV := commonpb.ConvertMetadataValue(oldMV, smft.GetType())
		newEncoded := readstore.EncodeMetadataValue(nil, newMV)

		// ReplaceMetadataIndex deletes old midx+eidx, writes new midx+eidx,
		// and updates the reverse map — all in one call.
		if err := b.wb.ReplaceMetadataIndex(
			kb, e.rmapKey,
			ledger, ns, smft.GetKey(),
			newEncoded, oldEncoded, e.entityID,
		); err != nil {
			return err
		}
	}

	return nil
}

// cloneBytes returns a copy of the given byte slice.
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)

	return c
}

// extractMetadataKeyFromReverseMap extracts the metadata key name from a
// reverse map key, given the prefix up to the namespace.
// For accounts:     [ledger\x00][a:][account\x00][version:4B BE][metadataKey]
// For transactions: [ledger\x00][t:][txID(8B)][version:4B BE][metadataKey].
//
// The 4-byte forward-encoding version sitting between the entity
// delimiter and the metadata key was introduced as part of EN-1323's
// per-replica versioning; the entity scan helpers skip past it.
func extractMetadataKeyFromReverseMap(key, nsPrefix []byte, ns string) string {
	suffix := key[len(nsPrefix):]
	if ns == readstore.NamespaceAccount {
		// Find the null terminator after the account address, then
		// skip the 4-byte version prefix that precedes metadataKey.
		for i, b := range suffix {
			if b == 0x00 {
				rest := suffix[i+1:]
				if len(rest) < 4 {
					return ""
				}

				return string(rest[4:])
			}
		}

		return ""
	}
	// Transaction: skip 8-byte txID then 4-byte version.
	if len(suffix) > 12 {
		return string(suffix[12:])
	}

	return ""
}

// isExcluded returns true if the (account, asset, color) tuple is in the
// excluded set (transient or purged ephemeral). All three dimensions matter
// — a multi-bucket account may have one (asset, color) purged while another
// color of the same asset stays kept, and we must not over-skip mappings for
// the kept bucket.
func isExcluded(excluded map[domain.AccountAssetKey]struct{}, account, asset, color string) bool {
	if excluded == nil {
		return false
	}

	_, ok := excluded[domain.AccountAssetKey{Account: account, Asset: asset, Color: color}]

	return ok
}

// extractEntityIDFromReverseMap extracts the entity ID portion from a reverse map key.
func extractEntityIDFromReverseMap(key, nsPrefix []byte, ns string) []byte {
	suffix := key[len(nsPrefix):]
	if ns == readstore.NamespaceAccount {
		// Entity ID is the account address (up to \x00)
		for i, b := range suffix {
			if b == 0x00 {
				return suffix[:i]
			}
		}

		return suffix
	}
	// Transaction: entity ID is first 8 bytes
	if len(suffix) >= 8 {
		return suffix[:8]
	}

	return suffix
}

// parseReverseMapKey parses a full reverse-map key (including the
// PrefixReverseMap discriminator byte) and returns the (entityID,
// metaKey, version) tuple it encodes. ok=false when the key shape is
// incompatible with the namespace (corrupt key, scanning past a fresh
// version frontier with truncated tails).
//
// This is the single chokepoint for "the rmap key offset math" — any
// caller that needs more than one of the three fields should route
// through here instead of calling the individual extractors. The
// extract* helpers are kept as-is for callers that want only one
// field cheaply, but the multi-field sites (schema rewrite, GC, field
// removal) all read from this helper so an offset bug only has one
// place to break.
func parseReverseMapKey(k, rmapPrefix []byte, ns string) (entityID []byte, metaKey string, version uint32, ok bool) {
	if len(k) < 1 || len(rmapPrefix) < 1 {
		return nil, "", 0, false
	}

	suffix := k[1:]
	nsPrefix := rmapPrefix[1:]

	metaKey = extractMetadataKeyFromReverseMap(suffix, nsPrefix, ns)
	if metaKey == "" {
		// Either the key is corrupt or the suffix is too short to hold
		// a non-empty metaKey — caller treats both as "skip".
		return nil, "", 0, false
	}

	v, vOk := extractVersionFromReverseMap(suffix, nsPrefix, ns)
	if !vOk {
		return nil, "", 0, false
	}

	return extractEntityIDFromReverseMap(suffix, nsPrefix, ns), metaKey, v, true
}

// extractVersionFromReverseMap returns the 4-byte big-endian
// forward-encoding version embedded in a reverse map key suffix.
// Returns (0, false) when the suffix is too short or malformed —
// callers treat that as "skip this entry" rather than crash.
//
// Key layout (after stripping the PrefixReverseMap byte):
//   - Account:     [ledger\x00][a:][account\x00][version:4B BE][metadataKey]
//   - Transaction: [ledger\x00][t:][txID(8B)][version:4B BE][metadataKey]
func extractVersionFromReverseMap(key, nsPrefix []byte, ns string) (uint32, bool) {
	suffix := key[len(nsPrefix):]
	if ns == readstore.NamespaceAccount {
		for i, b := range suffix {
			if b == 0x00 {
				rest := suffix[i+1:]
				if len(rest) < 4 {
					return 0, false
				}

				return binary.BigEndian.Uint32(rest[:4]), true
			}
		}

		return 0, false
	}
	// Transaction: version sits at suffix[8:12].
	if len(suffix) >= 12 {
		return binary.BigEndian.Uint32(suffix[8:12]), true
	}

	return 0, false
}
