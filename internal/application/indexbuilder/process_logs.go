package indexbuilder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/cockroachdb/pebble/v2"

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

	logsCursor, err := query.ReadLogsSince(ctx, handle, cursor, dal.WithReuse(), dal.WithResetFunc(resetLogForReuse))
	if err != nil {
		return cursor, err
	}

	defer func() { _ = logsCursor.Close() }()

	// Open audit iterator for transient account filtering.
	audit, err := newAuditSync(ctx, handle, b.lastAuditSeq)
	if err != nil {
		return cursor, fmt.Errorf("creating audit sync: %w", err)
	}

	defer func() { _ = audit.close() }()

	// Track whether we advanced the cursor without persisting it yet.
	needsPersist := false
	startCursor := cursor
	lastProgressLog := time.Now()
	persistAuditProgress := func(batch *dal.Batch, lastProcessedSeq uint64) error {
		audit.advanceBefore(lastProcessedSeq + 1)
		if auditSeq := audit.resumeSequence(); auditSeq > b.lastAuditSeq {
			if err := b.readStore.WriteAuditProgress(batch, auditSeq); err != nil {
				return err
			}

			b.lastAuditSeq = auditSeq
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
		b.wb.Init(batch)

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

			// Track CreateLedger for name → ID resolution.
			if cl, ok := log.GetPayload().GetType().(*commonpb.LogPayload_CreateLedger); ok && cl.CreateLedger != nil {
				b.ledgerNameToID[cl.CreateLedger.GetName()] = cl.CreateLedger.GetId()
			}

			// Handle ledger deletion: remove all read indexes for the deleted ledger.
			if dl, ok := log.GetPayload().GetType().(*commonpb.LogPayload_DeleteLedger); ok {
				if dl.DeleteLedger != nil {
					if ledgerID, ok := b.ledgerNameToID[dl.DeleteLedger.GetName()]; ok {
						if err := readstore.DeleteLedgerIndexes(batch, ledgerID); err != nil {
							_ = batch.Cancel()

							return cursor, err
						}
					}
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
			ledgerID := b.ledgerNameToID[ledgerName]
			var excludedAccounts map[string]struct{}
			if cfg.indexesPostingAddressMappings() {
				// Sync audit iterator and load excluded accounts (transient + purged ephemeral).
				excludedAccounts = audit.syncTo(lastSeq, ledgerName)
			}

			// Per-ledger log index is always maintained — every log gets a
			// LedgerLogKey(ledgerID, logID) → globalSeq entry. The controller's
			// ListLogs path relies on this being unconditional.
			if err := b.wb.WriteLedgerLogIndex(b.kb, ledgerID, ledgerLog.GetId(), log.GetSequence()); err != nil {
				_ = batch.Cancel()

				return cursor, err
			}

			// Index log date for date range filtering (opt-in via log date builtin index).
			if cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE) {
				if err := b.wb.WriteLedgerLogDateIndex(b.kb, ledgerID, ledgerLog.GetDate().GetData(), ledgerLog.GetId()); err != nil {
					_ = batch.Cancel()

					return cursor, err
				}
			}

			if err := b.indexPayload(b.kb, cfg, ledgerID, ledgerName, ledgerLog.GetData().GetPayload(), excludedAccounts); err != nil {
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

			// Persist only audit entries whose log range is fully behind lastSeq.
			if err := persistAuditProgress(batch, lastSeq); err != nil {
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

		// Create or delete a query checkpoint after the batch is committed
		// but BEFORE advancing the indexed sequence. This ensures the
		// checkpoint directory exists before readers are notified.
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

		if err := persistAuditProgress(batch, cursor); err != nil {
			_ = batch.Cancel()

			return cursor, fmt.Errorf("writing audit progress: %w", err)
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
	ledgerID uint32,
	ledgerName string,
	payload any,
	excludedAccounts map[string]struct{},
) error {
	switch p := payload.(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		return b.indexCreatedTransaction(kb, cfg, ledgerID, p.CreatedTransaction, excludedAccounts)
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		return b.indexRevertedTransaction(kb, cfg, ledgerID, p.RevertedTransaction, excludedAccounts)
	case *commonpb.LedgerLogPayload_SavedMetadata:
		return b.indexSavedMetadata(kb, cfg, ledgerID, p.SavedMetadata)
	case *commonpb.LedgerLogPayload_DeletedMetadata:
		return b.indexDeletedMetadata(kb, cfg, ledgerID, p.DeletedMetadata)
	case *commonpb.LedgerLogPayload_SetMetadataFieldType:
		// Defer the rewrite to a background task instead of scanning
		// the reverse map inline during the hot path.
		b.addSchemaRewriteTask(cfg, ledgerID, ledgerName, p.SetMetadataFieldType)
	case *commonpb.LedgerLogPayload_RemovedMetadataFieldType:
		return b.handleRemovedMetadataFieldType(kb, cfg, ledgerID, p.RemovedMetadataFieldType)
	case *commonpb.LedgerLogPayload_CreateIndex:
		b.handleCreatedIndexLog(ledgerName, ledgerID, p.CreateIndex)
	case *commonpb.LedgerLogPayload_DropIndex:
		b.handleDroppedIndexLog(ledgerName, p.DropIndex)
	}

	return nil
}

// createReadIndexCheckpoint creates a physical Pebble checkpoint of the read index
// for the given query checkpoint ID. Called after the batch containing all index
// data up to this point has been committed.
func (b *Builder) createReadIndexCheckpoint(checkpointID uint64) error {
	destDir := b.pebbleStore.QueryCheckpointReadIndexDir(checkpointID)
	if err := b.readStore.CreateCheckpoint(destDir); err != nil {
		return fmt.Errorf("creating read index checkpoint %d: %w", checkpointID, err)
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

// RebuildAll replays all system logs from scratch (starting at sequence 0),
// rebuilding the entire read index. This is intended for offline use
// (CLI backfill). Returns the last processed log sequence.
func (b *Builder) RebuildAll() (uint64, error) {
	return b.processLogs(context.Background(), 0, time.Time{})
}

// indexLogEntry dispatches a single log entry to the appropriate index handler.
// It does NOT call WriteProgress — the caller batches that.
// cfg is the index configuration to use for this log entry (may differ from
// b.indexConfig during backfill, where a temporary config is used).
func (b *Builder) indexLogEntry(cfg *ledgerIndexConfig, log *commonpb.Log, audit *auditSync) error {
	if log.GetPayload() == nil {
		return nil
	}

	// Handle ledger deletion: remove all read indexes for the deleted ledger.
	if dl, ok := log.GetPayload().GetType().(*commonpb.LogPayload_DeleteLedger); ok {
		if dl.DeleteLedger != nil && b.wb.Batch() != nil {
			if ledgerID, ok := b.ledgerNameToID[dl.DeleteLedger.GetName()]; ok {
				return readstore.DeleteLedgerIndexes(b.wb.Batch(), ledgerID)
			}
		}

		return nil
	}

	applyLog, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
	if !ok {
		return nil
	}

	ledgerName := applyLog.Apply.GetLedgerName()
	ledgerID := b.ledgerNameToID[ledgerName]
	var excludedAccounts map[string]struct{}
	if audit != nil {
		excludedAccounts = audit.syncTo(log.GetSequence(), ledgerName)
	}

	ledgerLog := applyLog.Apply.GetLog()
	if ledgerLog == nil || ledgerLog.GetData() == nil {
		return nil
	}

	// Per-ledger log index is always maintained — see the live-path comment.
	if err := b.wb.WriteLedgerLogIndex(b.kb, ledgerID, ledgerLog.GetId(), log.GetSequence()); err != nil {
		return err
	}

	// Index log date for date range filtering (opt-in via log date builtin index).
	if cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE) {
		if err := b.wb.WriteLedgerLogDateIndex(b.kb, ledgerID, ledgerLog.GetDate().GetData(), ledgerLog.GetId()); err != nil {
			return err
		}
	}

	switch p := ledgerLog.GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		return b.indexCreatedTransaction(b.kb, cfg, ledgerID, p.CreatedTransaction, excludedAccounts)
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		return b.indexRevertedTransaction(b.kb, cfg, ledgerID, p.RevertedTransaction, excludedAccounts)
	case *commonpb.LedgerLogPayload_SavedMetadata:
		return b.indexSavedMetadata(b.kb, cfg, ledgerID, p.SavedMetadata)
	case *commonpb.LedgerLogPayload_DeletedMetadata:
		return b.indexDeletedMetadata(b.kb, cfg, ledgerID, p.DeletedMetadata)
	case *commonpb.LedgerLogPayload_SetMetadataFieldType:
		// Schema changes scan the reverse map with iterators — flush buffered
		// writes first so the iterators see a consistent state, then create a
		// new indexed batch for the rewrite.
		if err := b.wb.Flush(); err != nil {
			return err
		}

		batch := b.readStore.NewBatch()
		b.wb.Init(batch) // re-init with a new batch after flush

		return b.indexSetMetadataFieldType(cfg, b.kb, ledgerID, p.SetMetadataFieldType)
	case *commonpb.LedgerLogPayload_CreateIndex:
		b.handleCreatedIndexLog(ledgerName, ledgerID, p.CreateIndex)
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
	ledger uint32,
	ct *commonpb.CreatedTransaction,
	excludedAccounts map[string]struct{},
) error {
	if ct.GetTransaction() == nil {
		return nil
	}

	txn := ct.GetTransaction()

	wb := b.wb

	// Collect unique accounts from postings (reuse builder's map)
	indexAny := cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS)
	indexSrc := cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS)
	indexDst := cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS)

	clear(b.accounts)

	for _, posting := range txn.GetPostings() {
		b.accounts[posting.GetSource()] = struct{}{}
		b.accounts[posting.GetDestination()] = struct{}{}

		if err := b.indexPostingAddressMappings(
			kb, ledger, txn.GetId(), posting.GetSource(), posting.GetDestination(),
			indexAny, indexSrc, indexDst, excludedAccounts,
		); err != nil {
			return err
		}
	}

	// Account metadata from account_metadata map
	prevAcctMeta := ct.GetPreviousAccountMetadata()

	for account, metadataMap := range ct.GetAccountMetadata() {
		if metadataMap != nil {
			for key, value := range metadataMap.GetValues() {
				if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key) {
					continue
				}

				reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, key)
				newEncoded := readstore.EncodeMetadataValue(nil, value)
				oldEncoded := lookupPreviousAccountValue(prevAcctMeta, account, key)

				if err := wb.ReplaceMetadataIndex(
					kb, reverseKey,
					ledger, readstore.NamespaceAccount, key,
					newEncoded, oldEncoded, []byte(account),
				); err != nil {
					return err
				}
			}
		}
	}

	// Transaction metadata (first write for new tx — no previous values)
	if len(txn.GetMetadata()) > 0 {
		txIDBytes := make([]byte, 0, 8)

		txIDBytes = readstore.EncodeTxID(txIDBytes, txn.GetId())
		for key, value := range txn.GetMetadata() {
			if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key) {
				continue
			}

			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txn.GetId(), key)
			newEncoded := readstore.EncodeMetadataValue(nil, value)

			if err := wb.ReplaceMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceTransaction, key,
				newEncoded, nil, txIDBytes,
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
	ledger uint32,
	rt *commonpb.RevertedTransaction,
	excludedAccounts map[string]struct{},
) error {
	if rt.GetRevertTransaction() == nil {
		return nil
	}

	revertTxn := rt.GetRevertTransaction()
	wb := b.wb

	// Account→tx mapping for revert postings (reuse builder's map)
	indexAny := cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS)
	indexSrc := cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS)
	indexDst := cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS)

	clear(b.accounts)

	for _, posting := range revertTxn.GetPostings() {
		b.accounts[posting.GetSource()] = struct{}{}
		b.accounts[posting.GetDestination()] = struct{}{}

		if err := b.indexPostingAddressMappings(
			kb, ledger, revertTxn.GetId(), posting.GetSource(), posting.GetDestination(),
			indexAny, indexSrc, indexDst, excludedAccounts,
		); err != nil {
			return err
		}
	}

	// Transaction metadata for the revert transaction (first write — no previous values)
	if len(revertTxn.GetMetadata()) > 0 {
		txIDBytes := make([]byte, 0, 8)

		txIDBytes = readstore.EncodeTxID(txIDBytes, revertTxn.GetId())
		for key, value := range revertTxn.GetMetadata() {
			if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key) {
				continue
			}

			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, revertTxn.GetId(), key)
			newEncoded := readstore.EncodeMetadataValue(nil, value)

			if err := wb.ReplaceMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceTransaction, key,
				newEncoded, nil, txIDBytes,
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

	return nil
}

func (b *Builder) indexPostingAddressMappings(
	kb *dal.KeyBuilder,
	ledger uint32,
	txID uint64,
	source string,
	destination string,
	indexAny bool,
	indexSrc bool,
	indexDst bool,
	excludedAccounts map[string]struct{},
) error {
	wb := b.wb
	srcExcluded := isExcluded(excludedAccounts, source)
	dstExcluded := isExcluded(excludedAccounts, destination)

	if indexAny {
		if !srcExcluded {
			if err := wb.WriteAccountTxMapping(kb, ledger, source, txID); err != nil {
				return err
			}
		}

		if !dstExcluded {
			if err := wb.WriteAccountTxMapping(kb, ledger, destination, txID); err != nil {
				return err
			}
		}
	}

	// Role-specific mappings skip transient and purged ephemeral accounts.
	if indexSrc && !srcExcluded {
		if err := wb.WriteSourceAccountTxMapping(kb, ledger, source, txID); err != nil {
			return err
		}
	}

	if indexDst && !dstExcluded {
		if err := wb.WriteDestAccountTxMapping(kb, ledger, destination, txID); err != nil {
			return err
		}
	}

	return nil
}

// indexSavedMetadata handles SavedMetadata logs.
func (b *Builder) indexSavedMetadata(
	kb *dal.KeyBuilder,
	cfg *ledgerIndexConfig,
	ledger uint32,
	sm *commonpb.SavedMetadata,
) error {
	if sm.GetTarget() == nil || len(sm.GetMetadata()) == 0 {
		return nil
	}

	wb := b.wb
	prevValues := sm.GetPreviousValues()

	switch t := sm.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		account := t.Account.GetAddr()

		for key, value := range sm.GetMetadata() {
			if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key) {
				continue
			}

			reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, key)
			newEncoded := readstore.EncodeMetadataValue(nil, value)
			var oldEncoded []byte
			if pv, ok := prevValues[key]; ok && pv != nil {
				oldEncoded = readstore.EncodeMetadataValue(nil, pv)
			}

			if err := wb.ReplaceMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceAccount, key,
				newEncoded, oldEncoded, []byte(account),
			); err != nil {
				return err
			}
		}
	case *commonpb.Target_Transaction:
		txID := t.Transaction.GetId()
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, txID)

		for key, value := range sm.GetMetadata() {
			if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key) {
				continue
			}

			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txID, key)
			newEncoded := readstore.EncodeMetadataValue(nil, value)
			var oldEncoded []byte
			if pv, ok := prevValues[key]; ok && pv != nil {
				oldEncoded = readstore.EncodeMetadataValue(nil, pv)
			}

			if err := wb.ReplaceMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceTransaction, key,
				newEncoded, oldEncoded, txIDBytes,
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
	ledger uint32,
	dm *commonpb.DeletedMetadata,
) error {
	if dm.GetTarget() == nil {
		return nil
	}

	wb := b.wb

	var oldEncoded []byte
	if dm.GetPreviousValue() != nil {
		oldEncoded = readstore.EncodeMetadataValue(nil, dm.GetPreviousValue())
	}

	switch t := dm.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_ACCOUNT, dm.GetKey()) {
			return nil
		}

		account := t.Account.GetAddr()
		reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, dm.GetKey())

		return wb.DeleteMetadataEntryWithPrevious(
			kb, reverseKey,
			ledger, readstore.NamespaceAccount, dm.GetKey(),
			oldEncoded, []byte(account),
		)
	case *commonpb.Target_Transaction:
		if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, dm.GetKey()) {
			return nil
		}

		txID := t.Transaction.GetId()
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, txID)
		reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txID, dm.GetKey())

		return wb.DeleteMetadataEntryWithPrevious(
			kb, reverseKey,
			ledger, readstore.NamespaceTransaction, dm.GetKey(),
			oldEncoded, txIDBytes,
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
	ledger uint32,
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
// For accounts:     [ledger\x00][a:][account\x00][metadataKey]
// For transactions: [ledger\x00][t:][txID(8B)][metadataKey].
func extractMetadataKeyFromReverseMap(key, nsPrefix []byte, ns string) string {
	suffix := key[len(nsPrefix):]
	if ns == readstore.NamespaceAccount {
		// Find the null terminator after the account address
		for i, b := range suffix {
			if b == 0x00 {
				return string(suffix[i+1:])
			}
		}

		return ""
	}
	// Transaction: skip 8-byte txID
	if len(suffix) > 8 {
		return string(suffix[8:])
	}

	return ""
}

// isExcluded returns true if the account is in the excluded set
// (transient or purged ephemeral).
func isExcluded(excluded map[string]struct{}, account string) bool {
	if excluded == nil {
		return false
	}

	_, ok := excluded[account]

	return ok
}

// lookupPreviousAccountValue looks up the encoded previous value for a metadata key
// from the previous_account_metadata map in the log.
func lookupPreviousAccountValue(prevAcctMeta map[string]*commonpb.MetadataMap, account, metadataKey string) []byte {
	if prevAcctMeta == nil {
		return nil
	}

	prevMap, ok := prevAcctMeta[account]
	if !ok || prevMap == nil {
		return nil
	}

	if value, found := prevMap.GetValues()[metadataKey]; found && value != nil {
		return readstore.EncodeMetadataValue(nil, value)
	}

	return nil
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
