package indexbuilder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
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
func (b *Builder) processLogs(cursor uint64, deadline time.Time) (uint64, error) {
	handle, err := b.pebbleStore.NewDirectReadHandle()
	if err != nil {
		return cursor, fmt.Errorf("creating read handle for log processing: %w", err)
	}

	defer func() { _ = handle.Close() }()

	logsCursor, err := query.ReadLogsSince(context.Background(), handle, cursor, dal.WithReuse(), dal.WithResetFunc(resetLogForReuse))
	if err != nil {
		return cursor, err
	}

	defer func() { _ = logsCursor.Close() }()

	// Open audit iterator for transient account filtering.
	audit, err := newAuditSync(handle, b.lastAuditSeq)
	if err != nil {
		return cursor, fmt.Errorf("creating audit sync: %w", err)
	}

	defer func() { _ = audit.close() }()

	// Track whether we advanced the cursor without persisting it yet.
	needsPersist := false
	startCursor := cursor
	lastProgressLog := time.Now()

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

			// Handle ledger deletion: remove all read indexes for the deleted ledger.
			if dl, ok := log.GetPayload().GetType().(*commonpb.LogPayload_DeleteLedger); ok {
				if dl.DeleteLedger != nil {
					if err := readstore.DeleteLedgerIndexes(batch, dl.DeleteLedger.GetName()); err != nil {
						_ = batch.Cancel()

						return cursor, err
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

			// Sync audit iterator and load excluded accounts (transient + purged ephemeral).
			b.excludedAccounts = audit.syncTo(lastSeq, ledgerName)

			cfg := b.ledgerConfig(ledgerName)

			// Index ledger log for per-ledger listing (opt-in via log builtin index).
			if cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER) {
				if err := b.wb.WriteLedgerLogIndex(b.kb, ledgerName, ledgerLog.GetId(), log.GetSequence()); err != nil {
					_ = batch.Cancel()

					return cursor, err
				}
			}

			// Index log date for date range filtering (opt-in via log date builtin index).
			if cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE) {
				if err := b.wb.WriteLedgerLogDateIndex(b.kb, ledgerName, ledgerLog.GetDate().GetData(), ledgerLog.GetId()); err != nil {
					_ = batch.Cancel()

					return cursor, err
				}
			}

			if err := b.indexPayload(b.kb, cfg, ledgerName, ledgerLog.GetData().GetPayload()); err != nil {
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

			// Persist audit progress alongside log progress.
			if auditSeq := audit.auditSequence(); auditSeq > b.lastAuditSeq {
				if err := b.readStore.WriteAuditProgress(batch, auditSeq); err != nil {
					_ = batch.Cancel()

					return cursor, err
				}

				b.lastAuditSeq = auditSeq
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

		cursor = lastSeq
		b.lastIndexedSeq.Store(cursor)
		b.logsIndexed.Add(uint64(batchCount))
		b.readStore.NotifyProgress()

		// Create or delete a query checkpoint after the batch is committed,
		// so the physical Pebble checkpoint captures all indexed data up to this point.
		if cpID := pendingCheckpointCreate; cpID > 0 {
			if err := b.createReadIndexCheckpoint(cpID); err != nil {
				return cursor, err
			}
		}

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

		if auditSeq := audit.auditSequence(); auditSeq > b.lastAuditSeq {
			if err := b.readStore.WriteAuditProgress(batch, auditSeq); err != nil {
				_ = batch.Cancel()

				return cursor, fmt.Errorf("writing audit progress: %w", err)
			}

			b.lastAuditSeq = auditSeq
		}

		if err := batch.Commit(); err != nil {
			_ = batch.Cancel()

			return cursor, fmt.Errorf("committing progress: %w", err)
		}
	}

	return cursor, nil
}

// indexPayload dispatches a ledger log payload to the appropriate index handler.
func (b *Builder) indexPayload(kb *dal.KeyBuilder, cfg *ledgerIndexConfig, ledgerName string, payload any) error {
	switch p := payload.(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		return b.indexCreatedTransaction(kb, cfg, ledgerName, p.CreatedTransaction)
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		return b.indexRevertedTransaction(kb, cfg, ledgerName, p.RevertedTransaction)
	case *commonpb.LedgerLogPayload_SavedMetadata:
		return b.indexSavedMetadata(kb, cfg, ledgerName, p.SavedMetadata)
	case *commonpb.LedgerLogPayload_DeletedMetadata:
		return b.indexDeletedMetadata(kb, cfg, ledgerName, p.DeletedMetadata)
	case *commonpb.LedgerLogPayload_SetMetadataFieldType:
		// Defer the rewrite to a background task instead of scanning
		// the reverse map inline during the hot path.
		b.addSchemaRewriteTask(cfg, ledgerName, p.SetMetadataFieldType)
	case *commonpb.LedgerLogPayload_CreateIndex:
		b.handleCreateIndexLog(ledgerName, p.CreateIndex)
	case *commonpb.LedgerLogPayload_DropIndex:
		b.handleDropIndexLog(ledgerName, p.DropIndex)
	case *commonpb.LedgerLogPayload_IndexReady:
		b.handleIndexReadyLog(ledgerName, p.IndexReady)
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
	return b.processLogs(0, time.Time{})
}

// indexLogEntry dispatches a single log entry to the appropriate index handler.
// It does NOT call WriteProgress — the caller batches that.
// cfg is the index configuration to use for this log entry (may differ from
// b.indexConfig during backfill, where a temporary config is used).
func (b *Builder) indexLogEntry(cfg *ledgerIndexConfig, log *commonpb.Log) error {
	if log.GetPayload() == nil {
		return nil
	}

	// Handle ledger deletion: remove all read indexes for the deleted ledger.
	if dl, ok := log.GetPayload().GetType().(*commonpb.LogPayload_DeleteLedger); ok {
		if dl.DeleteLedger != nil && b.wb.Batch() != nil {
			return readstore.DeleteLedgerIndexes(b.wb.Batch(), dl.DeleteLedger.GetName())
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

	// Index ledger log for per-ledger listing (opt-in via log builtin index).
	if cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER) {
		if err := b.wb.WriteLedgerLogIndex(b.kb, ledgerName, ledgerLog.GetId(), log.GetSequence()); err != nil {
			return err
		}
	}

	// Index log date for date range filtering (opt-in via log date builtin index).
	if cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE) {
		if err := b.wb.WriteLedgerLogDateIndex(b.kb, ledgerName, ledgerLog.GetDate().GetData(), ledgerLog.GetId()); err != nil {
			return err
		}
	}

	switch p := ledgerLog.GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		return b.indexCreatedTransaction(b.kb, cfg, ledgerName, p.CreatedTransaction)
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		return b.indexRevertedTransaction(b.kb, cfg, ledgerName, p.RevertedTransaction)
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
		b.wb.Init(batch) // re-init with a new batch after flush

		return b.indexSetMetadataFieldType(cfg, b.kb, ledgerName, p.SetMetadataFieldType)
	case *commonpb.LedgerLogPayload_CreateIndex:
		b.handleCreateIndexLog(ledgerName, p.CreateIndex)
	case *commonpb.LedgerLogPayload_DropIndex:
		b.handleDropIndexLog(ledgerName, p.DropIndex)
	case *commonpb.LedgerLogPayload_IndexReady:
		b.handleIndexReadyLog(ledgerName, p.IndexReady)
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

	excluded := b.excludedAccounts

	for _, posting := range txn.GetPostings() {
		b.accounts[posting.GetSource()] = struct{}{}
		b.accounts[posting.GetDestination()] = struct{}{}

		srcExcluded := isExcluded(excluded, posting.GetSource())
		dstExcluded := isExcluded(excluded, posting.GetDestination())

		// Account→transaction mapping (any role) — skip transient accounts.
		if indexAny {
			if !srcExcluded {
				if err := wb.WriteAccountTxMapping(kb, ledger, posting.GetSource(), txn.GetId()); err != nil {
					return err
				}
			}

			if !dstExcluded {
				if err := wb.WriteAccountTxMapping(kb, ledger, posting.GetDestination(), txn.GetId()); err != nil {
					return err
				}
			}
		}
		// Role-specific mappings — skip transient accounts.
		if indexSrc && !srcExcluded {
			if err := wb.WriteSourceAccountTxMapping(kb, ledger, posting.GetSource(), txn.GetId()); err != nil {
				return err
			}
		}

		if indexDst && !dstExcluded {
			if err := wb.WriteDestAccountTxMapping(kb, ledger, posting.GetDestination(), txn.GetId()); err != nil {
				return err
			}
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
	ledger string,
	rt *commonpb.RevertedTransaction,
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

	excluded := b.excludedAccounts

	for _, posting := range revertTxn.GetPostings() {
		b.accounts[posting.GetSource()] = struct{}{}
		b.accounts[posting.GetDestination()] = struct{}{}

		srcExcluded := isExcluded(excluded, posting.GetSource())
		dstExcluded := isExcluded(excluded, posting.GetDestination())

		if indexAny {
			if !srcExcluded {
				if err := wb.WriteAccountTxMapping(kb, ledger, posting.GetSource(), revertTxn.GetId()); err != nil {
					return err
				}
			}

			if !dstExcluded {
				if err := wb.WriteAccountTxMapping(kb, ledger, posting.GetDestination(), revertTxn.GetId()); err != nil {
					return err
				}
			}
		}
		// Role-specific mappings — skip transient accounts.
		if indexSrc && !srcExcluded {
			if err := wb.WriteSourceAccountTxMapping(kb, ledger, posting.GetSource(), revertTxn.GetId()); err != nil {
				return err
			}
		}

		if indexDst && !dstExcluded {
			if err := wb.WriteDestAccountTxMapping(kb, ledger, posting.GetDestination(), revertTxn.GetId()); err != nil {
				return err
			}
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
	ledger string,
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
		oldValue []byte // old MetadataValue protobuf
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
		// Decode old MetadataValue.
		oldMV := &commonpb.MetadataValue{}
		if err := oldMV.UnmarshalVT(e.oldValue); err != nil {
			b.logger.WithFields(map[string]any{
				"key":   smft.GetKey(),
				"error": err,
			}).Errorf("Failed to unmarshal reverse map value during schema change")

			continue
		}

		oldEncoded := readstore.EncodeMetadataValue(nil, oldMV)

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
