package indexbuilder

import (
	"context"
	"errors"
	"io"
	"time"

	bolt "go.etcd.io/bbolt"

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
// the log (no bbolt reads needed), processing uses a 2-pass design:
//   - Pass 1 (no bbolt tx): iterate Pebble logs, dispatch to handlers that
//     buffer writes into WriteBatch.
//   - Pass 2 (bbolt tx, only if needed): Init(tx), Flush, persist progress.
//
// When a batch produces no index writes, the bbolt transaction is skipped
// entirely. Progress is persisted once at the end, reducing fsyncs to O(1).
func (b *Builder) processLogs(cursor uint64, deadline time.Time) (uint64, error) {
	logsCursor, err := query.ReadLogsSince(context.Background(), b.pebbleStore, cursor, dal.WithReuse(), dal.WithResetFunc(resetLogForReuse))
	if err != nil {
		return cursor, err
	}

	defer func() { _ = logsCursor.Close() }()

	// Track whether we advanced the cursor without persisting it yet.
	needsPersist := false
	startCursor := cursor
	lastProgressLog := time.Now()

	for {
		var (
			batchCount int
			lastSeq    uint64
			eof        bool
		)

		// Pass 1: iterate logs from Pebble and buffer index writes.
		for batchCount < b.batchSize {
			log, err := logsCursor.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					eof = true

					break
				}

				return cursor, err
			}

			lastSeq = log.GetSequence()
			batchCount++

			if log.GetPayload() == nil {
				continue
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

			// Index ledger log for per-ledger listing (opt-in via log builtin index).
			if cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER) {
				b.wb.WriteLedgerLogIndex(b.kb, ledgerName, ledgerLog.GetId(), log.GetSequence())
			}

			// Index log date for date range filtering (opt-in via log date builtin index).
			if cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE) {
				b.wb.WriteLedgerLogDateIndex(b.kb, ledgerName, ledgerLog.GetDate().GetData(), ledgerLog.GetId())
			}

			switch p := ledgerLog.GetData().GetPayload().(type) {
			case *commonpb.LedgerLogPayload_CreatedTransaction:
				b.indexCreatedTransaction(b.kb, cfg, ledgerName, p.CreatedTransaction)
			case *commonpb.LedgerLogPayload_RevertedTransaction:
				b.indexRevertedTransaction(b.kb, cfg, ledgerName, p.RevertedTransaction)
			case *commonpb.LedgerLogPayload_SavedMetadata:
				b.indexSavedMetadata(b.kb, cfg, ledgerName, p.SavedMetadata)
			case *commonpb.LedgerLogPayload_DeletedMetadata:
				b.indexDeletedMetadata(b.kb, cfg, ledgerName, p.DeletedMetadata)
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
		}

		if batchCount == 0 {
			break
		}

		// Pass 2: flush buffered writes to bbolt (only if there are index writes).
		if !b.wb.Empty() {
			txErr := b.readStore.Update(func(tx *bolt.Tx) error {
				b.wb.Init(tx)

				if err := b.wb.Flush(); err != nil {
					return err
				}

				return b.persistProgress(tx, lastSeq)
			})

			if txErr != nil {
				b.logger.WithFields(map[string]any{
					"batchSize": batchCount,
					"lastSeq":   lastSeq,
					"error":     txErr,
				}).Errorf("Error processing batch")

				return cursor, txErr
			}

			needsPersist = false
		} else {
			b.wb.Reset()
			needsPersist = true
		}

		cursor = lastSeq
		b.lastIndexedSeq.Store(cursor)
		b.logsIndexed.Add(uint64(batchCount))
		b.readStore.NotifyProgress()

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
		_ = b.readStore.Update(func(tx *bolt.Tx) error {
			return b.persistProgress(tx, cursor)
		})
	}

	return cursor, nil
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
func (b *Builder) indexLogEntry(tx *bolt.Tx, cfg *ledgerIndexConfig, log *commonpb.Log) error {
	if log.GetPayload() == nil {
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
		b.wb.WriteLedgerLogIndex(b.kb, ledgerName, ledgerLog.GetId(), log.GetSequence())
	}

	// Index log date for date range filtering (opt-in via log date builtin index).
	if cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE) {
		b.wb.WriteLedgerLogDateIndex(b.kb, ledgerName, ledgerLog.GetDate().GetData(), ledgerLog.GetId())
	}

	switch p := ledgerLog.GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		b.indexCreatedTransaction(b.kb, cfg, ledgerName, p.CreatedTransaction)
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		b.indexRevertedTransaction(b.kb, cfg, ledgerName, p.RevertedTransaction)
	case *commonpb.LedgerLogPayload_SavedMetadata:
		b.indexSavedMetadata(b.kb, cfg, ledgerName, p.SavedMetadata)
	case *commonpb.LedgerLogPayload_DeletedMetadata:
		b.indexDeletedMetadata(b.kb, cfg, ledgerName, p.DeletedMetadata)
	case *commonpb.LedgerLogPayload_SetMetadataFieldType:
		// Schema changes scan buckets directly with cursors — flush buffered
		// writes first so the cursors see a consistent state.
		if err := b.wb.Flush(); err != nil {
			return err
		}

		b.wb.Init(tx) // re-init after flush (Flush calls Reset)

		return b.indexSetMetadataFieldType(tx, cfg, b.kb, ledgerName, p.SetMetadataFieldType)
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
) {
	if ct.GetTransaction() == nil {
		return
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

		// Account→transaction mapping (any role)
		if indexAny {
			wb.WriteAccountTxMapping(kb, ledger, posting.GetSource(), txn.GetId())
			wb.WriteAccountTxMapping(kb, ledger, posting.GetDestination(), txn.GetId())
		}
		// Role-specific mappings
		if indexSrc {
			wb.WriteSourceAccountTxMapping(kb, ledger, posting.GetSource(), txn.GetId())
		}

		if indexDst {
			wb.WriteDestAccountTxMapping(kb, ledger, posting.GetDestination(), txn.GetId())
		}
	}

	// Account metadata from account_metadata map
	prevAcctMeta := ct.GetPreviousAccountMetadata()

	for account, metadataSet := range ct.GetAccountMetadata() {
		if metadataSet != nil {
			for _, md := range metadataSet.GetMetadata() {
				if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_ACCOUNT, md.GetKey()) {
					continue
				}

				reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, md.GetKey())
				newEncoded := readstore.EncodeMetadataValue(nil, md.GetValue())
				oldEncoded := lookupPreviousAccountValue(prevAcctMeta, account, md.GetKey())
				wb.ReplaceMetadataIndex(
					kb, reverseKey,
					ledger, readstore.NamespaceAccount, md.GetKey(),
					newEncoded, oldEncoded, []byte(account),
				)
			}
		}
	}

	// Transaction metadata (first write for new tx — no previous values)
	if txn.GetMetadata() != nil {
		txIDBytes := make([]byte, 0, 8)

		txIDBytes = readstore.EncodeTxID(txIDBytes, txn.GetId())
		for _, md := range txn.GetMetadata().GetMetadata() {
			if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, md.GetKey()) {
				continue
			}

			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txn.GetId(), md.GetKey())
			newEncoded := readstore.EncodeMetadataValue(nil, md.GetValue())
			wb.ReplaceMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceTransaction, md.GetKey(),
				newEncoded, nil, txIDBytes,
			)
		}
	}

	// Builtin indexes
	if cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE) && txn.GetReference() != "" {
		wb.WriteTransactionReferenceIndex(kb, ledger, txn.GetReference(), txn.GetId())
	}

	if cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP) && txn.GetTimestamp() != nil {
		wb.WriteTransactionTimestampIndex(kb, ledger, txn.GetTimestamp().GetData(), txn.GetId())
	}
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
) {
	if rt.GetRevertTransaction() == nil {
		return
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
		if indexAny {
			wb.WriteAccountTxMapping(kb, ledger, posting.GetSource(), revertTxn.GetId())
			wb.WriteAccountTxMapping(kb, ledger, posting.GetDestination(), revertTxn.GetId())
		}
		// Role-specific mappings
		if indexSrc {
			wb.WriteSourceAccountTxMapping(kb, ledger, posting.GetSource(), revertTxn.GetId())
		}

		if indexDst {
			wb.WriteDestAccountTxMapping(kb, ledger, posting.GetDestination(), revertTxn.GetId())
		}
	}

	// Transaction metadata for the revert transaction (first write — no previous values)
	if revertTxn.GetMetadata() != nil {
		txIDBytes := make([]byte, 0, 8)

		txIDBytes = readstore.EncodeTxID(txIDBytes, revertTxn.GetId())
		for _, md := range revertTxn.GetMetadata().GetMetadata() {
			if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, md.GetKey()) {
				continue
			}

			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, revertTxn.GetId(), md.GetKey())
			newEncoded := readstore.EncodeMetadataValue(nil, md.GetValue())
			wb.ReplaceMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceTransaction, md.GetKey(),
				newEncoded, nil, txIDBytes,
			)
		}
	}

	// Builtin indexes (no reference on revert transactions)
	if cfg.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP) && revertTxn.GetTimestamp() != nil {
		wb.WriteTransactionTimestampIndex(kb, ledger, revertTxn.GetTimestamp().GetData(), revertTxn.GetId())
	}
}

// indexSavedMetadata handles SavedMetadata logs.
func (b *Builder) indexSavedMetadata(
	kb *dal.KeyBuilder,
	cfg *ledgerIndexConfig,
	ledger string,
	sm *commonpb.SavedMetadata,
) {
	if sm.GetTarget() == nil || sm.GetMetadata() == nil {
		return
	}

	wb := b.wb
	prevValues := sm.GetPreviousValues()

	switch t := sm.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		account := t.Account.GetAddr()

		for _, md := range sm.GetMetadata().GetMetadata() {
			if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_ACCOUNT, md.GetKey()) {
				continue
			}

			reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, md.GetKey())
			newEncoded := readstore.EncodeMetadataValue(nil, md.GetValue())
			var oldEncoded []byte
			if pv, ok := prevValues[md.GetKey()]; ok && pv != nil {
				oldEncoded = readstore.EncodeMetadataValue(nil, pv)
			}
			wb.ReplaceMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceAccount, md.GetKey(),
				newEncoded, oldEncoded, []byte(account),
			)
		}
	case *commonpb.Target_Transaction:
		txID := t.Transaction.GetId()
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, txID)

		for _, md := range sm.GetMetadata().GetMetadata() {
			if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, md.GetKey()) {
				continue
			}

			reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txID, md.GetKey())
			newEncoded := readstore.EncodeMetadataValue(nil, md.GetValue())
			var oldEncoded []byte
			if pv, ok := prevValues[md.GetKey()]; ok && pv != nil {
				oldEncoded = readstore.EncodeMetadataValue(nil, pv)
			}
			wb.ReplaceMetadataIndex(
				kb, reverseKey,
				ledger, readstore.NamespaceTransaction, md.GetKey(),
				newEncoded, oldEncoded, txIDBytes,
			)
		}
	}
}

// indexDeletedMetadata handles DeletedMetadata logs.
func (b *Builder) indexDeletedMetadata(
	kb *dal.KeyBuilder,
	cfg *ledgerIndexConfig,
	ledger string,
	dm *commonpb.DeletedMetadata,
) {
	if dm.GetTarget() == nil {
		return
	}

	wb := b.wb

	var oldEncoded []byte
	if dm.GetPreviousValue() != nil {
		oldEncoded = readstore.EncodeMetadataValue(nil, dm.GetPreviousValue())
	}

	switch t := dm.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_ACCOUNT, dm.GetKey()) {
			return
		}

		account := t.Account.GetAddr()
		reverseKey := readstore.AccountReverseMapKey(kb, ledger, account, dm.GetKey())
		wb.DeleteMetadataEntryWithPrevious(
			kb, reverseKey,
			ledger, readstore.NamespaceAccount, dm.GetKey(),
			oldEncoded, []byte(account),
		)
	case *commonpb.Target_Transaction:
		if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, dm.GetKey()) {
			return
		}

		txID := t.Transaction.GetId()
		txIDBytes := make([]byte, 0, 8)
		txIDBytes = readstore.EncodeTxID(txIDBytes, txID)
		reverseKey := readstore.TransactionReverseMapKey(kb, ledger, txID, dm.GetKey())
		wb.DeleteMetadataEntryWithPrevious(
			kb, reverseKey,
			ledger, readstore.NamespaceTransaction, dm.GetKey(),
			oldEncoded, txIDBytes,
		)
	}
}

// indexSetMetadataFieldType handles schema change logs by re-encoding all
// inverted index entries for the affected key using the new type.
//
// Strategy: iterate the reverse map to find all entities that have this metadata key,
// then for each entity: delete the old forward index entry, convert the value,
// insert the new forward index entry, and update the reverse map.
func (b *Builder) indexSetMetadataFieldType(
	tx *bolt.Tx,
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

	midxBucket := tx.Bucket(readstore.BucketMetadataIndex)
	eidxBucket := tx.Bucket(readstore.BucketEntityExists)
	rmapBucket := tx.Bucket(readstore.BucketReverseMap)

	// Iterate the reverse map for this namespace to find all entities with the key.
	rmapPrefix := kb.Reset().
		PutLedgerName(ledger).
		PutNamespace(ns).
		Snapshot()

	type rmapEntry struct {
		rmapKey  []byte // full reverse map key
		entityID []byte // account address or txID bytes
		oldValue []byte // old MetadataValue protobuf
	}

	var entries []rmapEntry

	rc := rmapBucket.Cursor()
	for k, v := rc.Seek(rmapPrefix); k != nil && readstore.HasPrefix(k, rmapPrefix); k, v = rc.Next() {
		metaKey := extractMetadataKeyFromReverseMap(k, rmapPrefix, ns)
		if metaKey != smft.GetKey() {
			continue
		}

		entries = append(entries, rmapEntry{
			rmapKey:  cloneBytes(k),
			entityID: extractEntityIDFromReverseMap(k, rmapPrefix, ns),
			oldValue: cloneBytes(v),
		})
	}

	// For each entity: delete old forward index, convert, insert new forward index, update reverse map.
	for _, e := range entries {
		// Decode old MetadataValue
		oldMV := &commonpb.MetadataValue{}
		if err := oldMV.UnmarshalVT(e.oldValue); err != nil {
			b.logger.WithFields(map[string]any{
				"key":   smft.GetKey(),
				"error": err,
			}).Errorf("Failed to unmarshal reverse map value during schema change")

			continue
		}

		// Delete old forward index entry
		oldEncoded := readstore.EncodeMetadataValue(nil, oldMV)

		oldKey := readstore.MetadataIndexKey(kb, ledger, ns, smft.GetKey(), oldEncoded, e.entityID)
		if err := midxBucket.Delete(oldKey); err != nil {
			return err
		}

		// Convert to new type
		newMV := commonpb.ConvertMetadataValue(oldMV, smft.GetType())
		newEncoded := readstore.EncodeMetadataValue(nil, newMV)

		// Update eidx if null status changed
		oldIsNull := len(oldEncoded) > 0 && oldEncoded[0] == readstore.TypeTagNull

		newIsNull := len(newEncoded) > 0 && newEncoded[0] == readstore.TypeTagNull
		if oldIsNull != newIsNull {
			oldEidxKey := readstore.EntityExistsKey(kb, ledger, ns, smft.GetKey(), oldIsNull, e.entityID)
			if err := eidxBucket.Delete(oldEidxKey); err != nil {
				return err
			}

			newEidxKey := readstore.EntityExistsKey(kb, ledger, ns, smft.GetKey(), newIsNull, e.entityID)
			if err := eidxBucket.Put(newEidxKey, nil); err != nil {
				return err
			}
		}

		// Write new forward index entry
		newKey := readstore.MetadataIndexKey(kb, ledger, ns, smft.GetKey(), newEncoded, e.entityID)
		if err := midxBucket.Put(newKey, nil); err != nil {
			return err
		}

		// Update reverse map with new value
		newMVBytes, err := newMV.MarshalVT()
		if err != nil {
			return err
		}

		if err := rmapBucket.Put(e.rmapKey, newMVBytes); err != nil {
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

// lookupPreviousAccountValue looks up the encoded previous value for a metadata key
// from the previous_account_metadata map in the log.
func lookupPreviousAccountValue(prevAcctMeta map[string]*commonpb.MetadataSet, account, metadataKey string) []byte {
	if prevAcctMeta == nil {
		return nil
	}

	prevSet, ok := prevAcctMeta[account]
	if !ok || prevSet == nil {
		return nil
	}

	for _, md := range prevSet.GetMetadata() {
		if md.GetKey() == metadataKey {
			return readstore.EncodeMetadataValue(nil, md.GetValue())
		}
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
