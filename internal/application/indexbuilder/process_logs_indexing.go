package indexbuilder

import (
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// indexPayload dispatches a ledger log payload to the appropriate index handler.
func (b *Builder) indexPayload(
	kb *dal.KeyBuilder,
	cfg *ledgerIndexConfig,
	ledgerName string,
	payload any,
	excludedVolumes *postingExclusions,
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
		return b.handleCreatedIndexLog(ledgerName, p.CreateIndex)
	case *commonpb.LedgerLogPayload_DropIndex:
		if err := b.handleDroppedIndexLog(kb, ledgerName, p.DropIndex); err != nil {
			return err
		}
	}

	return nil
}

// backfillLogDateRow indexes one replayed log's date when cfg builds the log
// date index. The date index covers every log of the ledger — a date filter
// reads the same universe ListLogs scans — so this runs for a config-mutation
// log too, while the entity projections in indexLogEntry stay specific to a
// data log's payload. A no-op for any other backfill kind. The caller has
// already established that log belongs to the ledger being backfilled.
func (b *Builder) backfillLogDateRow(cfg *ledgerIndexConfig, log *commonpb.Log) error {
	if !cfg.isLogBuiltinIndexed(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE) {
		return nil
	}

	applyLog, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
	if !ok {
		return nil
	}

	ledgerLog := applyLog.Apply.GetLog()
	if ledgerLog == nil || ledgerLog.GetData() == nil {
		return nil
	}

	b.wb.SetEventSequence(log.GetSequence())

	return b.wb.WriteLedgerLogDateIndex(b.kb, applyLog.Apply.GetLedgerName(), ledgerLog.GetDate().GetData(), ledgerLog.GetId())
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
		if dl.DeleteLedger == nil {
			return errors.New("invariant: nil DeletedLedger payload")
		}
		if b.wb.Batch() == nil {
			return errors.New("invariant: DeletedLedger encountered without an active readstore batch")
		}

		name := dl.DeleteLedger.GetName()
		if err := readstore.DeleteLedgerIndexes(b.wb.Batch(), name); err != nil {
			return err
		}

		b.markLedgerDeletedInBatch(name)

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

	b.wb.SetEventSequence(log.GetSequence())

	// Per-ledger log index is always maintained — see the live-path comment.
	if err := b.wb.WriteLedgerLogIndex(b.kb, ledgerName, ledgerLog.GetId(), log.GetSequence()); err != nil {
		return err
	}

	// Schema logs are deliberately absent from this switch. The only caller,
	// processBackfill, gates on the generated HISTORY category, which admits exactly
	// CreatedTransaction / RevertedTransaction / SavedMetadata / DeletedMetadata
	// / OrderSkipped and switches on the same discriminator used here — so a
	// SetMetadataFieldType or RemovedMetadataFieldType log can never arrive,
	// and neither can the CreateIndex / DropIndex cases below, which the live
	// path reaches through indexPayload.
	// Schema handling belongs to the live path (indexPayload), where
	// SetMetadataFieldType defers to addSchemaRewriteTask / processSchemaRewrite
	// and RemovedMetadataFieldType to handleRemovedMetadataFieldType. A retype
	// that lands mid-backfill is handled by addSchemaRewriteTask resetting the
	// in-flight cursor, not by replaying the schema log here. Do NOT add a case
	// for CONTROL payloads without also changing their protobuf category
	// annotation: an unreachable second implementation of the schema rewrite is
	// what this replaced.
	switch p := ledgerLog.GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction:
		return b.indexCreatedTransaction(b.kb, cfg, ledgerName, p.CreatedTransaction, excludedVolumes)
	case *commonpb.LedgerLogPayload_RevertedTransaction:
		return b.indexRevertedTransaction(b.kb, cfg, ledgerName, p.RevertedTransaction, excludedVolumes)
	case *commonpb.LedgerLogPayload_SavedMetadata:
		return b.indexSavedMetadata(b.kb, cfg, ledgerName, p.SavedMetadata)
	case *commonpb.LedgerLogPayload_DeletedMetadata:
		return b.indexDeletedMetadata(b.kb, cfg, ledgerName, p.DeletedMetadata)
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
	excludedVolumes *postingExclusions,
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

	// Account metadata from account_metadata map. Accounts may already exist,
	// so resolve the index's old encoding through the reverse map (overlay +
	// committed state). The overlay also makes repeated account updates in the
	// same batch replace one another correctly during a schema rewrite.
	for account, metadataMap := range ct.GetAccountMetadata() {
		if metadataMap != nil {
			for key, value := range metadataMap.GetValues() {
				if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key) {
					continue
				}

				if err := b.dualWriteMetadataIndex(
					kb,
					ledger, readstore.NamespaceAccount, key,
					commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					value, []byte(account),
					func(version uint32) []byte {
						return readstore.AccountReverseMapKeyV(kb, ledger, account, key, version)
					},
				); err != nil {
					return err
				}
			}
		}
	}

	// Transaction metadata is the first index write for this transaction ID.
	// Insert directly instead of reading the reverse map: live folding sees a
	// freshly-minted ID, and a creation backfill always targets a single-use
	// pending version. A retype-driven reset bumps that pending version before
	// replay, so the restarted fold also writes a fresh keyspace.
	if len(txn.GetMetadata()) > 0 {
		txIDBytes := make([]byte, 0, 8)

		txIDBytes = readstore.EncodeTxID(txIDBytes, txn.GetId())
		txID := txn.GetId()
		for key, value := range txn.GetMetadata() {
			if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key) {
				continue
			}

			if err := b.dualInsertKnownAbsentMetadataIndex(
				kb,
				ledger, readstore.NamespaceTransaction, key,
				commonpb.TargetType_TARGET_TYPE_TRANSACTION,
				value, txIDBytes,
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
	excludedVolumes *postingExclusions,
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

	// Transaction metadata for the freshly-minted revert transaction follows
	// the same insert-known-absent contract as CreatedTransaction. The normal
	// fold writes a new ID, while creation-backfill replay writes a fresh,
	// single-use pending version (including after a retype reset).
	if len(revertTxn.GetMetadata()) > 0 {
		txIDBytes := make([]byte, 0, 8)

		txIDBytes = readstore.EncodeTxID(txIDBytes, revertTxn.GetId())
		txID := revertTxn.GetId()
		for key, value := range revertTxn.GetMetadata() {
			if !cfg.isMetadataIndexed(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key) {
				continue
			}

			if err := b.dualInsertKnownAbsentMetadataIndex(
				kb,
				ledger, readstore.NamespaceTransaction, key,
				commonpb.TargetType_TARGET_TYPE_TRANSACTION,
				value, txIDBytes,
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
	excludedVolumes *postingExclusions,
) error {
	wb := b.wb
	var current, historical map[domain.AccountAssetKey]struct{}
	if excludedVolumes != nil {
		current, historical = excludedVolumes.current, excludedVolumes.historical
	}
	sourceExcluded := isExcluded(current, source, asset, color)
	destinationExcluded := isExcluded(current, destination, asset, color)

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

	sourceExcluded = isExcluded(historical, source, asset, color)
	destinationExcluded = isExcluded(historical, destination, asset, color)

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

	// Historical role mappings skip only TRANSIENT cells. Purging an
	// EPHEMERAL cell must not erase its source or destination relationship.
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
// Returns nil when the entity has no current entry. The reverse map is the
// authority for the index's actual encoding, including while a schema rewrite
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

			if err := b.dualWriteMetadataIndex(
				kb,
				ledger, readstore.NamespaceAccount, key,
				commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				value, []byte(account),
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

			if err := b.dualWriteMetadataIndex(
				kb,
				ledger, readstore.NamespaceTransaction, key,
				commonpb.TargetType_TARGET_TYPE_TRANSACTION,
				value, txIDBytes,
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

// cloneBytes returns a copy of the given byte slice.
func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)

	return c
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
