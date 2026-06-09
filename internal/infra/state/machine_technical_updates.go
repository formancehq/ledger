package state

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// saveLedgerWithCache updates a LedgerInfo in the in-memory cache, the 0xF1
// attribute zone, the 0xFF cache zone, and the ZoneGlobal durable store — all
// in the same Pebble batch. Uses CacheAwareEntry.PutWithCache for the first
// three writes, then SaveLedger for the Ledger-specific ZoneGlobal store.
func (fsm *Machine) saveLedgerWithCache(batch *dal.Batch, ledgerKey domain.LedgerKey, info *commonpb.LedgerInfo) error {
	genByte := byte(fsm.Registry.Cache.CurrentGeneration() % 2)

	if _, _, err := fsm.Registry.Ledgers.PutWithCache(batch, genByte, ledgerKey.Bytes(), info); err != nil {
		return fmt.Errorf("updating ledger with cache: %w", err)
	}

	if err := SaveLedger(batch, info); err != nil {
		return fmt.Errorf("persisting ledger info: %w", err)
	}

	return nil
}

// applyTechnicalUpdates applies Proposal-level technical updates that bypass
// the Order/Log system. These are applied directly to Pebble and run on every
// proposal, regardless of whether it also carries orders.
func (fsm *Machine) applyTechnicalUpdates(batch *dal.Batch, raftIndex uint64, proposal *raftcmdpb.Proposal) error {
	if cfg := proposal.GetClusterConfig(); cfg != nil {
		if err := fsm.applyClusterConfig(batch, raftIndex, cfg); err != nil {
			return fmt.Errorf("applying cluster config: %w", err)
		}
	}

	for _, update := range proposal.GetEventsSinkUpdates() {
		if err := fsm.applyEventsSinkUpdate(batch, update); err != nil {
			return fmt.Errorf("applying events sink update: %w", err)
		}
	}

	for _, update := range proposal.GetMirrorSyncUpdates() {
		if err := fsm.applyMirrorSyncUpdate(batch, update); err != nil {
			return fmt.Errorf("applying mirror sync update: %w", err)
		}
	}

	if eviction := proposal.GetIdempotencyEviction(); eviction != nil {
		if err := fsm.applyIdempotencyEviction(batch, eviction); err != nil {
			return fmt.Errorf("applying idempotency eviction: %w", err)
		}
	}

	for _, convBatch := range proposal.GetMetadataConversionBatches() {
		if err := fsm.applyMetadataConversionBatch(batch, convBatch); err != nil {
			return fmt.Errorf("applying metadata conversion batch: %w", err)
		}
	}

	for _, complete := range proposal.GetMetadataConversionsComplete() {
		if err := fsm.applyMetadataConversionCompletion(batch, complete); err != nil {
			return fmt.Errorf("applying metadata conversion completion: %w", err)
		}
	}

	for _, ready := range proposal.GetIndexReadyUpdates() {
		if err := fsm.applyIndexReady(batch, ready); err != nil {
			return fmt.Errorf("applying index ready: %w", err)
		}
	}

	return nil
}

// applyClusterConfig handles cluster config updates (Raft-replicated).
// When the rotation threshold changes, the generation boundaries shift and the
// alternating-byte persistence scheme in 0xFF can lose data on even-generation
// skips. Reset the cache and purge 0xFF entirely — the preloader falls back to
// Pebble reads (0xF1) and the cache rebuilds naturally.
func (fsm *Machine) applyClusterConfig(batch *dal.Batch, raftIndex uint64, cfg *commonpb.ClusterConfig) error {
	oldThreshold := fsm.Registry.Cache.GenerationThreshold()
	newThreshold := cfg.GetRotationThreshold()

	if newThreshold != oldThreshold {
		fsm.logger.WithFields(map[string]any{
			"oldThreshold": oldThreshold,
			"newThreshold": newThreshold,
			"raftIndex":    raftIndex,
		}).Infof("Applying cluster config change: resetting cache and purging 0xFF")

		fsm.Registry.Cache.ResetWithThreshold(newThreshold)

		// Purge both generation byte positions (0 and 1) in the 0xFF cache zone.
		// We can't use a single DeleteRange from [0xFF] to [0xFF+1] because
		// 0xFF+1 overflows to 0x00 as a byte. Instead, purge each gen byte
		// separately using the same pattern as writeCacheRotation.
		for _, genByte := range []byte{0, 1} {
			if err := batch.DeleteRangeNoSync(
				[]byte{dal.ZoneCache, genByte},
				[]byte{dal.ZoneCache, genByte + 1},
			); err != nil {
				return fmt.Errorf("purging cache gen %d: %w", genByte, err)
			}
		}

		// Reset the cache metadata sentinel to currentGeneration=0 (post-reset state).
		// We must NOT delete it — RestoreFromStore tolerates a missing sentinel
		// but other code paths may depend on it being present.
		if err := batch.SetProto(
			[]byte{dal.ZoneCache, dal.SubCacheMeta},
			&raftcmdpb.CacheSnapshotMeta{CurrentGeneration: 0},
		); err != nil {
			return fmt.Errorf("resetting cache snapshot meta: %w", err)
		}
	}

	// Check if bloom filter config changed. If so, purge persisted blocks
	// and rebuild filters with new dimensions. The preloader falls back to
	// Pebble Gets while IsReady() returns false.
	if fsm.BloomFilters != nil && !bloom.BloomConfigEqual(cfg, fsm.lastClusterConfig) {
		fsm.logger.WithFields(map[string]any{
			"raftIndex": raftIndex,
		}).Infof("Bloom filter config changed: purging blocks and rebuilding")

		// Purge all persisted bloom blocks.
		if err := batch.DeleteRangeNoSync(
			[]byte{dal.ZoneGlobal, dal.SubGlobBloom},
			[]byte{dal.ZoneGlobal, dal.SubGlobBloom + 1},
		); err != nil {
			return fmt.Errorf("purging bloom blocks: %w", err)
		}

		// Rebuild filters with new dimensions (sets IsReady=false).
		fsm.BloomFilters.Rebuild(cfg)

		// Launch async repopulation from attribute scan.
		fsm.cacheSnapshotter.StartAsyncBloomPopulate("bloom config changed via cluster config")
	}

	// Persist the cluster state with the current cache epoch.
	// The epoch is deterministic (incremented only by ResetWithThreshold
	// in the FSM apply path) and must be persisted so that nodes
	// restoring from a checkpoint have the correct epoch.
	if err := saveClusterState(batch, &commonpb.PersistedClusterState{
		Config:     cfg,
		CacheEpoch: fsm.Registry.Cache.Epoch(),
	}); err != nil {
		return fmt.Errorf("saving cluster state: %w", err)
	}

	fsm.hashAlgorithm = cfg.GetHashAlgorithm()
	fsm.lastClusterConfig = cfg

	return nil
}

// applyEventsSinkUpdate applies a per-sink cursor and status update. No log entry is produced.
func (fsm *Machine) applyEventsSinkUpdate(batch *dal.Batch, update *raftcmdpb.EventsSinkUpdate) error {
	if update.GetCursor() > 0 {
		if err := SetSinkCursor(batch, update.GetSinkName(), update.GetCursor()); err != nil {
			return fmt.Errorf("setting sink cursor: %w", err)
		}
	}

	if update.GetClearError() {
		if err := ClearSinkStatus(batch, update.GetSinkName()); err != nil {
			return fmt.Errorf("clearing sink status: %w", err)
		}
	} else if update.GetError() != nil {
		if err := SetSinkStatus(batch, &commonpb.SinkStatus{
			SinkName: update.GetSinkName(),
			Cursor:   update.GetCursor(),
			Error:    update.GetError(),
		}); err != nil {
			return fmt.Errorf("setting sink status: %w", err)
		}
	}

	return nil
}

// applyMirrorSyncUpdate applies a per-ledger mirror cursor and status update. No log entry is produced.
func (fsm *Machine) applyMirrorSyncUpdate(batch *dal.Batch, update *raftcmdpb.MirrorSyncUpdate) error {
	ledgerInfo, _, err := fsm.Registry.Ledgers.Get(domain.LedgerKey{Name: update.GetLedgerName()}.Bytes())
	if err != nil || ledgerInfo == nil {
		return nil // ledger not found (may have been deleted)
	}

	ledgerID := ledgerInfo.GetId()

	if update.GetCursor() > 0 {
		if err := SetMirrorCursor(batch, ledgerID, update.GetCursor()); err != nil {
			return fmt.Errorf("setting mirror cursor: %w", err)
		}
	}

	if update.GetSourceLogCount() > 0 {
		if err := SetMirrorSourceHead(batch, ledgerID, update.GetSourceLogCount()); err != nil {
			return fmt.Errorf("setting mirror source head: %w", err)
		}
	}

	if update.GetClearError() {
		if err := clearMirrorStatus(batch, ledgerID); err != nil {
			return fmt.Errorf("clearing mirror status: %w", err)
		}
	} else if update.GetError() != nil {
		if err := SetMirrorStatus(batch, ledgerID, update.GetError()); err != nil {
			return fmt.Errorf("setting mirror status: %w", err)
		}
	}

	return nil
}

// applyIdempotencyEviction evicts expired idempotency keys. No log entry is produced.
// The key hashes were pre-scanned by the leader and included in the proposal,
// so this method is write-only — no Pebble reads occur.
func (fsm *Machine) applyIdempotencyEviction(batch *dal.Batch, eviction *raftcmdpb.IdempotencyEviction) error {
	evicted, err := fsm.Registry.Idempotency.Evict(batch, eviction.GetCutoffMicros(), eviction.GetLastScannedTimeIndexKey(), eviction.GetPebbleKeyHashes())
	if err != nil {
		return fmt.Errorf("evicting idempotency keys: %w", err)
	}

	if evicted > 0 {
		fsm.logger.Infof("Evicted %d expired idempotency keys (cutoff=%d)", evicted, eviction.GetCutoffMicros())
	}

	return nil
}

// applyMetadataConversionBatch applies a background metadata conversion batch.
// No log entry is produced.
func (fsm *Machine) applyMetadataConversionBatch(batch *dal.Batch, b *raftcmdpb.MetadataConversionBatch) error {
	ledgerKey := domain.LedgerKey{Name: b.GetLedger()}

	info, _, err := fsm.Registry.Ledgers.Get(ledgerKey.Bytes())
	if err != nil {
		return nil // ledger not in cache — stale conversion batch, skip
	}

	if info == nil {
		return nil // ledger entry is nil — should not happen, skip silently
	}

	if info.GetDeletedAt() != nil {
		return nil // ledger was deleted — stale conversion, skip
	}

	info = info.CloneVT()

	// Staleness check: the field must still be CONVERTING with the expected type.
	_, fieldSchema := processing.SchemaFieldForTarget(info.GetMetadataSchema(), b.GetTargetType(), b.GetKey())
	if fieldSchema == nil ||
		fieldSchema.GetStatus() != commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING ||
		fieldSchema.GetType() != b.GetExpectedType() {
		return nil // stale batch
	}

	for _, entry := range b.GetEntries() {
		value, err := fsm.getConvertBatchValue(b.GetTargetType(), entry.GetCanonicalKey())
		if err != nil {
			return err
		}

		if value == nil {
			continue // key deleted since scan
		}

		if !commonpb.TypeMatches(value, b.GetExpectedType()) {
			if err := fsm.putConvertBatchValue(batch, b.GetTargetType(), entry.GetCanonicalKey(), entry.GetConvertedValue()); err != nil {
				return err
			}
		}
	}

	// Persist conversion progress in the schema.
	fieldSchema.TotalKeys = b.GetTotalKeys()
	fieldSchema.ConvertedKeys = b.GetConvertedKeysSoFar()

	return fsm.saveLedgerWithCache(batch, ledgerKey, info)
}

// applyMetadataConversionCompletion applies a metadata conversion completion.
// No log entry is produced.
func (fsm *Machine) applyMetadataConversionCompletion(batch *dal.Batch, complete *raftcmdpb.MetadataConversionCompletion) error {
	ledgerKey := domain.LedgerKey{Name: complete.GetLedger()}

	info, _, err := fsm.Registry.Ledgers.Get(ledgerKey.Bytes())
	if err != nil {
		return nil // ledger not in cache — stale conversion completion, skip
	}

	if info == nil {
		return nil // ledger entry is nil — should not happen, skip silently
	}

	if info.GetDeletedAt() != nil {
		return nil // ledger was deleted — stale conversion, skip
	}

	info = info.CloneVT()

	_, fieldSchema := processing.SchemaFieldForTarget(info.GetMetadataSchema(), complete.GetTargetType(), complete.GetKey())
	if fieldSchema == nil ||
		fieldSchema.GetStatus() != commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING ||
		fieldSchema.GetType() != complete.GetExpectedType() {
		return nil // stale
	}

	fieldSchema.Status = commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE
	fieldSchema.ConvertedKeys = fieldSchema.GetTotalKeys()

	return fsm.saveLedgerWithCache(batch, ledgerKey, info)
}

// applyIndexReady applies an index-ready notification. No log entry is produced.
// The index builder detects the status change by reading LedgerInfo on its next tick.
func (fsm *Machine) applyIndexReady(batch *dal.Batch, ready *raftcmdpb.IndexReadyUpdate) error {
	ledgerKey := domain.LedgerKey{Name: ready.GetLedger()}

	info, _, err := fsm.Registry.Ledgers.Get(ledgerKey.Bytes())
	if err != nil {
		return nil // ledger not in cache — stale index ready, skip
	}

	if info == nil {
		return nil // ledger entry is nil — should not happen, skip silently
	}

	if info.GetDeletedAt() != nil {
		return nil // ledger was deleted — stale index ready, skip
	}

	info = info.CloneVT()

	switch idx := ready.GetIndex().(type) {
	case *raftcmdpb.IndexReadyUpdate_Transaction:
		switch kind := idx.Transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			if info.GetBuiltinIndexes() != nil {
				processing.SetBuiltinStatus(info.GetBuiltinIndexes(), kind.Builtin, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY)
			}
		case *commonpb.TransactionIndex_MetadataKey:
			processing.ProcessIndexReadyMetadata(info, commonpb.TargetType_TARGET_TYPE_TRANSACTION, kind.MetadataKey)
		}
	case *raftcmdpb.IndexReadyUpdate_Account:
		switch kind := idx.Account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			_ = kind // No account builtins yet
		case *commonpb.AccountIndex_MetadataKey:
			processing.ProcessIndexReadyMetadata(info, commonpb.TargetType_TARGET_TYPE_ACCOUNT, kind.MetadataKey)
		}
	case *raftcmdpb.IndexReadyUpdate_LogBuiltin:
		if info.GetLogBuiltinIndexes() != nil {
			processing.SetLogBuiltinStatus(info.GetLogBuiltinIndexes(), idx.LogBuiltin, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY)
		}
	}

	return fsm.saveLedgerWithCache(batch, ledgerKey, info)
}

// getConvertBatchValue retrieves the current metadata value for a canonical key,
// dispatching to the correct Registry store based on target type.
func (fsm *Machine) getConvertBatchValue(targetType commonpb.TargetType, canonicalKey []byte) (*commonpb.MetadataValue, error) {
	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		v, _, err := fsm.Registry.AccountMetadata.Get(canonicalKey)
		if err != nil {
			return nil, nil //nolint:nilerr // key not found = deleted since scan
		}

		return v, nil
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		v, _, err := fsm.Registry.LedgerMetadata.Get(canonicalKey)
		if err != nil {
			return nil, nil //nolint:nilerr // key not found = deleted since scan
		}

		return v, nil
	default:
		return nil, nil
	}
}

// putConvertBatchValue stores a converted metadata value via PutWithCache,
// which atomically writes to the in-memory KeyStore, 0xF1, and 0xFF cache zone.
func (fsm *Machine) putConvertBatchValue(batch *dal.Batch, targetType commonpb.TargetType, canonicalKey []byte, value *commonpb.MetadataValue) error {
	genByte := byte(fsm.Registry.Cache.CurrentGeneration() % 2)

	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		if _, _, err := fsm.Registry.AccountMetadata.PutWithCache(batch, genByte, canonicalKey, value); err != nil {
			return fmt.Errorf("setting account metadata with cache: %w", err)
		}
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		if _, _, err := fsm.Registry.LedgerMetadata.PutWithCache(batch, genByte, canonicalKey, value); err != nil {
			return fmt.Errorf("setting ledger metadata with cache: %w", err)
		}
	}

	return nil
}
