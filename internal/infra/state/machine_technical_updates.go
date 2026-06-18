package state

import (
	"bytes"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// saveLedgerWithCache updates a LedgerInfo in the in-memory cache, the 0xF1
// attribute zone, the 0xFF cache zone, and the ZoneGlobal durable store — all
// in the same Pebble batch. Uses CacheAwareEntry.PutWithCache for the first
// three writes, then SaveLedger for the Ledger-specific ZoneGlobal store.
func (fsm *Machine) saveLedgerWithCache(batch *dal.WriteSession, ledgerKey domain.LedgerKey, info *commonpb.LedgerInfo) error {
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
func (fsm *Machine) applyTechnicalUpdates(batch *dal.WriteSession, raftIndex uint64, proposal *raftcmdpb.Proposal) error {
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
		if err := fsm.applyIndexReady(batch, ready, proposal.GetDate()); err != nil {
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
func (fsm *Machine) applyClusterConfig(batch *dal.WriteSession, raftIndex uint64, cfg *commonpb.ClusterConfig) error {
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
	if fsm.BloomFilters != nil && !bloom.BloomConfigEqual(cfg, fsm.State.LastClusterConfig) {
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

		// Signal the bloom-rebuild dispatcher (owned by Recovery, which holds
		// the Pebble reader) to launch async repopulation from an attribute
		// scan. We do not call StartAsyncBloomPopulate directly because the
		// hot-path Machine does not hold a reader.
		select {
		case fsm.bloomRebuildCh <- "bloom config changed via cluster config":
		default:
			// A rebuild is already pending — the latest reason wins via the
			// next signal opportunity; nothing to do here.
		}
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

	fsm.State.UpdateClusterConfig(cfg)

	return nil
}

// applyEventsSinkUpdate applies a per-sink cursor and status update. No log entry is produced.
func (fsm *Machine) applyEventsSinkUpdate(batch *dal.WriteSession, update *raftcmdpb.EventsSinkUpdate) error {
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
func (fsm *Machine) applyMirrorSyncUpdate(batch *dal.WriteSession, update *raftcmdpb.MirrorSyncUpdate) error {
	ledgerInfo, _, err := fsm.Registry.Ledgers.Get(domain.LedgerKey{Name: update.GetLedgerName()}.Bytes())
	if err != nil || ledgerInfo == nil {
		return nil // ledger not found (may have been deleted)
	}

	ledgerName := ledgerInfo.GetName()

	if update.GetCursor() > 0 {
		if err := SetMirrorCursor(batch, ledgerName, update.GetCursor()); err != nil {
			return fmt.Errorf("setting mirror cursor: %w", err)
		}
	}

	if update.GetSourceLogCount() > 0 {
		if err := SetMirrorSourceHead(batch, ledgerName, update.GetSourceLogCount()); err != nil {
			return fmt.Errorf("setting mirror source head: %w", err)
		}
	}

	if update.GetClearError() {
		if err := clearMirrorStatus(batch, ledgerName); err != nil {
			return fmt.Errorf("clearing mirror status: %w", err)
		}
	} else if update.GetError() != nil {
		if err := SetMirrorStatus(batch, ledgerName, update.GetError()); err != nil {
			return fmt.Errorf("setting mirror status: %w", err)
		}
	}

	return nil
}

// applyIdempotencyEviction evicts expired idempotency keys. No log entry is produced.
// The key hashes were pre-scanned by the leader and included in the proposal,
// so this method is write-only — no Pebble reads occur.
func (fsm *Machine) applyIdempotencyEviction(batch *dal.WriteSession, eviction *raftcmdpb.IdempotencyEviction) error {
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
// No log entry is produced. This path NEVER writes the LedgerInfo back: the
// previous design persisted progress counters (TotalKeys/ConvertedKeys) on
// the schema field, but those are gone and the batch apply now only mutates
// the metadata values via the per-entry compare-and-set. Skipping the
// LedgerInfo save also closes the "stale schema preload overwrites a newer
// LedgerInfo" race surfaced in PR #359 review: a preloaded LedgerInfo can
// be older than the in-cache copy when an earlier schema change rotated the
// new version out of the cache before this batch applies; saving the stale
// clone would silently roll back the newer schema. With no save, the batch
// is read-only on LedgerInfo and the race is structurally impossible.
func (fsm *Machine) applyMetadataConversionBatch(batch *dal.WriteSession, b *raftcmdpb.MetadataConversionBatch) error {
	info, _, err := fsm.Registry.Ledgers.Get(domain.LedgerKey{Name: b.GetLedger()}.Bytes())
	if err != nil {
		return nil // ledger not in cache — stale conversion batch, skip
	}

	if info == nil {
		// Get succeeded → the cache returned an entry → info MUST be
		// non-nil. A nil here means CacheAwareEntry.Get's contract was
		// broken (e.g. a Put with a nil value slipped through). Fail
		// loudly so this surfaces in CI/antithesis instead of silently
		// dropping the batch.
		return fmt.Errorf("invariant: Registry.Ledgers.Get returned nil info for %q without error", b.GetLedger())
	}

	if info.GetDeletedAt() != nil {
		return nil // ledger was deleted — stale conversion, skip
	}

	// Staleness check: the field must still be CONVERTING with the expected
	// type. Read-only on the cached LedgerInfo — no clone, no save.
	_, fieldSchema := processing.SchemaFieldForTarget(info.GetMetadataSchema(), b.GetTargetType(), b.GetKey())
	if fieldSchema == nil ||
		fieldSchema.GetStatus() != commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING ||
		fieldSchema.GetType() != b.GetExpectedType() {
		return nil // stale batch
	}

	// Each entry carries the value the converter scanned (ExpectedValue,
	// raw VT bytes). The proposal's Preload guarantees every canonical
	// key is in the cache before this loop runs (#359). The FSM then
	// compares against the cache state so we neither resurrect a deleted
	// key nor clobber a fresh write that landed in Raft order between
	// scan and apply (#313):
	//
	//   * Cache tombstone (entry.Deleted): a user delete passed through
	//     the FSM after the scan. Skip — preserve the deletion.
	//   * Cache hit, current value bytes == ExpectedValue: same value the
	//     converter saw. Safe to write converted_value.
	//   * Cache hit, current value bytes != ExpectedValue: a user mutation
	//     replaced the scanned value. Skip — preserve the new write; the
	//     next scan pass will skip this key (TypeMatches).
	//   * Cache miss (no entry at all): a scan-vs-apply race — the
	//     converter scanned the key from a Pebble snapshot but a user
	//     delete plus enough rotations evicted both the value and the
	//     tombstone before this apply, so the proposer's preload had
	//     nothing to ship. Skip — same outcome as the tombstone branch.
	//
	// Convergence is the converter's responsibility: it keeps scanning
	// until a full pass turns up zero entries needing conversion, then
	// proposes MetadataConversionCompletion. The FSM never auto-COMPLETES
	// here — counting writes across passes would race with concurrent
	// user mutations and could flip Status while keys still mismatched.
	for _, entry := range b.GetEntries() {
		if err := fsm.applyConvertEntry(batch, b.GetTargetType(), entry); err != nil {
			return err
		}
	}

	return nil
}

// applyMetadataConversionCompletion applies a metadata conversion completion.
// No log entry is produced. Reads `LedgerInfo` from the cache, which the
// preload path (see `metadataBatchProposer.Propose` adding the ledger key
// to `needs.Ledgers`) guarantees is populated with the fresh value at
// propose time.
func (fsm *Machine) applyMetadataConversionCompletion(batch *dal.WriteSession, complete *raftcmdpb.MetadataConversionCompletion) error {
	ledgerKey := domain.LedgerKey{Name: complete.GetLedger()}

	info, _, err := fsm.Registry.Ledgers.Get(ledgerKey.Bytes())
	if err != nil {
		return nil // ledger not in cache — stale conversion completion, skip
	}

	if info == nil {
		// See applyMetadataConversionBatch above: nil info on a successful
		// Get violates the CacheAwareEntry contract — fail loudly.
		return fmt.Errorf("invariant: Registry.Ledgers.Get returned nil info for %q without error", complete.GetLedger())
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

	return fsm.saveLedgerWithCache(batch, ledgerKey, info)
}

// applyIndexReady applies an index-ready notification. No log entry is produced.
// The index builder detects the status change by reading LedgerInfo on its next tick.
func (fsm *Machine) applyIndexReady(batch *dal.WriteSession, ready *raftcmdpb.IndexReadyUpdate, proposalDate *commonpb.Timestamp) error {
	ledgerKey := domain.LedgerKey{Name: ready.GetLedger()}

	info, _, err := fsm.Registry.Ledgers.Get(ledgerKey.Bytes())
	if err != nil {
		return nil // ledger not in cache — stale index ready, skip
	}

	if info == nil {
		// See applyMetadataConversionBatch above: nil info on a successful
		// Get violates the CacheAwareEntry contract — fail loudly.
		return fmt.Errorf("invariant: Registry.Ledgers.Get returned nil info for %q without error", ready.GetLedger())
	}

	if info.GetDeletedAt() != nil {
		return nil // ledger was deleted — stale index ready, skip
	}

	idx := indexes.Find(info, ready.GetId())
	if idx == nil {
		return nil // index entry has been dropped between scheduling and apply
	}

	info = info.CloneVT()
	idx = indexes.Find(info, ready.GetId())
	idx.BuildStatus = commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	idx.LastBuiltAt = proposalDate
	idx.LastError = ""

	return fsm.saveLedgerWithCache(batch, ledgerKey, info)
}

// applyConvertEntry decides whether to write the converted_value for a
// single entry. See applyMetadataConversionBatch for the freshness
// invariants. Returns nil for both write and silent skip cases — the
// caller no longer cares which since convergence is driven by the
// converter, not by counting writes here.
func (fsm *Machine) applyConvertEntry(batch *dal.WriteSession, targetType commonpb.TargetType, entry *raftcmdpb.ConvertMetadataEntry) error {
	cacheEntry, present, err := fsm.getMetadataCacheEntry(targetType, entry.GetCanonicalKey())
	if err != nil {
		return err
	}

	if !present {
		// Genuine "key absent everywhere" path — NOT an invariant
		// violation. The converter scans a Pebble snapshot and enqueues
		// the canonical key; between scan and apply a user delete can
		// commit (Pebble delete + cache tombstone). After enough
		// intervening proposals the cache tombstone rotates out, and
		// the proposer's preload finds nothing in cache OR Pebble for
		// this key, so it emits no preload payload. The apply then sees
		// `present=false` here and the safe outcome is the same as the
		// `cacheEntry.Deleted` branch below: skip, the conversion is
		// stale and the next scan pass will not re-enqueue this key
		// (it's gone from Pebble too). Returning an error here would
		// turn a normal scan-vs-apply race into an apply failure
		// (flemzord review on #359).
		//
		// Limitation: at this layer we cannot distinguish (a) the race
		// above — proposer declared the key in needs.Metadata, preload
		// saw cache+Pebble miss, shipped nothing — from (b) a future
		// regression where the proposer forgets to declare the key
		// (invariant #6 violation, silently masked as soft skip).
		// Follow-up #451 (preload.View capability) makes the
		// distinction observable: ErrNotPreloaded for (b), ErrNotFound
		// for (a). Until then, this soft skip is the safe choice and
		// metadata_batch_proposer is the single declaration site to
		// audit.
		return nil
	}

	if cacheEntry.Deleted {
		// Deletion landed after the scan.
		return nil
	}

	currentBytes, err := cacheEntry.Data.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling current metadata value: %w", err)
	}

	if !bytes.Equal(currentBytes, entry.GetExpectedValue()) {
		// Mutation landed after the scan; preserve the newer value.
		return nil
	}

	return fsm.putConvertBatchValue(batch, targetType, entry.GetCanonicalKey(), entry.GetConvertedValue())
}

// getMetadataCacheEntry returns the underlying cache entry (with its
// Deleted tombstone flag) for a metadata canonical key. present=false
// means the key has no map entry at all (never cached, or rotated out).
func (fsm *Machine) getMetadataCacheEntry(targetType commonpb.TargetType, canonicalKey []byte) (attributes.Entry[*commonpb.MetadataValue], bool, error) {
	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		e, ok := fsm.Registry.AccountMetadata.KeyStore().GetEntry(canonicalKey)

		return e, ok, nil
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		e, ok := fsm.Registry.LedgerMetadata.KeyStore().GetEntry(canonicalKey)

		return e, ok, nil
	default:
		return attributes.Entry[*commonpb.MetadataValue]{}, false, fmt.Errorf("unsupported target type for conversion: %v", targetType)
	}
}

// putConvertBatchValue stores a converted metadata value via PutWithCache,
// which atomically writes to the in-memory KeyStore, 0xF1, and 0xFF cache zone.
func (fsm *Machine) putConvertBatchValue(batch *dal.WriteSession, targetType commonpb.TargetType, canonicalKey []byte, value *commonpb.MetadataValue) error {
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
