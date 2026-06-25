package indexbuilder

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// ledgerIndexConfig caches which indexes are enabled and ready for a ledger.
// Keyed by IndexID canonical form (indexes.Canonical) for O(1) lookup; the
// stored value is the Index entry itself (status + audit metadata).
type ledgerIndexConfig struct {
	byCanonical map[string]*commonpb.Index
}

// newLedgerIndexConfig creates a new ledgerIndexConfig with the map initialized.
func newLedgerIndexConfig() *ledgerIndexConfig {
	return &ledgerIndexConfig{
		byCanonical: make(map[string]*commonpb.Index),
	}
}

// initIndexConfig populates the in-memory index config cache from the
// bucket-scoped Index registry. Three steps:
//
//  1. ReadAllIndexVersionStates seeds the per-replica version cache so
//     loadIndexRegistry's BUILDING-scheduling decision can consult
//     CurrentVersion for metadata indexes (a non-zero CurrentVersion
//     means the local replica already built v_current; the BUILDING
//     flag reflects a retype handled by scheduleResumedRewrites
//     instead of a from-scratch backfill).
//  2. ReadLedgers seeds an empty ledgerIndexConfig per active ledger so
//     handle{Created,Dropped}IndexLog can target the right cache
//     without racing the registry scan.
//  3. A streaming scan of SubAttrIndex enumerates Index entries; each
//     is routed by Index.Ledger into the matching ledgerIndexConfig.
//     BUILDING entries spawn backfill tasks unless step 1 indicated
//     the local replica already finished a prior build (rewrite path).
//
// Bucket-scoped entries (Index.Ledger == "") land in b.bucketIndexConfig
// and are reserved for audit-style indexes (see #436); they aren't tied
// to any ledger and don't trigger per-ledger backfill paths.
func (b *Builder) initIndexConfig(ctx context.Context) {
	handle, err := b.pebbleStore.NewDirectReadHandle()
	if err != nil {
		b.logger.Errorf("Failed to create read handle for index config: %v", err)

		return
	}

	defer func() { _ = handle.Close() }()

	// Restore the per-replica forward-encoding versions from the read
	// store. Done before the ledger scan so loadIndexRegistry can
	// consult the cache when deciding whether to schedule a backfill.
	versionEntries, err := b.readStore.ReadAllIndexVersionStates()
	if err != nil {
		b.logger.Errorf("Failed to read index version state: %v", err)

		return
	}

	for _, e := range versionEntries {
		b.putVersionState(e.LedgerName, e.CanonicalID, e.State)
	}

	if err := b.seedLedgerIndexConfig(ctx, handle); err != nil {
		b.logger.Errorf("Failed to seed ledger index config: %v", err)

		return
	}

	if err := b.loadIndexRegistry(handle); err != nil {
		b.logger.Errorf("Failed to load index registry: %v", err)

		return
	}

	// Load persisted backfill progress from Pebble.
	if len(b.backfillTasks) > 0 {
		for _, task := range b.backfillTasks {
			if c, ok := b.readStore.ReadBackfillProgress(task.bbKey); ok {
				task.cursor = c
			}
		}
		for _, task := range b.backfillTasks {
			b.logger.WithFields(map[string]any{
				"ledger": task.ledger,
				"index":  backfillIndexName(task.index),
				"cursor": task.cursor,
			}).Infof("Loaded backfill task")
		}
	}

	// Resume any rewrite that was in flight when the previous process
	// stopped: every (ledger, indexedField) with pending_version != 0
	// AND a non-zero current_version (i.e. the local replica already
	// built v_current at some point) gets a schemaRewriteTask. The
	// atomic switch hasn't fired yet on this replica, so v_current
	// keeps serving queries while the rewrite catches up and v_pending
	// receives the new keyspace. Cursor and toType come from the
	// persisted BackfillCursor — the rewrite resumes mid-rmap-scan
	// instead of restarting from scratch.
	b.scheduleResumedRewrites()

	// Crash-recovery sweep: the atomic switch GCs v_old in the same
	// batch as the version promotion, so steady-state operation never
	// leaves orphan versions on disk. A crash mid-batch leaves either
	// a fully-pre-switch state (handled by resuming the rewrite via
	// pending_version) or a fully-post-switch state with no orphans.
	// What this sweep guards against is the long-tail case: a
	// re-retype that bumped pending past an in-flight rewrite (the
	// abandoned v_n is never the local current and never the new
	// pending, so its keyspace lingers), or a snapshot install whose
	// read-store delta dropped a version entry. Cheap unconditional
	// pass — DeleteRange on an empty range is a tombstone no-op.
	if err := b.purgeOrphanVersions(); err != nil {
		b.logger.Errorf("Failed to purge orphan index versions: %v", err)
	}
}

// seedLedgerIndexConfig enumerates every (non-deleted) ledger and seeds an
// empty ledgerIndexConfig per ledger. loadIndexRegistry then fills the cfg
// maps from the registry's actual entries.
func (b *Builder) seedLedgerIndexConfig(ctx context.Context, handle *dal.ReadHandle) error {
	cursor, err := query.ReadLedgers(ctx, handle)
	if err != nil {
		return fmt.Errorf("reading ledgers: %w", err)
	}

	defer func() { _ = cursor.Close() }()

	for {
		info, err := cursor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return fmt.Errorf("iterating ledgers: %w", err)
		}

		if info.GetDeletedAt() != nil {
			continue
		}

		b.indexConfig[info.GetName()] = newLedgerIndexConfig()
	}

	return nil
}

// loadIndexRegistry streams the SubAttrIndex zone and dispatches each entry
// to the matching ledgerIndexConfig (by Index.Ledger), then schedules a
// backfill for every BUILDING entry. Bucket-scoped entries (LedgerID == 0,
// Index.Ledger empty) are kept aside in bucketIndexConfig.
//
// Backfill scheduling logic for BUILDING entries:
//   - Non-metadata indexes (builtin tx/account/log): schedule unconditionally.
//     BuildStatus is the FSM-side signal for these — versioning doesn't apply.
//   - Metadata indexes: cross-check the local IndexVersionState cache.
//     If CurrentVersion != 0, the local replica already finished a prior
//     build, so any BUILDING flag reflects a *new* retype which is handled
//     by scheduleResumedRewrites (rewrite task) — NOT a backfill from
//     cursor 0. If CurrentVersion == 0, the local replica has never built
//     this index; a backfill IS needed to populate v_pending (or v=1 by
//     default).
func (b *Builder) loadIndexRegistry(handle *dal.ReadHandle) error {
	iter, err := b.attrs.Index.NewStreamingIter(handle, nil)
	if err != nil {
		return fmt.Errorf("opening index registry iterator: %w", err)
	}

	defer func() { _ = iter.Close() }()

	for iter.Next() {
		entry := iter.Entry()

		idx := entry.Value
		if idx == nil || idx.GetId() == nil {
			continue
		}

		ledgerName := idx.GetLedger()

		// Bucket-scoped entries are not attached to any ledger; the per-
		// ledger lookup map skips them. The bucket-scope cache lives next
		// to b.indexConfig for symmetry; introducing it as a separate field
		// avoids special-casing the empty-string key.
		if ledgerName == "" {
			if b.bucketIndexConfig == nil {
				b.bucketIndexConfig = newLedgerIndexConfig()
			}

			b.bucketIndexConfig.byCanonical[indexes.Canonical(idx.GetId())] = idx

			continue
		}

		cfg, ok := b.indexConfig[ledgerName]
		if !ok {
			// The ledger entry that owned this index was deleted but the
			// SubAttrIndex range wasn't purged in lock-step. Drop the entry
			// silently: an admin can re-run a compaction to clean up the
			// orphan keys.
			b.logger.WithFields(map[string]any{
				"ledger": ledgerName,
				"index":  indexes.Canonical(idx.GetId()),
			}).Infof("Skipping orphan index entry (no matching ledger)")

			continue
		}

		canonical := indexes.Canonical(idx.GetId())
		cfg.byCanonical[canonical] = idx

		if idx.GetBuildStatus() != commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			continue
		}

		if _, isMetadata := idx.GetId().GetKind().(*commonpb.IndexID_Metadata); isMetadata {
			current, _ := b.versionFor(ledgerName, canonical)
			if current != 0 {
				// The local replica already built v_current; the
				// resume path (scheduleResumedRewrites) owns the
				// BUILDING flag here.
				continue
			}
		}

		b.scheduleBackfillForIndex(ledgerName, idx.GetId())
	}

	return iter.Err()
}

// scheduleBackfillForIndex dispatches a backfill task for a freshly-created or
// recovered BUILDING index. Unknown kinds are silently ignored — future kinds
// (e.g. account_type) plug in their own scheduler here.
func (b *Builder) scheduleBackfillForIndex(ledgerName string, id *commonpb.IndexID) {
	switch k := id.GetKind().(type) {
	case *commonpb.IndexID_TxBuiltin:
		b.addBackfillTaskForTxBuiltin(ledgerName, k.TxBuiltin)
	case *commonpb.IndexID_LogBuiltin:
		b.addBackfillTaskForLogBuiltin(ledgerName, k.LogBuiltin)
	case *commonpb.IndexID_AccountBuiltin:
		// No account builtin backfills yet — placeholder for future kinds.
	case *commonpb.IndexID_Metadata:
		switch k.Metadata.GetTarget() {
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			b.addBackfillTaskForAcctMetadata(ledgerName, k.Metadata.GetKey())
		case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
			b.addBackfillTaskForTxMetadata(ledgerName, k.Metadata.GetKey())
		}
	}
}

// stripBuildingIndexes temporarily removes BUILDING indexes from all configs,
// returning a restore function. This is used during the initial catch-up to
// skip redundant writes — backfill tasks will handle those ranges independently.
// After restore, the normal loop includes BUILDING indexes for new incoming logs.
func (b *Builder) stripBuildingIndexes() func() {
	type stripped struct {
		ledger string
		key    string
		entry  *commonpb.Index
	}

	var removed []stripped

	for _, task := range b.backfillTasks {
		cfg := b.indexConfig[task.ledger]
		if cfg == nil {
			continue
		}

		key := indexes.Canonical(task.index)

		entry, ok := cfg.byCanonical[key]
		if !ok {
			continue
		}

		removed = append(removed, stripped{ledger: task.ledger, key: key, entry: entry})
		delete(cfg.byCanonical, key)
	}

	return func() {
		for _, s := range removed {
			cfg := b.indexConfig[s.ledger]
			if cfg == nil {
				continue
			}

			cfg.byCanonical[s.key] = s.entry
		}
	}
}

// isIndexed returns true iff the index identified by id is registered in the
// cache (READY or BUILDING). Nil-safe on the receiver.
func (c *ledgerIndexConfig) isIndexed(id *commonpb.IndexID) bool {
	if c == nil || id == nil {
		return false
	}

	_, ok := c.byCanonical[indexes.Canonical(id)]

	return ok
}

// isMetadataIndexed checks if a specific metadata index is enabled.
// Returns false if the receiver is nil (unknown ledger).
func (c *ledgerIndexConfig) isMetadataIndexed(target commonpb.TargetType, key string) bool {
	return c.isIndexed(indexes.MetadataID(target, key))
}

// isBuiltinIndexed checks if a specific transaction builtin index is enabled.
// Returns false if the receiver is nil (unknown ledger).
func (c *ledgerIndexConfig) isBuiltinIndexed(index commonpb.TransactionBuiltinIndex) bool {
	return c.isIndexed(indexes.TxBuiltinID(index))
}

func (c *ledgerIndexConfig) indexesPostingAddressMappings() bool {
	return c.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS) ||
		c.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS) ||
		c.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS)
}

// isLogBuiltinIndexed checks if a specific log builtin index is enabled.
// Returns false if the receiver is nil (unknown ledger).
func (c *ledgerIndexConfig) isLogBuiltinIndexed(index commonpb.LogBuiltinIndex) bool {
	return c.isIndexed(indexes.LogBuiltinID(index))
}

// ledgerConfig returns the index config for a ledger, or nil if unknown.
func (b *Builder) ledgerConfig(ledger string) *ledgerIndexConfig {
	return b.indexConfig[ledger]
}

// getOrCreateLedgerConfig returns the index config for a ledger, creating it if needed.
func (b *Builder) getOrCreateLedgerConfig(ledger string) *ledgerIndexConfig {
	cfg, ok := b.indexConfig[ledger]
	if !ok {
		cfg = newLedgerIndexConfig()
		b.indexConfig[ledger] = cfg
	}

	return cfg
}

// handleCreatedIndexLog updates the index config cache when a CreateIndex log is processed.
// The index starts in BUILDING state — it is NOT marked as ready here.
// A backfill task is created to replay historical logs for the new index.
//
// Idempotency: when the same CreateIndex is replayed against an index that
// is already cached as READY (the processor short-circuited a duplicate
// create on an already-built index), we skip the backfill scheduling so the
// builder does not redo work that has already completed.
func (b *Builder) handleCreatedIndexLog(ledgerName string, log *commonpb.CreatedIndexLog) {
	id := log.GetId()
	if id == nil {
		return
	}

	cfg := b.getOrCreateLedgerConfig(ledgerName)

	if existing := cfg.byCanonical[indexes.Canonical(id)]; existing != nil &&
		existing.GetBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY {
		return
	}

	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{
		Id:                     id,
		BuildStatus:            commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
		ForwardEncodingVersion: 1,
	}

	// First time this replica sees the index: target v=1 via the
	// backfill task. current stays at 0 until the backfill completes
	// and switches it via the atomic-switch path. Persisted in the
	// active batch so the per-replica readiness signal survives a
	// crash between CreateIndex apply and backfill completion — the
	// boot recovery would otherwise have to guess from cfg.byCanonical
	// alone, which loses the distinction between "fresh index" and
	// "stale READY index from a snapshot install".
	state := readstore.IndexVersionState{
		CurrentVersion: 0,
		PendingVersion: 1,
	}

	if b.wb != nil && b.readStore != nil {
		if batch := b.wb.Batch(); batch != nil {
			if err := b.readStore.WriteIndexVersionState(batch, ledgerName, indexes.Canonical(id), state); err != nil {
				b.logger.WithFields(map[string]any{
					"ledger": ledgerName,
					"index":  indexes.Canonical(id),
					"error":  err,
				}).Errorf("Persisting IndexVersionState on CreateIndex")
			}
		}
	}

	b.putVersionState(ledgerName, indexes.Canonical(id), state)

	b.scheduleBackfillForIndex(ledgerName, id)
}

// handleDroppedIndexLog updates the index config cache when a DropIndex log is processed.
// It also removes any active backfill / schema-rewrite task tied to the
// dropped index — without that, a rewrite finishing post-drop would wait
// forever for an IndexReady that applyIndexReady silently ignores once
// the index has been removed.
func (b *Builder) handleDroppedIndexLog(ledger string, log *commonpb.DroppedIndexLog) {
	id := log.GetId()
	if id == nil {
		return
	}

	cfg := b.getOrCreateLedgerConfig(ledger)
	delete(cfg.byCanonical, indexes.Canonical(id))
	b.removeBackfillTask(ledger, id)
	b.dropVersionState(ledger, indexes.Canonical(id))
	_ = b.readStore.DeleteIndexVersionState(ledger, indexes.Canonical(id))

	if meta, ok := id.GetKind().(*commonpb.IndexID_Metadata); ok && meta.Metadata != nil {
		b.removeSchemaRewriteTaskByField(ledger, meta.Metadata.GetTarget(), meta.Metadata.GetKey())
	}
}
