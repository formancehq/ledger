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
func (b *Builder) initIndexConfig(ctx context.Context) error {
	// Reset builder-local init state so every attempt (including a retry
	// after a partial failure) starts from a clean slate. backfillTasks
	// and schemaRewriteTasks are slices appended to by
	// scheduleBackfillForIndex / scheduleResumedRewrites; without this
	// reset a retry would double-schedule them. The maps are keyed and
	// would only be overwritten, but are reset too for a self-contained
	// attempt.
	b.indexConfig = make(map[string]*ledgerIndexConfig)
	b.bucketIndexConfig = nil
	b.backfillTasks = nil
	b.schemaRewriteTasks = nil
	b.indexVersions = nil

	handle, err := b.pebbleStore.NewDirectReadHandle()
	if err != nil {
		return fmt.Errorf("creating read handle for index config: %w", err)
	}

	defer func() { _ = handle.Close() }()

	// Restore the per-replica forward-encoding versions from the read
	// store. Done before the ledger scan so loadIndexRegistry can
	// consult the cache when deciding whether to schedule a backfill.
	versionEntries, err := b.readStore.ReadAllIndexVersionStates()
	if err != nil {
		return fmt.Errorf("reading index version state: %w", err)
	}

	for _, e := range versionEntries {
		b.putVersionState(e.LedgerName, e.CanonicalID, e.State)
	}

	if err := b.seedLedgerIndexConfig(ctx, handle); err != nil {
		return fmt.Errorf("seeding ledger index config: %w", err)
	}

	if err := b.loadIndexRegistry(handle); err != nil {
		return fmt.Errorf("loading index registry: %w", err)
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

	return nil
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
// Backfill scheduling for BUILDING entries cross-checks the local
// IndexVersionState cache, which every index kind (builtin tx/account/log and
// metadata) maintains identically — CreateIndex seeds {current:0, pending:1}
// and completeBackfill promotes to {current:1, pending:0}:
//   - CurrentVersion == 0: this replica has never built the index; a backfill
//     IS needed to populate v_pending (v=1 by default).
//   - CurrentVersion != 0: this replica already finished the backfill. The
//     BUILDING flag only lingers because the cluster-wide READY flip (a
//     non-audited TechnicalUpdate) hasn't landed yet, or — for metadata — a
//     *new* retype owned by scheduleResumedRewrites (a rewrite task, NOT a
//     backfill from cursor 0). Re-scheduling would re-run a completed backfill
//     and trip the pending==0 invariant in completeBackfill, so it is skipped.
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

		// Every index kind (builtin tx/account/log and metadata) records a
		// per-replica IndexVersionState: handleCreatedIndexLog seeds
		// {current:0, pending:1} and completeBackfill promotes it to
		// {current:1, pending:0}. A non-zero current_version therefore means
		// this replica already finished the backfill; the lingering BUILDING
		// flag only reflects the cluster-wide READY flip (a non-audited
		// TechnicalUpdate) not having landed yet, or — for metadata — a retype
		// owned by scheduleResumedRewrites. Re-scheduling here would re-run a
		// completed backfill and trip the pending==0 invariant in
		// completeBackfill, stranding the task in a BUILDING logging loop. A
		// backfill is only needed while current_version == 0 (never built
		// locally); a drop+recreate clears the version state so a genuine
		// rebuild still re-enters this branch with current == 0.
		if current, _ := b.versionFor(ledgerName, canonical); current != 0 {
			continue
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
		// Only the account has-asset index has a posting-replay backfill;
		// other account builtin kinds plug in here as they land.
		if k.AccountBuiltin == commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET {
			b.addBackfillTaskForAccountBuiltin(ledgerName, k.AccountBuiltin)
		}
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
		c.isBuiltinIndexed(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS)
}

// indexesPostingDerived reports whether any enabled index is derived from
// transaction postings and therefore needs the per-log excluded-volume set
// (transient/purged volumes) to skip ephemeral accounts. Covers the tx address
// mappings and the account has-asset index — the account-asset index alone is
// enough to require exclusion, otherwise asset-presence rows would be written
// for transient/purged volumes on ledgers that enable only that index.
func (c *ledgerIndexConfig) indexesPostingDerived() bool {
	return c.indexesPostingAddressMappings() ||
		c.isAccountBuiltinIndexed(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET)
}

// isLogBuiltinIndexed checks if a specific log builtin index is enabled.
// Returns false if the receiver is nil (unknown ledger).
func (c *ledgerIndexConfig) isLogBuiltinIndexed(index commonpb.LogBuiltinIndex) bool {
	return c.isIndexed(indexes.LogBuiltinID(index))
}

// isAccountBuiltinIndexed reports whether the given account builtin index is
// registered (regardless of build status) for this ledger config.
// Returns false if the receiver is nil (unknown ledger).
func (c *ledgerIndexConfig) isAccountBuiltinIndexed(index commonpb.AccountBuiltinIndex) bool {
	return c.isIndexed(indexes.AccountBuiltinID(index))
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
// Initial fast path (EN-1564): when the log carries the initial flag (index
// declared on a born-empty ledger), there is no local history to replay — the
// index is promoted straight to live (current=1) and NO backfill is scheduled.
//
// Idempotency: when the same CreateIndex is replayed (or re-submitted) against
// an index this replica has already promoted to live, we skip the reset and
// backfill scheduling so the builder does not redo work that has already
// completed — and, more importantly, does not knock a live index back into
// ErrIndexBuilding.
func (b *Builder) handleCreatedIndexLog(ledgerName string, log *commonpb.CreatedIndexLog) {
	id := log.GetId()
	if id == nil {
		return
	}

	cfg := b.getOrCreateLedgerConfig(ledgerName)

	// Post-EN-1323 the per-replica readiness signal is
	// IndexVersionState.CurrentVersion, not the (now purely informational)
	// registry BuildStatus — nothing ever flips BuildStatus back to READY, so a
	// BuildStatus-based short-circuit here is dead. If this replica has already
	// promoted the index to live (current != 0), a repeated CreatedIndexLog —
	// a duplicate CreateIndex re-emitted by the processor, or an apply replay —
	// must be a no-op: re-seeding {current:0, pending:1} and rescheduling a
	// backfill would flip an already-live index back to ErrIndexBuilding. This
	// mirrors the loadIndexRegistry boot guard and covers both the EN-1564
	// initial fast path and the normal post-backfill live state.
	if current, _ := b.versionFor(ledgerName, indexes.Canonical(id)); current != 0 {
		return
	}

	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{
		Id:                     id,
		BuildStatus:            commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
		ForwardEncodingVersion: 1,
	}

	// EN-1564: an index declared on a born-empty ledger has no local history to
	// replay. Promote it straight to live (current=1) and skip the backfill;
	// the live indexing path maintains it from ledger birth. Persist so a reboot
	// sees current!=0 and loadIndexRegistry skips scheduling a backfill.
	if log.GetInitial() {
		state := readstore.IndexVersionState{
			CurrentVersion: 1,
			PendingVersion: 0,
		}

		if b.wb != nil && b.readStore != nil {
			if batch := b.wb.Batch(); batch != nil {
				if err := b.readStore.WriteIndexVersionState(batch, ledgerName, indexes.Canonical(id), state); err != nil {
					b.logger.WithFields(map[string]any{
						"ledger": ledgerName,
						"index":  indexes.Canonical(id),
						"error":  err,
					}).Errorf("Persisting IndexVersionState on initial CreateIndex")
				}
			}
		}

		b.putVersionState(ledgerName, indexes.Canonical(id), state)

		return
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
