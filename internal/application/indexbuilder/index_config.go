package indexbuilder

import (
	"context"
	"errors"
	"io"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
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

// initIndexConfig scans all ledgers from Pebble and populates the index config cache.
// It also detects BUILDING indexes and creates backfill tasks, loading persisted
// cursors from Pebble so backfills survive restarts.
func (b *Builder) initIndexConfig(ctx context.Context) {
	handle, err := b.pebbleStore.NewDirectReadHandle()
	if err != nil {
		b.logger.Errorf("Failed to create read handle for index config: %v", err)

		return
	}

	defer func() { _ = handle.Close() }()

	cursor, err := query.ReadLedgers(ctx, handle)
	if err != nil {
		b.logger.Errorf("Failed to read ledgers for index config: %v", err)

		return
	}

	defer func() { _ = cursor.Close() }()

	for {
		info, err := cursor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			b.logger.Errorf("Error reading ledger info: %v", err)

			return
		}

		if info.GetDeletedAt() != nil {
			continue
		}

		b.loadLedgerIndexConfig(info)
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

	// Recover unfinished schema rewrites left over from a previous boot.
	//
	// IndexReady is proposed by the first node whose local rewrite
	// completes — it is NOT a cluster-wide "all nodes done" signal. So a
	// node can crash mid-rewrite, another node can finish and apply
	// IndexReady, and this node reboots with LedgerInfo.READY but a
	// forward index that still mixes pre/post-retype encodings. Relying
	// on the BUILDING flag alone (via scheduleBackfillForIndex above)
	// misses that case because BUILDING is already gone.
	//
	// Mitigation: any persisted schema-rewrite cursor signals an
	// unfinished local rewrite at the previous shutdown. Schedule a
	// full backfill from cursor 0 for the corresponding metadata index
	// — even if LedgerInfo says READY. The replay re-encodes the
	// forward under the current declared_type via the per-batch
	// schemaResolver; cluster-wide IndexReady has already been
	// proposed, so no new proposal will fire from this node when the
	// backfill finishes. Once scheduled, delete the persisted cursor —
	// the backfill keeps its own cursor under a different bbKey.
	//
	// Cluster-wide "consensus IndexReady" is the long-term fix
	// tracked in LED-XXX; this recovery is the stopgap.
	rewriteEntries, err := b.readStore.ReadAllSchemaRewriteProgress()
	if err != nil {
		b.logger.Errorf("Failed to read schema-rewrite progress: %v", err)

		return
	}

	for _, entry := range rewriteEntries {
		target := commonpb.TargetType(entry.TargetType)
		if target != commonpb.TargetType_TARGET_TYPE_ACCOUNT &&
			target != commonpb.TargetType_TARGET_TYPE_TRANSACTION {
			// Unknown target — drop the cursor and move on.
			_ = b.readStore.DeleteBackfillProgress(entry.BBKey)

			continue
		}

		id := indexes.MetadataID(target, entry.Key)
		b.scheduleBackfillForIndex(entry.LedgerName, id)

		// Force the backfill to restart from 0: the schemaResolver
		// re-encodes every replayed log under the new declared_type.
		for _, bt := range b.backfillTasks {
			if bt.ledger == entry.LedgerName && indexes.Equal(bt.index, id) {
				bt.cursor = 0
				bt.appliedProposalSeq = 0
				bt.proposed = false
				bt.lastProgressSeq = 0
				_ = b.readStore.DeleteBackfillProgress(bt.bbKey)

				break
			}
		}

		// The schema-rewrite cursor itself is no longer used.
		_ = b.readStore.DeleteBackfillProgress(entry.BBKey)

		b.logger.WithFields(map[string]any{
			"ledger": entry.LedgerName,
			"target": target.String(),
			"key":    entry.Key,
		}).Infof("Recovered unfinished schema rewrite — scheduled full backfill")
	}
}

// loadLedgerIndexConfig populates the index config cache for a single ledger.
// Both READY and BUILDING indexes are included so that normal processing writes
// to new indexes immediately (covering logs after CreateIndex).
func (b *Builder) loadLedgerIndexConfig(info *commonpb.LedgerInfo) {
	cfg := newLedgerIndexConfig()

	for _, idx := range info.GetIndexes() {
		if idx.GetId() == nil {
			continue
		}

		cfg.byCanonical[indexes.Canonical(idx.GetId())] = idx

		if idx.GetBuildStatus() != commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			continue
		}

		b.scheduleBackfillForIndex(info.GetName(), idx.GetId())
	}

	b.indexConfig[info.GetName()] = cfg
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
		Id:          id,
		BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
	}

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

	if meta, ok := id.GetKind().(*commonpb.IndexID_Metadata); ok && meta.Metadata != nil {
		b.removeSchemaRewriteTaskByField(ledger, meta.Metadata.GetTarget(), meta.Metadata.GetKey())
	}
}
