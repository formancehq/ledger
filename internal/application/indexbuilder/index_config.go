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

	b.recoverSchemaRewriteTasks()

	if len(b.schemaRewriteTasks) > 0 {
		b.logger.WithFields(map[string]any{
			"count": len(b.schemaRewriteTasks),
		}).Infof("Recovered schema rewrite tasks")
	}
}

// recoverSchemaRewriteTasks rebuilds in-flight schema rewrite tasks from
// the persisted progress entries. The persisted entry only carries
// ledgerID, so we resolve it back to ledgerName via b.ledgerNameToID
// (seeded by loadLedgerIndexConfig). Without the resolved name,
// proposeSchemaRewriteIndexReady would emit IndexReadyUpdate{Ledger: ""}
// and isSchemaRewriteIndexReady would call query.GetLedgerByName("") —
// always NotFound — so the task would loop forever and the index would
// stay BUILDING across restart (PR #277 review).
func (b *Builder) recoverSchemaRewriteTasks() {
	entries, err := b.readStore.ReadAllSchemaRewriteProgress()
	if err != nil {
		b.logger.Errorf("Failed to read schema rewrite progress: %v", err)

		return
	}

	idToName := make(map[uint32]string, len(b.ledgerNameToID))
	for name, id := range b.ledgerNameToID {
		idToName[id] = name
	}

	for _, e := range entries {
		ledgerName, ok := idToName[e.LedgerID]
		if !ok {
			// The ledger this task was attached to no longer exists
			// (deleted while the task was persisted). Drop the entry —
			// keeping it would leave the task stuck forever.
			b.logger.WithFields(map[string]any{
				"ledgerID":   e.LedgerID,
				"targetType": e.TargetType,
				"key":        e.Key,
			}).Errorf("Dropping schema rewrite task for unknown ledger")

			continue
		}

		b.schemaRewriteTasks = append(b.schemaRewriteTasks, &schemaRewriteTask{
			ledger:     ledgerName,
			ledgerID:   e.LedgerID,
			targetType: commonpb.TargetType(e.TargetType),
			key:        e.Key,
			toType:     commonpb.MetadataType(e.ToType),
			rmapCursor: e.Cursor,
			bbKey:      e.BBKey,
		})
	}
}

// loadLedgerIndexConfig populates the index config cache for a single ledger.
// Both READY and BUILDING indexes are included so that normal processing writes
// to new indexes immediately (covering logs after CreateIndex).
func (b *Builder) loadLedgerIndexConfig(info *commonpb.LedgerInfo) {
	// Seed the name → ID lookup so logs that arrive AFTER the persisted cursor
	// can still resolve their ledger ID. process_logs.go used to populate this
	// map exclusively from CreateLedger payloads — but on a restart the cursor
	// is past those, so every pre-existing ledger fell back to the map's zero
	// value (0). Indexes were then written under ledgerID=0 while queries used
	// the real id from LedgerInfo, silently freezing the read index (#304).
	// recoverSchemaRewriteTasks also depends on this map to resolve persisted
	// ledgerIDs back to names during recovery (#277).
	b.ledgerNameToID[info.GetName()] = info.GetId()

	cfg := newLedgerIndexConfig()

	for _, idx := range info.GetIndexes() {
		if idx.GetId() == nil {
			continue
		}

		cfg.byCanonical[indexes.Canonical(idx.GetId())] = idx

		if idx.GetBuildStatus() != commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
			continue
		}

		b.scheduleBackfillForIndex(info.GetName(), info.GetId(), idx.GetId())
	}

	b.indexConfig[info.GetName()] = cfg
}

// scheduleBackfillForIndex dispatches a backfill task for a freshly-created or
// recovered BUILDING index. Unknown kinds are silently ignored — future kinds
// (e.g. account_type) plug in their own scheduler here.
func (b *Builder) scheduleBackfillForIndex(ledger string, ledgerID uint32, id *commonpb.IndexID) {
	switch k := id.GetKind().(type) {
	case *commonpb.IndexID_TxBuiltin:
		b.addBackfillTaskForTxBuiltin(ledger, ledgerID, k.TxBuiltin)
	case *commonpb.IndexID_LogBuiltin:
		b.addBackfillTaskForLogBuiltin(ledger, ledgerID, k.LogBuiltin)
	case *commonpb.IndexID_AccountBuiltin:
		// No account builtin backfills yet — placeholder for future kinds.
	case *commonpb.IndexID_Metadata:
		switch k.Metadata.GetTarget() {
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			b.addBackfillTaskForAcctMetadata(ledger, ledgerID, k.Metadata.GetKey())
		case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
			b.addBackfillTaskForTxMetadata(ledger, ledgerID, k.Metadata.GetKey())
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
func (b *Builder) handleCreatedIndexLog(ledger string, ledgerID uint32, log *commonpb.CreatedIndexLog) {
	id := log.GetId()
	if id == nil {
		return
	}

	cfg := b.getOrCreateLedgerConfig(ledger)

	if existing := cfg.byCanonical[indexes.Canonical(id)]; existing != nil &&
		existing.GetBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY {
		return
	}

	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{
		Id:          id,
		BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
	}

	b.scheduleBackfillForIndex(ledger, ledgerID, id)
}

// handleDroppedIndexLog updates the index config cache when a DropIndex log is processed.
// It also removes any active backfill task for the dropped index.
func (b *Builder) handleDroppedIndexLog(ledger string, log *commonpb.DroppedIndexLog) {
	id := log.GetId()
	if id == nil {
		return
	}

	cfg := b.getOrCreateLedgerConfig(ledger)
	delete(cfg.byCanonical, indexes.Canonical(id))
	b.removeBackfillTask(id)
}
