package indexbuilder

import (
	"context"
	"errors"
	"io"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

// indexID identifies a specific index (transaction, account, or log builtin).
type indexID struct {
	transaction *commonpb.TransactionIndex // set for transaction indexes (builtin or metadata)
	account     *commonpb.AccountIndex     // set for account indexes (builtin or metadata)
	logBuiltin  *commonpb.LogBuiltinIndex  // set for log builtin indexes
}

// ledgerIndexConfig caches which indexes are enabled and ready for a ledger.
type ledgerIndexConfig struct {
	txMetadataIndexed   map[string]bool                           // transaction metadata key → indexed
	txBuiltinIndexed    map[commonpb.TransactionBuiltinIndex]bool // transaction builtin → indexed
	acctMetadataIndexed map[string]bool                           // account metadata key → indexed
	acctBuiltinIndexed  map[commonpb.AccountBuiltinIndex]bool     // account builtin → indexed
	logBuiltinIndexed   map[commonpb.LogBuiltinIndex]bool         // log builtin → indexed
}

// newLedgerIndexConfig creates a new ledgerIndexConfig with all maps initialized.
func newLedgerIndexConfig() *ledgerIndexConfig {
	return &ledgerIndexConfig{
		txMetadataIndexed:   make(map[string]bool),
		txBuiltinIndexed:    make(map[commonpb.TransactionBuiltinIndex]bool),
		acctMetadataIndexed: make(map[string]bool),
		acctBuiltinIndexed:  make(map[commonpb.AccountBuiltinIndex]bool),
		logBuiltinIndexed:   make(map[commonpb.LogBuiltinIndex]bool),
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

	// Recover schema rewrite tasks from Pebble.
	entries, err := b.readStore.ReadAllSchemaRewriteProgress()
	if err != nil {
		b.logger.Errorf("Failed to read schema rewrite progress: %v", err)
	} else {
		for _, e := range entries {
			b.schemaRewriteTasks = append(b.schemaRewriteTasks, &schemaRewriteTask{
				ledgerID:   e.LedgerID,
				targetType: commonpb.TargetType(e.TargetType),
				key:        e.Key,
				toType:     commonpb.MetadataType(e.ToType),
				rmapCursor: e.Cursor,
				bbKey:      e.BBKey,
			})
		}
	}

	if len(b.schemaRewriteTasks) > 0 {
		b.logger.WithFields(map[string]any{
			"count": len(b.schemaRewriteTasks),
		}).Infof("Recovered schema rewrite tasks")
	}
}

// loadLedgerIndexConfig populates the index config cache for a single ledger.
// Both READY and BUILDING indexes are included so that normal processing writes
// to new indexes immediately (covering logs after CreateIndex).
func (b *Builder) loadLedgerIndexConfig(info *commonpb.LedgerInfo) {
	cfg := newLedgerIndexConfig()

	// Metadata indexes — include both READY and BUILDING.
	if info.GetMetadataSchema() != nil {
		b.loadMetadataIndexes(cfg, info.GetName(), info.GetId(), commonpb.TargetType_TARGET_TYPE_ACCOUNT, info.GetMetadataSchema().GetAccountFields())
		b.loadMetadataIndexes(cfg, info.GetName(), info.GetId(), commonpb.TargetType_TARGET_TYPE_TRANSACTION, info.GetMetadataSchema().GetTransactionFields())
	}

	// Builtin transaction indexes (including address indexes) — include both READY and BUILDING.
	if bi := info.GetBuiltinIndexes(); bi != nil {
		for _, entry := range []struct {
			index   commonpb.TransactionBuiltinIndex
			enabled bool
			status  commonpb.IndexBuildStatus
		}{
			{commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE, bi.GetReference(), bi.GetReferenceStatus()},
			{commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP, bi.GetTimestamp(), bi.GetTimestampStatus()},
			{commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS, bi.GetAddress(), bi.GetAddressStatus()},
			{commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS, bi.GetSourceAddress(), bi.GetSourceAddressStatus()},
			{commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS, bi.GetDestAddress(), bi.GetDestAddressStatus()},
			{commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT, bi.GetInsertedAt(), bi.GetInsertedAtStatus()},
		} {
			if !entry.enabled {
				continue
			}

			cfg.txBuiltinIndexed[entry.index] = true
			if entry.status == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
				b.addBackfillTaskForTxBuiltin(info.GetName(), info.GetId(), entry.index)
			}
		}
	}

	// Builtin log indexes — include both READY and BUILDING.
	if li := info.GetLogBuiltinIndexes(); li != nil {
		for _, entry := range []struct {
			index   commonpb.LogBuiltinIndex
			enabled bool
			status  commonpb.IndexBuildStatus
		}{
			{commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER, li.GetLedger(), li.GetLedgerStatus()},
			{commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE, li.GetDate(), li.GetDateStatus()},
		} {
			if !entry.enabled {
				continue
			}

			cfg.logBuiltinIndexed[entry.index] = true
			if entry.status == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
				b.addBackfillTaskForLogBuiltin(info.GetName(), info.GetId(), entry.index)
			}
		}
	}

	b.indexConfig[info.GetName()] = cfg
}

// stripBuildingIndexes temporarily removes BUILDING indexes from all configs,
// returning a restore function. This is used during the initial catch-up to
// skip redundant writes — backfill tasks will handle those ranges independently.
// After restore, the normal loop includes BUILDING indexes for new incoming logs.
func (b *Builder) stripBuildingIndexes() func() {
	// Remove each backfill task's index from the config.
	for _, task := range b.backfillTasks {
		cfg := b.indexConfig[task.ledger]
		if cfg == nil {
			continue
		}

		if task.index.transaction != nil {
			switch txIdx := task.index.transaction.GetKind().(type) {
			case *commonpb.TransactionIndex_Builtin:
				delete(cfg.txBuiltinIndexed, txIdx.Builtin)
			case *commonpb.TransactionIndex_MetadataKey:
				delete(cfg.txMetadataIndexed, txIdx.MetadataKey)
			}
		}

		if task.index.account != nil {
			if acctIdx, ok := task.index.account.GetKind().(*commonpb.AccountIndex_MetadataKey); ok {
				delete(cfg.acctMetadataIndexed, acctIdx.MetadataKey)
			}
		}

		if task.index.logBuiltin != nil {
			delete(cfg.logBuiltinIndexed, *task.index.logBuiltin)
		}
	}

	// Return a restore function that adds the BUILDING indexes back.
	return func() {
		for _, task := range b.backfillTasks {
			cfg := b.indexConfig[task.ledger]
			if cfg == nil {
				continue
			}

			if task.index.transaction != nil {
				switch txIdx := task.index.transaction.GetKind().(type) {
				case *commonpb.TransactionIndex_Builtin:
					cfg.txBuiltinIndexed[txIdx.Builtin] = true
				case *commonpb.TransactionIndex_MetadataKey:
					cfg.txMetadataIndexed[txIdx.MetadataKey] = true
				}
			}

			if task.index.account != nil {
				if acctIdx, ok := task.index.account.GetKind().(*commonpb.AccountIndex_MetadataKey); ok {
					cfg.acctMetadataIndexed[acctIdx.MetadataKey] = true
				}
			}

			if task.index.logBuiltin != nil {
				cfg.logBuiltinIndexed[*task.index.logBuiltin] = true
			}
		}
	}
}

// loadMetadataIndexes loads metadata indexes for a given target type.
func (b *Builder) loadMetadataIndexes(
	cfg *ledgerIndexConfig,
	ledgerName string,
	ledgerID uint32,
	target commonpb.TargetType,
	fields map[string]*commonpb.MetadataFieldSchema,
) {
	for key, field := range fields {
		if !field.GetIndexed() {
			continue
		}

		switch target {
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			cfg.acctMetadataIndexed[key] = true
			if field.GetIndexBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
				b.addBackfillTaskForAcctMetadata(ledgerName, ledgerID, key)
			}
		case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
			cfg.txMetadataIndexed[key] = true
			if field.GetIndexBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING {
				b.addBackfillTaskForTxMetadata(ledgerName, ledgerID, key)
			}
		}
	}
}

// isMetadataIndexed checks if a specific metadata index is enabled.
// Returns false if the receiver is nil (unknown ledger).
func (c *ledgerIndexConfig) isMetadataIndexed(target commonpb.TargetType, key string) bool {
	if c == nil {
		return false
	}

	switch target {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		return c.acctMetadataIndexed[key]
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		return c.txMetadataIndexed[key]
	default:
		return false
	}
}

// isBuiltinIndexed checks if a specific builtin index is enabled.
// Returns false if the receiver is nil (unknown ledger).
func (c *ledgerIndexConfig) isBuiltinIndexed(index commonpb.TransactionBuiltinIndex) bool {
	if c == nil {
		return false
	}

	return c.txBuiltinIndexed[index]
}

// isLogBuiltinIndexed checks if a specific log builtin index is enabled.
// Returns false if the receiver is nil (unknown ledger).
func (c *ledgerIndexConfig) isLogBuiltinIndexed(index commonpb.LogBuiltinIndex) bool {
	if c == nil {
		return false
	}

	return c.logBuiltinIndexed[index]
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
func (b *Builder) handleCreatedIndexLog(ledger string, ledgerID uint32, log *commonpb.CreatedIndexLog) {
	cfg := b.getOrCreateLedgerConfig(ledger)

	switch idx := log.GetIndex().(type) {
	case *commonpb.CreatedIndexLog_Transaction:
		switch txIdx := idx.Transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			cfg.txBuiltinIndexed[txIdx.Builtin] = true
			b.addBackfillTaskForTxBuiltin(ledger, ledgerID, txIdx.Builtin)
		case *commonpb.TransactionIndex_MetadataKey:
			cfg.txMetadataIndexed[txIdx.MetadataKey] = true
			b.addBackfillTaskForTxMetadata(ledger, ledgerID, txIdx.MetadataKey)
		}
	case *commonpb.CreatedIndexLog_Account:
		switch acctIdx := idx.Account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			cfg.acctBuiltinIndexed[acctIdx.Builtin] = true
			// No backfill function for account builtins yet — add when needed.
		case *commonpb.AccountIndex_MetadataKey:
			cfg.acctMetadataIndexed[acctIdx.MetadataKey] = true
			b.addBackfillTaskForAcctMetadata(ledger, ledgerID, acctIdx.MetadataKey)
		}
	case *commonpb.CreatedIndexLog_LogBuiltin:
		cfg.logBuiltinIndexed[idx.LogBuiltin] = true
		b.addBackfillTaskForLogBuiltin(ledger, ledgerID, idx.LogBuiltin)
	}
}

// handleDroppedIndexLog updates the index config cache when a DropIndex log is processed.
// It also removes any active backfill task for the dropped index.
func (b *Builder) handleDroppedIndexLog(ledger string, log *commonpb.DroppedIndexLog) {
	cfg := b.getOrCreateLedgerConfig(ledger)

	switch idx := log.GetIndex().(type) {
	case *commonpb.DroppedIndexLog_Transaction:
		switch txIdx := idx.Transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			delete(cfg.txBuiltinIndexed, txIdx.Builtin)
			b.removeBackfillTask(indexID{transaction: idx.Transaction})
		case *commonpb.TransactionIndex_MetadataKey:
			delete(cfg.txMetadataIndexed, txIdx.MetadataKey)
			b.removeBackfillTask(indexID{transaction: idx.Transaction})
		}
	case *commonpb.DroppedIndexLog_Account:
		switch acctIdx := idx.Account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			delete(cfg.acctBuiltinIndexed, acctIdx.Builtin)
			b.removeBackfillTask(indexID{account: idx.Account})
		case *commonpb.AccountIndex_MetadataKey:
			delete(cfg.acctMetadataIndexed, acctIdx.MetadataKey)
			b.removeBackfillTask(indexID{account: idx.Account})
		}
	case *commonpb.DroppedIndexLog_LogBuiltin:
		delete(cfg.logBuiltinIndexed, idx.LogBuiltin)
		b.removeBackfillTask(indexID{logBuiltin: &idx.LogBuiltin})
	}
}

// matchesBackfillIndex checks if two indexIDs represent the same index.
func matchesBackfillIndex(a, b indexID) bool {
	if a.transaction != nil && b.transaction != nil {
		return matchesTransactionIndex(a.transaction, b.transaction)
	}

	if a.account != nil && b.account != nil {
		return matchesAccountIndex(a.account, b.account)
	}

	if a.logBuiltin != nil && b.logBuiltin != nil {
		return *a.logBuiltin == *b.logBuiltin
	}

	return false
}

// matchesTransactionIndex checks if two TransactionIndex values represent the same index.
func matchesTransactionIndex(a, b *commonpb.TransactionIndex) bool {
	switch ak := a.GetKind().(type) {
	case *commonpb.TransactionIndex_Builtin:
		if bk, ok := b.GetKind().(*commonpb.TransactionIndex_Builtin); ok {
			return ak.Builtin == bk.Builtin
		}
	case *commonpb.TransactionIndex_MetadataKey:
		if bk, ok := b.GetKind().(*commonpb.TransactionIndex_MetadataKey); ok {
			return ak.MetadataKey == bk.MetadataKey
		}
	}

	return false
}

// matchesAccountIndex checks if two AccountIndex values represent the same index.
func matchesAccountIndex(a, b *commonpb.AccountIndex) bool {
	switch ak := a.GetKind().(type) {
	case *commonpb.AccountIndex_Builtin:
		if bk, ok := b.GetKind().(*commonpb.AccountIndex_Builtin); ok {
			return ak.Builtin == bk.Builtin
		}
	case *commonpb.AccountIndex_MetadataKey:
		if bk, ok := b.GetKind().(*commonpb.AccountIndex_MetadataKey); ok {
			return ak.MetadataKey == bk.MetadataKey
		}
	}

	return false
}
