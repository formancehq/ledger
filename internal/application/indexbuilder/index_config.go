package indexbuilder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// ledgerIndexConfig caches which indexes are enabled for a ledger.
// Keyed by IndexID canonical form (indexes.Canonical) for O(1) lookup; the
// stored Index carries the identity needed by the fold. Boot reconstructs it
// without importing newer audit metadata from the main-store registry.
type ledgerIndexConfig struct {
	byCanonical map[string]*commonpb.Index
}

// newLedgerIndexConfig creates a new ledgerIndexConfig with the map initialized.
func newLedgerIndexConfig() *ledgerIndexConfig {
	return &ledgerIndexConfig{
		byCanonical: make(map[string]*commonpb.Index),
	}
}

// initIndexConfig restores the fold configuration from the read-store history
// and version states at its persisted cursor. The main registry may already
// contain later drops or ledger deletions: those take effect only when replay
// reaches their logs, or earlier logs would be folded without their indexes.
// Main-store declarations without a local version remain replay obligations;
// they cannot activate an index before its CreatedIndexLog is folded.
//
// Bucket-scoped entries (Index.Ledger == "") land in b.bucketIndexConfig
// and are reserved for audit-style indexes (see #436); they aren't tied
// to any ledger and don't trigger per-ledger backfill paths.
func (b *Builder) initIndexConfig(ctx context.Context) (err error) {
	snapshot := b.readStore.NewSnapshot()
	defer func() {
		if closeErr := snapshot.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("closing read-store index config snapshot: %w", closeErr))
		}
	}()

	if err := b.loadLedgerHistory(snapshot); err != nil {
		return fmt.Errorf("reading ledger history state: %w", err)
	}

	return b.initIndexConfigAfterHistory(ctx, snapshot)
}

// initIndexConfigAfterHistory uses the same read-store snapshot as the caller's
// history tracker and cursor. Main-store inventory is read separately and only
// supplies replay expectations and bucket-scoped declarations.
func (b *Builder) initIndexConfigAfterHistory(ctx context.Context, reader dal.PebbleReader) error {
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
	b.unresolvedIndexes = make(map[string]map[string]*commonpb.Index)
	b.pendingLedgerDeletes = make(map[string]struct{})

	handle, err := b.pebbleStore.NewReadHandle()
	if err != nil {
		return fmt.Errorf("creating read handle for index config: %w", err)
	}

	defer func() { _ = handle.Close() }()

	// Version states and ledger history must describe the same fold position.
	versionEntries, err := b.readStore.ReadAllIndexVersionStatesFrom(reader)
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
	// receives the new keyspace. toType comes from the version state's
	// PendingType, the cursor from the persisted BackfillCursor — the
	// rewrite resumes mid-rmap-scan instead of restarting from scratch.
	b.scheduleResumedRewrites()

	// Crash-recovery sweep for versions no state names (a version still
	// retained as Previous is named, and retirePrevious purges it on the
	// first loop wake): a re-retype that
	// bumped pending past an in-flight rewrite (the abandoned v_n is never
	// the local current and never the new pending, so its keyspace
	// lingers), or a snapshot install whose read-store delta dropped a
	// version entry. Cheap unconditional pass — DeleteRange on an empty
	// range is a tombstone no-op.
	if err := b.purgeOrphanVersions(); err != nil {
		b.logger.Errorf("Failed to purge orphan index versions: %v", err)
	}

	return nil
}

// seedLedgerIndexConfig retains every ledger alive at the fold cursor. A ledger
// already absent from main must still receive its intervening history before
// DeleteLedger removes its configuration. Track that required deletion so a
// corrupt history tracker cannot be accepted merely because it seeded a cfg.
// Ledgers only present in main are empty placeholders until CreatedLedger replay.
func (b *Builder) seedLedgerIndexConfig(ctx context.Context, handle *dal.ReadHandle) error {
	for ledger := range b.ledgerHistory {
		b.getOrCreateLedgerConfig(ledger)
		b.pendingLedgerDeletes[ledger] = struct{}{}
	}

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

		b.getOrCreateLedgerConfig(info.GetName())
		delete(b.pendingLedgerDeletes, info.GetName())
	}

	return nil
}

// restoreIndexConfigFromVersions restores indexes active at the fold cursor,
// independently of the newer main registry. Tombstones retain only their
// allocation high-water mark. Initial builds resume historical backfill;
// served versions with pending work are owned by scheduleResumedRewrites.
func (b *Builder) restoreIndexConfigFromVersions() error {
	for ledger, versions := range b.indexVersions {
		for canonical, version := range versions {
			if version.Tombstoned() {
				continue
			}
			if _, exists := b.historyStateFor(ledger); !exists {
				return historyReplayInvariantf("active IndexVersionState for %q/%s has no ledger history state", ledger, canonical)
			}
			id, err := indexes.ParseCanonical(canonical)
			if err != nil {
				return historyReplayInvariantf("invalid IndexVersionState identity for %q/%s: %v", ledger, canonical, err)
			}
			if !indexes.Supported(id) {
				return historyReplayInvariantf("unsupported IndexVersionState identity for %q/%s", ledger, canonical)
			}

			cfg := b.getOrCreateLedgerConfig(ledger)
			cfg.byCanonical[canonical] = &commonpb.Index{Ledger: ledger, Id: id}
			if version.CurrentVersion == 0 {
				b.scheduleBackfillForIndex(ledger, id)
			}
		}
	}

	return nil
}

// loadIndexRegistry restores active local indexes, then streams SubAttrIndex
// only to collect unresolved CreateIndex replay obligations and bucket-scoped
// entries. Registry absence cannot remove an index restored at the fold cursor.
func (b *Builder) loadIndexRegistry(handle *dal.ReadHandle) error {
	if err := b.restoreIndexConfigFromVersions(); err != nil {
		return err
	}

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

		_, ok := b.indexConfig[ledgerName]
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
		if state, exists := b.versionStateFor(ledgerName, canonical); exists && !state.Tombstoned() {
			continue
		}

		if b.unresolvedIndexes[ledgerName] == nil {
			if b.unresolvedIndexes == nil {
				b.unresolvedIndexes = make(map[string]map[string]*commonpb.Index)
			}
			b.unresolvedIndexes[ledgerName] = make(map[string]*commonpb.Index)
		}
		b.unresolvedIndexes[ledgerName][canonical] = idx
	}

	return iter.Err()
}

func (b *Builder) validateHistoryReplayState() error {
	for ledger := range b.pendingLedgerDeletes {
		return historyReplayInvariantf("ledger history state for inactive ledger %q survived catch-up without DeleteLedger replay", ledger)
	}

	for ledger, indexesByCanonical := range b.unresolvedIndexes {
		for canonical := range indexesByCanonical {
			return historyReplayInvariantf("index registry entry %q/%s was not resolved by CreatedIndex replay", ledger, canonical)
		}
	}

	for ledger := range b.indexConfig {
		if _, ok := b.historyStateFor(ledger); !ok {
			return historyReplayInvariantf("active ledger %q has no EMPTY/NON_EMPTY history state after catch-up", ledger)
		}
	}
	for ledger := range b.ledgerHistory {
		if _, ok := b.indexConfig[ledger]; !ok {
			return historyReplayInvariantf("ledger history state for inactive ledger %q survived catch-up", ledger)
		}
	}

	return nil
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
		task   *backfillTask
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

		removed = append(removed, stripped{ledger: task.ledger, key: key, entry: entry, task: task})
		delete(cfg.byCanonical, key)
	}

	return func() {
		for _, s := range removed {
			if !slices.Contains(b.backfillTasks, s.task) {
				continue
			}
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

// isLogDateIndex reports whether id is the log date builtin — the one index
// whose rows are per-log rather than per-entity, so its history is every log
// of the ledger.
func isLogDateIndex(id *commonpb.IndexID) bool {
	k, ok := id.GetKind().(*commonpb.IndexID_LogBuiltin)

	return ok && k.LogBuiltin == commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE
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
// The indexbuilder-owned durable ledger history tracker decides whether there
// is business history to replay, independently of proposal boundaries.
//
// Log replay idempotency: when the same CreatedIndexLog is folded again against
// an index this replica has already promoted to live, we skip the reset and
// backfill scheduling so the builder does not redo work that has already
// completed — and, more importantly, does not knock a live index back into
// ErrIndexBuilding.
func (b *Builder) handleCreatedIndexLog(ledgerName string, log *commonpb.CreatedIndexLog) error {
	id := log.GetId()
	if id == nil {
		return nil
	}

	historyState, historyExists := b.historyStateFor(ledgerName)
	if !historyExists {
		return historyReplayInvariantf("CreateIndex for %q has no EMPTY/NON_EMPTY history state", ledgerName)
	}
	if historyState != ledgerHistoryEmpty && historyState != ledgerHistoryNonEmpty {
		return historyReplayInvariantf("CreateIndex for %q has invalid history state %d", ledgerName, historyState)
	}

	canonical := indexes.Canonical(id)

	// The per-replica readiness signal is IndexVersionState.CurrentVersion
	// (EN-1323). If this replica has already promoted the index to live
	// (current != 0), a repeated CreatedIndexLog during replay must be a no-op.
	// Allocating a new pending version and rescheduling a backfill would
	// flip an already-live index back to ErrIndexBuilding. This mirrors the
	// loadIndexRegistry boot guard and covers both the EN-1771 EMPTY fast
	// path and the normal post-backfill live state. Fresh duplicate requests
	// are rejected by the FSM before emitting a log.
	if current, pending := b.versionFor(ledgerName, canonical); current != 0 {
		return nil
	} else if pending != 0 {
		// A build for this incarnation is already in flight: the running
		// backfill fills that pending version and will promote it. Allocating
		// a fresh number here would orphan the half-built keyspace while the
		// task's caught-up cursor promotes the never-filled replacement — a
		// permanently empty index. The duplicate create is a no-op.
		return nil
	}

	cfg, cfgExisted := b.indexConfig[ledgerName]
	if !cfgExisted {
		cfg = newLedgerIndexConfig()
		b.indexConfig[ledgerName] = cfg
	}
	priorIndex, hadPriorIndex := cfg.byCanonical[canonical]
	priorVersion, hadPriorVersion := b.versionStateFor(ledgerName, canonical)
	priorTasks := slices.Clone(b.backfillTasks)
	priorUnresolved := cloneIndexMap(b.unresolvedIndexes[ledgerName])
	_, hadUnresolvedLedger := b.unresolvedIndexes[ledgerName]
	b.recordFoldRollback(func() {
		switch {
		case !cfgExisted:
			delete(b.indexConfig, ledgerName)
		case hadPriorIndex:
			cfg.byCanonical[canonical] = priorIndex
		default:
			delete(cfg.byCanonical, canonical)
		}

		if hadPriorVersion {
			b.putVersionState(ledgerName, canonical, priorVersion)
		} else if inner := b.indexVersions[ledgerName]; inner != nil {
			delete(inner, canonical)
			if len(inner) == 0 {
				delete(b.indexVersions, ledgerName)
			}
		}
		b.backfillTasks = priorTasks
		if hadUnresolvedLedger {
			b.unresolvedIndexes[ledgerName] = priorUnresolved
		} else {
			delete(b.unresolvedIndexes, ledgerName)
		}
	})

	cfg.byCanonical[canonical] = &commonpb.Index{
		Id:                     id,
		ForwardEncodingVersion: 1,
	}
	if unresolved := b.unresolvedIndexes[ledgerName]; unresolved != nil {
		delete(unresolved, canonical)
		if len(unresolved) == 0 {
			delete(b.unresolvedIndexes, ledgerName)
		}
	}

	// A prior incarnation's tombstone holds the high-water version; a fresh
	// index allocates above it so no keyspace is ever written by two passes.
	if priorVersion.HighWater == ^uint32(0) {
		return fmt.Errorf("invariant: IndexVersionState high-water exhausted for %q/%s", ledgerName, canonical)
	}
	next := priorVersion.HighWater + 1

	// The first version binds to the declared type stamped into the log by
	// the FSM at mint time — the schema entry in force at exactly this log's
	// sequence. A local schema read here could not reproduce that: the
	// readable schema is batch-final at best and arbitrarily far ahead when
	// the log folds during a backfill or a rebuild replay.
	boundType, declared := log.GetBoundType(), log.GetBoundTypeDeclared()

	if b.wb == nil || b.wb.Batch() == nil {
		return historyReplayInvariantf("CreateIndex for %q encountered without an active readstore batch", ledgerName)
	}

	if historyState == ledgerHistoryEmpty {
		state := readstore.IndexVersionState{
			CurrentVersion:      next,
			PendingVersion:      0,
			HighWater:           next,
			CurrentType:         boundType,
			CurrentTypeDeclared: declared,
		}

		if err := b.readStore.WriteIndexVersionState(b.wb.Batch(), ledgerName, canonical, state); err != nil {
			return fmt.Errorf("persisting IndexVersionState on EMPTY CreateIndex: %w", err)
		}

		b.putVersionState(ledgerName, canonical, state)

		return nil
	}

	// First time this replica sees this index incarnation: target the freshly
	// allocated HighWater+1 version via the
	// backfill task. current stays at 0 until the backfill completes
	// and switches it via the atomic-switch path. Persisted in the
	// active batch so the per-replica readiness signal survives a
	// crash between CreateIndex apply and backfill completion — the
	// boot recovery would otherwise have to guess from cfg.byCanonical
	// alone, which loses the distinction between "fresh index" and
	// "stale READY index from a snapshot install".
	state := readstore.IndexVersionState{
		CurrentVersion:      0,
		PendingVersion:      next,
		HighWater:           next,
		PendingType:         boundType,
		PendingTypeDeclared: declared,
	}

	if err := b.readStore.WriteIndexVersionState(b.wb.Batch(), ledgerName, canonical, state); err != nil {
		return fmt.Errorf("persisting IndexVersionState on NON_EMPTY CreateIndex: %w", err)
	}

	b.putVersionState(ledgerName, canonical, state)

	b.scheduleBackfillForIndex(ledgerName, id)

	return nil
}

func cloneIndexMap(in map[string]*commonpb.Index) map[string]*commonpb.Index {
	if in == nil {
		return nil
	}
	out := make(map[string]*commonpb.Index, len(in))
	maps.Copy(out, in)

	return out
}

// handleDroppedIndexLog updates the index config cache when a DropIndex log
// is processed, purges the dropped metadata index's rows, and removes any
// active backfill / schema-rewrite task tied to the index — without that, a
// rewrite finishing post-drop would atomic-switch a keyspace live for an
// index that no longer exists.
func (b *Builder) handleDroppedIndexLog(kb *dal.KeyBuilder, ledger string, log *commonpb.DroppedIndexLog) error {
	id := log.GetId()
	if id == nil {
		return nil
	}

	cfg, cfgExisted := b.indexConfig[ledger]
	if !cfgExisted {
		cfg = newLedgerIndexConfig()
		b.indexConfig[ledger] = cfg
	}
	canonical := indexes.Canonical(id)
	priorIndex, hadPriorIndex := cfg.byCanonical[canonical]
	b.recordFoldRollback(func() {
		switch {
		case !cfgExisted:
			delete(b.indexConfig, ledger)
		case hadPriorIndex:
			cfg.byCanonical[canonical] = priorIndex
		default:
			delete(cfg.byCanonical, canonical)
		}
	})
	delete(cfg.byCanonical, canonical)
	if err := b.removeBackfillTask(ledger, id); err != nil {
		return err
	}

	// Tombstoned, never deleted: the record keeps the high-water version so a
	// re-created index cannot reuse a keyspace this incarnation wrote.
	// Queries read a tombstone exactly like an absent record (removed, not
	// building) — see PinnedVersionResolver.
	if err := b.tombstoneVersionState(ledger, canonical); err != nil {
		return err
	}

	if meta, ok := id.GetKind().(*commonpb.IndexID_Metadata); ok && meta.Metadata != nil {
		if err := b.removeSchemaRewriteTaskByField(ledger, meta.Metadata.GetTarget(), meta.Metadata.GetKey()); err != nil {
			return err
		}

		// The rows go with the index, in the same fold batch: all versions of
		// the forward index, exists index and reverse map by field-bounded
		// range tombstones. The purge keeps a re-created index's replay from
		// meeting this incarnation's permanent events; the version high-water
		// above also prevents keyspace reuse if corruption introduces an old row
		// outside the atomic cleanup path.
		ns := namespaceForTarget(meta.Metadata.GetTarget())
		if ns == "" {
			return nil
		}

		key := meta.Metadata.GetKey()

		batch := b.wb.Batch()
		if batch == nil {
			return fmt.Errorf(
				"invariant: no readstore write batch bound during DropIndex for ledger %q field %q",
				ledger, key)
		}

		if err := deleteReadStoreRange(batch, readstore.MetadataIndexFieldPrefix(kb, ledger, ns, key)); err != nil {
			return err
		}

		if err := deleteReadStoreRange(batch, readstore.EntityExistsFieldPrefix(kb, ledger, ns, key)); err != nil {
			return err
		}

		if err := b.purgeReverseMapForKey(kb, ledger, ns, key); err != nil {
			return err
		}
	}

	return nil
}
