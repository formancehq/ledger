package indexbuilder

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// handleRemovedMetadataFieldType honours the cascade declared by the
// processor (`RemovedMetadataFieldTypeLog.dropped_index`): when an indexed
// metadata field is removed from the schema, its read-store entries must
// be purged so subsequent queries do not surface stale results.
//
// Three stores are touched for a metadata index on (ns, key) across every
// per-replica version that ever existed:
//   - 0x01 (forward): MetadataIndexFieldPrefix(ns, key) covers every v_n
//     and is wiped via a single DeleteRange.
//   - 0x02 (entity-exists): EntityExistsFieldPrefix(ns, key) likewise
//     covers every v_n (both null and non-null variants).
//   - 0x03 (reverse map): ReverseMapFieldPrefix(ns, key) covers every
//     version and entity for the field.
//
// Each range tombstone gets a higher sequence number than earlier same-batch
// SETs and therefore shadows committed and uncommitted rows alike. The handler also strips the
// corresponding entry from the local ledgerIndexConfig cache so the
// live path stops considering the index as active immediately, and
// drops the per-replica IndexVersionState entry so the boot orphan
// sweep doesn't try to GC versions of a field that no longer exists.
func (b *Builder) handleRemovedMetadataFieldType(
	kb *dal.KeyBuilder,
	cfg *ledgerIndexConfig,
	ledgerName string,
	log *commonpb.RemovedMetadataFieldTypeLog,
) error {
	dropped := log.GetDroppedIndex()
	if dropped == nil {
		return nil
	}

	meta, ok := dropped.GetKind().(*commonpb.IndexID_Metadata)
	if !ok {
		return nil
	}

	ns := namespaceForTarget(meta.Metadata.GetTarget())
	if ns == "" {
		return nil
	}

	key := meta.Metadata.GetKey()
	batch := b.wb.Batch()

	if batch == nil {
		// handleRemovedMetadataFieldType is only ever reached through
		// indexPayload, whose callers Init the WriteBatch before dispatching
		// log payloads. A nil batch here means the call site is broken —
		// surface it loudly per CLAUDE.md invariant #7, exactly as
		// bumpPendingVersion does. Returning nil would report success while
		// skipping all three limbs at once: the forward-index range delete,
		// the entity-exists range delete and the reverse-map range tombstone.
		return fmt.Errorf(
			"invariant: no readstore write batch bound during RemovedMetadataFieldType for ledger %q field %q",
			ledgerName, key)
	}

	// Forward inverted index — every version in one range delete.
	if err := deleteReadStoreRange(batch, readstore.MetadataIndexFieldPrefix(kb, ledgerName, ns, key)); err != nil {
		return err
	}

	// Entity-existence index — every version, both null and non-null.
	if err := deleteReadStoreRange(batch, readstore.EntityExistsFieldPrefix(kb, ledgerName, ns, key)); err != nil {
		return err
	}

	if err := b.purgeReverseMapForKey(kb, ledgerName, ns, key); err != nil {
		return err
	}

	// Mirror the change in the in-memory config so subsequent logs in this
	// same processing pass skip the now-defunct index without an extra
	// LedgerInfo reload.
	delete(cfg.byCanonical, indexes.Canonical(dropped))

	// Drop any in-flight schema-rewrite task for this (ledger, target, key).
	// Without this, a rewrite started by a prior SetMetadataFieldType would
	// outlive the index it was rewriting, and the builder would keep
	// driving a rewrite for an index that no longer exists.
	b.removeSchemaRewriteTaskByField(ledgerName, meta.Metadata.GetTarget(), key)

	// Same hazard on the backfill side: an initial CreateIndex backfill
	// for this metadata index could still be running. processBackfill
	// uses a one-index cfg, so it would repopulate the entries we just
	// purged and then loop forever. removeBackfillTask drops the task
	// and deletes its persisted progress.
	b.removeBackfillTask(ledgerName, dropped)

	// Tombstone the per-replica IndexVersionState: the rows are purged above,
	// but the high-water version must survive so a re-declared field's fresh
	// index cannot reuse a version number — if this purge ever misses a row,
	// the miss stays isolated in a keyspace no future pass writes into.
	canonical := indexes.Canonical(dropped)
	if err := b.tombstoneVersionState(ledgerName, canonical); err != nil {
		return err
	}

	return nil
}

// purgeReverseMapForKey deletes every reverse-map entry for (ns, key), across
// all versions and entities, with one field-bounded range tombstone. Because
// the delete is queued after earlier writes in the same batch it also covers
// those uncommitted rows (EN-1443) without separately walking the overlay.
func (b *Builder) purgeReverseMapForKey(kb *dal.KeyBuilder, ledgerName string, ns, key string) error {
	rmapPrefix := readstore.ReverseMapFieldPrefix(kb, ledgerName, ns, key)
	upper := readstore.IncrementBytes(rmapPrefix)

	return b.wb.DeleteReverseMapRange(rmapPrefix, upper)
}

func deleteReadStoreRange(batch *dal.WriteSession, start []byte) error {
	end := readstore.IncrementBytes(start)

	return batch.DeleteRangeNoSync(start, end)
}

func namespaceForTarget(t commonpb.TargetType) string {
	switch t {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		return readstore.NamespaceAccount
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		return readstore.NamespaceTransaction
	default:
		return ""
	}
}
