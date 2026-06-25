package indexbuilder

import (
	"github.com/cockroachdb/pebble/v2"

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
//   - 0x03 (reverse map): keys are
//     [ns:][entityID\x00][version:4B BE][metaKey] — metaKey is the
//     suffix but it's preceded by a fixed-width version block, so we
//     scan the per-namespace range and use extractMetadataKeyFromReverseMap
//     (which already accounts for the version block) to delete every
//     row matching this metaKey regardless of version.
//
// The reverse-map scan reuses the in-flight indexed batch as snapshot
// would not see uncommitted writes, but the scan iterates from a fresh
// Pebble snapshot for consistency. The handler also strips the
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
		return nil
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
	// outlive the index it was rewriting, and the builder would retry
	// IndexReady proposals forever against an index that no longer exists.
	b.removeSchemaRewriteTaskByField(ledgerName, meta.Metadata.GetTarget(), key)

	// Same hazard on the backfill side: an initial CreateIndex backfill
	// for this metadata index could still be running. processBackfill
	// uses a one-index cfg, so it would repopulate the entries we just
	// purged and then loop forever. removeBackfillTask drops the task
	// and deletes its persisted progress.
	b.removeBackfillTask(ledgerName, dropped)

	// Drop the per-replica IndexVersionState for the removed field
	// so the boot orphan sweep doesn't try to GC versions of a field
	// that no longer exists in the schema.
	canonical := indexes.Canonical(dropped)
	b.dropVersionState(ledgerName, canonical)

	if err := b.readStore.DeleteIndexVersionState(ledgerName, canonical); err != nil {
		b.logger.WithFields(map[string]any{
			"ledger":    ledgerName,
			"canonical": canonical,
			"error":     err,
		}).Errorf("Deleting IndexVersionState on field removal")
	}

	return nil
}

// purgeReverseMapForKey scans the reverse map for a (ns, key) pair and
// deletes every entry whose metadata key field matches, across every
// per-replica forward-encoding version. The reverse map can't be
// range-deleted by (ns, key) because the rmap key shape is
// [entity\x00][version:4B BE][metaKey] — metaKey sits after a
// fixed-width version block, not directly after the entity null. The
// scan cost is bounded by the number of (entity, version) tuples that
// ever held this metadata key.
func (b *Builder) purgeReverseMapForKey(kb *dal.KeyBuilder, ledgerName string, ns, key string) error {
	rmapPrefix := readstore.ReverseMapPrefix(kb, ledgerName, ns)
	upper := readstore.IncrementBytes(rmapPrefix)

	// Use a Pebble snapshot so the scan sees committed data only; the
	// in-flight batch writes from the same processing pass would otherwise
	// surface as ghost entries.
	snap := b.readStore.NewSnapshot()
	defer func() { _ = snap.Close() }()

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: rmapPrefix,
		UpperBound: upper,
	})
	if err != nil {
		return err
	}

	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()

		_, mk, _, parsed := parseReverseMapKey(k, rmapPrefix, ns)
		if !parsed || mk != key {
			continue
		}

		if err := b.wb.Batch().DeleteKey(append([]byte(nil), k...)); err != nil {
			return err
		}
	}

	return nil
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
