package indexbuilder

import (
	"bytes"

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
//     suffix but it's preceded by a fixed-width version block, so it
//     can't be range-deleted by (ns, key). purgeReverseMapForKey scans
//     the per-namespace range and deletes every row matching this
//     metaKey regardless of version.
//
// Unlike the forward/entity-exists range deletes — whose range tombstone
// gets a higher sequence number than earlier same-batch SETs and so
// shadows uncommitted rows too — the reverse-map point-delete scan reads
// a committed-only Pebble snapshot. It therefore also consults the
// batch's read-your-writes overlay to purge reverse-map PUTs written
// earlier in this same uncommitted batch (see purgeReverseMapForKey).
// The handler also strips the
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

// purgeReverseMapForKey deletes every reverse-map entry whose metadata key
// field matches (ns, key), across every per-replica forward-encoding version.
// The reverse map can't be range-deleted by (ns, key) because the rmap key
// shape is [entity\x00][version:4B BE][metaKey] — metaKey sits after a
// fixed-width version block, not directly after the entity null.
//
// Two sources are purged:
//   - Committed rows: scanned from a fresh Pebble snapshot (committed data
//     only). Scan cost is bounded by the number of (entity, version) tuples
//     that ever held this metadata key.
//   - In-flight rows: reverse-map PUTs written earlier in this same
//     uncommitted batch are invisible to the snapshot, so we also consult the
//     batch's read-your-writes overlay. Without this, a SavedMetadata on the
//     indexed field in the same batch as the removal would commit an orphaned
//     reverse-map row for a field that no longer exists (EN-1443).
func (b *Builder) purgeReverseMapForKey(kb *dal.KeyBuilder, ledgerName string, ns, key string) error {
	rmapPrefix := readstore.ReverseMapPrefix(kb, ledgerName, ns)
	upper := readstore.IncrementBytes(rmapPrefix)

	// Deduplicate so a key present in both committed state and the in-flight
	// overlay is deleted only once.
	seen := make(map[string]struct{})
	deleteMatch := func(k []byte) error {
		if _, done := seen[string(k)]; done {
			return nil
		}
		seen[string(k)] = struct{}{}

		return b.wb.DeleteReverseMapKey(k)
	}

	// Committed rows.
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

		// iter.Key() is only valid until the next iterator move — clone it
		// before it outlives this iteration in the batch/dedup set.
		if err := deleteMatch(append([]byte(nil), k...)); err != nil {
			return err
		}
	}

	// In-flight rows from the same uncommitted batch.
	var pending [][]byte
	b.wb.RangeReverseMapOverlay(func(reverseKey []byte, value []byte) {
		if value == nil {
			return // already deleted in this batch
		}
		if !bytes.HasPrefix(reverseKey, rmapPrefix) {
			return // different ledger / namespace
		}

		_, mk, _, parsed := parseReverseMapKey(reverseKey, rmapPrefix, ns)
		if !parsed || mk != key {
			return
		}

		pending = append(pending, reverseKey)
	})

	for _, k := range pending {
		if err := deleteMatch(k); err != nil {
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
