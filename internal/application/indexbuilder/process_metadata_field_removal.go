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
// Three stores are touched for a metadata index on (ns, key):
//   - 0x01 (forward): `MetadataIndexPrefix(ns, key)` is a clean prefix and is
//     wiped via a single DeleteRange.
//   - 0x02 (entity-exists): `EntityExistsKeyPrefix(ns, key)` covers both
//     null and non-null variants; another DeleteRange.
//   - 0x03 (reverse map): keys are `[ns:][entityID\x00][metaKey]` — metaKey
//     is a suffix, so we scan the per-namespace range and delete the keys
//     ending with this metaKey inline.
//
// The reverse-map scan reuses the in-flight indexed batch as snapshot would
// not see uncommitted writes, but the scan iterates from a fresh Pebble
// snapshot for consistency. The handler also strips the corresponding entry
// from the local ledgerIndexConfig cache so the live path stops considering
// the index as active immediately.
func (b *Builder) handleRemovedMetadataFieldType(
	kb *dal.KeyBuilder,
	cfg *ledgerIndexConfig,
	ledgerID uint32,
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

	// Forward inverted index.
	if err := deleteReadStoreRange(batch, readstore.MetadataIndexPrefix(kb, ledgerID, ns, key)); err != nil {
		return err
	}

	// Entity-existence index (covers both null and non-null variants).
	if err := deleteReadStoreRange(batch, readstore.EntityExistsKeyPrefix(kb, ledgerID, ns, key)); err != nil {
		return err
	}

	if err := b.purgeReverseMapForKey(kb, ledgerID, ns, key); err != nil {
		return err
	}

	// Mirror the change in the in-memory config so subsequent logs in this
	// same processing pass skip the now-defunct index without an extra
	// LedgerInfo reload.
	delete(cfg.byCanonical, indexes.Canonical(dropped))

	return nil
}

// purgeReverseMapForKey scans the reverse map for a (ns, key) pair and
// deletes every entry whose suffix matches the metadata key. The reverse
// map cannot be range-deleted directly because the metadata key sits as a
// suffix; the scan cost is bounded by the number of entities that had this
// metadata key.
func (b *Builder) purgeReverseMapForKey(kb *dal.KeyBuilder, ledgerID uint32, ns, key string) error {
	rmapPrefix := readstore.ReverseMapPrefix(kb, ledgerID, ns)
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

	keyBytes := []byte(key)

	for iter.First(); iter.Valid(); iter.Next() {
		k := iter.Key()

		// Reverse map keys end with the metadata key as a raw suffix (no
		// null terminator). Match by trailing equality.
		if !bytes.HasSuffix(k, keyBytes) {
			continue
		}

		// Defensive: ensure the suffix sits at the actual reverse-map slot,
		// i.e. immediately after the entity null terminator. Otherwise a
		// metadata key string that happens to share a suffix with another
		// would collide. We look for the rightmost `\x00` and check that
		// the metaKey starts right after it.
		nullIdx := bytes.LastIndexByte(k, 0)
		if nullIdx < 0 || nullIdx != len(k)-1-len(keyBytes) {
			continue
		}

		if err := b.wb.Batch().DeleteKey(append([]byte(nil), k...)); err != nil {
			return err
		}
	}

	return nil
}

func deleteReadStoreRange(batch *dal.Batch, start []byte) error {
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
