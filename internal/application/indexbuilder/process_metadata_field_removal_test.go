package indexbuilder

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// countReverseMapRows scans the reverse map (0x03) for a namespace and returns
// how many rows carry the given metadata key, across every entity and version.
func countReverseMapRows(t *testing.T, b *Builder, ledger, ns, metaKey string) int {
	t.Helper()

	prefix := readstore.ReverseMapPrefix(b.kb, ledger, ns)
	upper := readstore.IncrementBytes(prefix)

	snap := b.readStore.NewSnapshot()
	defer func() { _ = snap.Close() }()

	iter, err := snap.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: upper})
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		_, mk, _, ok := parseReverseMapKey(iter.Key(), prefix, ns)
		if ok && mk == metaKey {
			count++
		}
	}

	return count
}

func savedAccountMetadata(account, key string, value int64) *commonpb.SavedMetadata {
	return &commonpb.SavedMetadata{
		Target: &commonpb.Target{
			Target: &commonpb.Target_Account{
				Account: &commonpb.TargetAccount{Addr: account},
			},
		},
		Metadata: map[string]*commonpb.MetadataValue{key: commonpb.NewIntValue(value)},
	}
}

func savedTransactionMetadata(txID uint64, key string, value int64) *commonpb.SavedMetadata {
	return &commonpb.SavedMetadata{
		Target: &commonpb.Target{
			Target: &commonpb.Target_TransactionId{TransactionId: txID},
		},
		Metadata: map[string]*commonpb.MetadataValue{key: commonpb.NewIntValue(value)},
	}
}

func deletedAccountMetadata(account, key string) *commonpb.DeletedMetadata {
	return &commonpb.DeletedMetadata{
		Target: &commonpb.Target{
			Target: &commonpb.Target_Account{
				Account: &commonpb.TargetAccount{Addr: account},
			},
		},
		Key: key,
	}
}

// TestHandleRemovedMetadataFieldType_PurgesReverseMap pins EN-1443: when an
// indexed metadata field is removed, every reverse-map row for that field must
// be purged — including PUTs written earlier in the *same* uncommitted builder
// batch, which the committed-only snapshot scan cannot see. The "same batch"
// and "mixed" cases fail before the read-your-writes overlay consult is added.
func TestHandleRemovedMetadataFieldType_PurgesReverseMap(t *testing.T) {
	t.Parallel()

	const (
		ledger     = "test"
		acct1      = "acct-1"
		acct2      = "acct-2"
		removedKey = "role"
		keepKey    = "team"
	)
	ns := readstore.NamespaceAccount

	newActiveCfg := func() *ledgerIndexConfig {
		cfg := newLedgerIndexConfig()
		for _, k := range []string{removedKey, keepKey} {
			id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, k)
			cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}
		}

		return cfg
	}

	removeRole := func(t *testing.T, b *Builder, cfg *ledgerIndexConfig) {
		t.Helper()

		removed := &commonpb.RemovedMetadataFieldTypeLog{
			DroppedIndex: indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, removedKey),
		}
		require.NoError(t, b.handleRemovedMetadataFieldType(b.kb, cfg, ledger, removed))
	}

	t.Run("committed then removed", func(t *testing.T) {
		t.Parallel()

		b := newTestBuilderWithStore(t)
		b.seedBatchSchema(t)
		cfg := newActiveCfg()

		// Batch 1: write role on acct-1 and commit.
		b.wb.Init(b.readStore.NewBatch())
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct1, removedKey, 1)))
		require.NoError(t, b.wb.Flush())
		require.Equal(t, 1, countReverseMapRows(t, b, ledger, ns, removedKey))

		// Batch 2: remove role and commit.
		b.wb.Init(b.readStore.NewBatch())
		removeRole(t, b, cfg)
		require.NoError(t, b.wb.Flush())

		require.Equal(t, 0, countReverseMapRows(t, b, ledger, ns, removedKey))
	})

	t.Run("same batch repro", func(t *testing.T) {
		t.Parallel()

		b := newTestBuilderWithStore(t)
		b.seedBatchSchema(t)
		cfg := newActiveCfg()

		// One batch: uncommitted role PUT, then removal, then flush.
		b.wb.Init(b.readStore.NewBatch())
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct1, removedKey, 1)))
		removeRole(t, b, cfg)
		require.NoError(t, b.wb.Flush())

		require.Equal(t, 0, countReverseMapRows(t, b, ledger, ns, removedKey))
	})

	t.Run("mixed committed and same batch", func(t *testing.T) {
		t.Parallel()

		b := newTestBuilderWithStore(t)
		b.seedBatchSchema(t)
		cfg := newActiveCfg()

		// Batch 1: committed role PUT on acct-1.
		b.wb.Init(b.readStore.NewBatch())
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct1, removedKey, 1)))
		require.NoError(t, b.wb.Flush())

		// Batch 2: uncommitted role PUT on acct-2, then removal, then flush.
		b.wb.Init(b.readStore.NewBatch())
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct2, removedKey, 2)))
		removeRole(t, b, cfg)
		require.NoError(t, b.wb.Flush())

		require.Equal(t, 0, countReverseMapRows(t, b, ledger, ns, removedKey))
	})

	t.Run("unrelated key survives", func(t *testing.T) {
		t.Parallel()

		b := newTestBuilderWithStore(t)
		b.seedBatchSchema(t)
		cfg := newActiveCfg()

		// One batch: role and team PUTs (uncommitted), then remove only role.
		b.wb.Init(b.readStore.NewBatch())
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct1, removedKey, 1)))
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct1, keepKey, 2)))
		removeRole(t, b, cfg)
		require.NoError(t, b.wb.Flush())

		require.Equal(t, 0, countReverseMapRows(t, b, ledger, ns, removedKey))
		require.Equal(t, 1, countReverseMapRows(t, b, ledger, ns, keepKey))
	})

	t.Run("same batch write then delete then remove", func(t *testing.T) {
		t.Parallel()

		b := newTestBuilderWithStore(t)
		b.seedBatchSchema(t)
		cfg := newActiveCfg()

		// One batch: write role, then delete it (overlay entry becomes nil),
		// then remove the field. Exercises the overlay's already-deleted
		// (value == nil) skip branch in purgeReverseMapForKey.
		b.wb.Init(b.readStore.NewBatch())
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct1, removedKey, 1)))
		require.NoError(t, b.indexDeletedMetadata(b.kb, cfg, ledger, deletedAccountMetadata(acct1, removedKey)))
		removeRole(t, b, cfg)
		require.NoError(t, b.wb.Flush())

		require.Equal(t, 0, countReverseMapRows(t, b, ledger, ns, removedKey))
	})

	t.Run("committed then same batch rewrite same key", func(t *testing.T) {
		t.Parallel()

		b := newTestBuilderWithStore(t)
		b.seedBatchSchema(t)
		cfg := newActiveCfg()

		// Batch 1: commit role on acct-1.
		b.wb.Init(b.readStore.NewBatch())
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct1, removedKey, 1)))
		require.NoError(t, b.wb.Flush())
		require.Equal(t, 1, countReverseMapRows(t, b, ledger, ns, removedKey))

		// Batch 2: rewrite the SAME reverse-map key in-flight (committed row +
		// non-nil overlay entry for the same key), then remove the field. The
		// committed scan nils the overlay entry before the overlay pass, so the
		// row is purged exactly once.
		b.wb.Init(b.readStore.NewBatch())
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct1, removedKey, 2)))
		removeRole(t, b, cfg)
		require.NoError(t, b.wb.Flush())

		require.Equal(t, 0, countReverseMapRows(t, b, ledger, ns, removedKey))
	})
}

// TestHandleRemovedMetadataFieldType_PurgesReverseMap_Transaction mirrors the
// account cases for the transaction namespace, whose reverse-map key layout
// (fixed 8-byte txID + version block) differs from the account null-scan. It
// exercises the same-batch overlay-purge path for transactions specifically.
func TestHandleRemovedMetadataFieldType_PurgesReverseMap_Transaction(t *testing.T) {
	t.Parallel()

	const (
		ledger     = "test"
		tx1        = uint64(1)
		tx2        = uint64(2)
		removedKey = "role"
	)
	ns := readstore.NamespaceTransaction

	newActiveCfg := func() *ledgerIndexConfig {
		cfg := newLedgerIndexConfig()
		id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, removedKey)
		cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

		return cfg
	}

	removeRole := func(t *testing.T, b *Builder, cfg *ledgerIndexConfig) {
		t.Helper()

		removed := &commonpb.RemovedMetadataFieldTypeLog{
			DroppedIndex: indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, removedKey),
		}
		require.NoError(t, b.handleRemovedMetadataFieldType(b.kb, cfg, ledger, removed))
	}

	t.Run("committed then removed", func(t *testing.T) {
		t.Parallel()

		b := newTestBuilderWithStore(t)
		b.seedBatchSchema(t)
		cfg := newActiveCfg()

		b.wb.Init(b.readStore.NewBatch())
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedTransactionMetadata(tx1, removedKey, 1)))
		require.NoError(t, b.wb.Flush())
		require.Equal(t, 1, countReverseMapRows(t, b, ledger, ns, removedKey))

		b.wb.Init(b.readStore.NewBatch())
		removeRole(t, b, cfg)
		require.NoError(t, b.wb.Flush())

		require.Equal(t, 0, countReverseMapRows(t, b, ledger, ns, removedKey))
	})

	t.Run("same batch repro", func(t *testing.T) {
		t.Parallel()

		b := newTestBuilderWithStore(t)
		b.seedBatchSchema(t)
		cfg := newActiveCfg()

		// One batch: uncommitted tx metadata PUT, then removal, then flush. The
		// committed-only snapshot cannot see the in-flight PUT — the overlay
		// pass is what purges it. Fails before EN-1443 (orphan row survives).
		b.wb.Init(b.readStore.NewBatch())
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedTransactionMetadata(tx1, removedKey, 1)))
		removeRole(t, b, cfg)
		require.NoError(t, b.wb.Flush())

		require.Equal(t, 0, countReverseMapRows(t, b, ledger, ns, removedKey))
	})

	t.Run("mixed committed and same batch", func(t *testing.T) {
		t.Parallel()

		b := newTestBuilderWithStore(t)
		b.seedBatchSchema(t)
		cfg := newActiveCfg()

		b.wb.Init(b.readStore.NewBatch())
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedTransactionMetadata(tx1, removedKey, 1)))
		require.NoError(t, b.wb.Flush())

		b.wb.Init(b.readStore.NewBatch())
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedTransactionMetadata(tx2, removedKey, 2)))
		removeRole(t, b, cfg)
		require.NoError(t, b.wb.Flush())

		require.Equal(t, 0, countReverseMapRows(t, b, ledger, ns, removedKey))
	})
}
