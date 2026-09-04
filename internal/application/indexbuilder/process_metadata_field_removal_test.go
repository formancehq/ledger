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
		rk, err := readstore.ParseReverseMapKey(iter.Key())
		require.NoError(t, err)
		require.Equal(t, ledger, rk.Ledger)
		require.Equal(t, ns, rk.Namespace)

		if rk.MetadataKey == metaKey {
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
// be purged — including PUTs written earlier in the same uncommitted builder
// batch. The field-bounded range tombstone covers both committed and in-flight
// rows.
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
		b.wb.SetEventSequence(1)
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
		b.wb.SetEventSequence(1)
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
		b.wb.SetEventSequence(1)
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct1, removedKey, 1)))
		require.NoError(t, b.wb.Flush())

		// Batch 2: uncommitted role PUT on acct-2, then removal, then flush.
		b.wb.Init(b.readStore.NewBatch())
		b.wb.SetEventSequence(2)
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
		b.wb.SetEventSequence(1)
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct1, removedKey, 1)))
		b.wb.SetEventSequence(2)
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
		// then remove the field. The range tombstone remains idempotent when
		// the exact overlay row is already deleted.
		b.wb.Init(b.readStore.NewBatch())
		b.wb.SetEventSequence(1)
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct1, removedKey, 1)))
		b.wb.SetEventSequence(2)
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
		b.wb.SetEventSequence(1)
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct1, removedKey, 1)))
		require.NoError(t, b.wb.Flush())
		require.Equal(t, 1, countReverseMapRows(t, b, ledger, ns, removedKey))

		// Batch 2: rewrite the SAME reverse-map key in-flight (committed row +
		// non-nil overlay entry for the same key), then remove the field. The
		// range tombstone supersedes both the committed row and its later exact
		// overlay replacement.
		b.wb.Init(b.readStore.NewBatch())
		b.wb.SetEventSequence(2)
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedAccountMetadata(acct1, removedKey, 2)))
		removeRole(t, b, cfg)
		require.NoError(t, b.wb.Flush())

		require.Equal(t, 0, countReverseMapRows(t, b, ledger, ns, removedKey))
	})
}

// TestHandleRemovedMetadataFieldType_PurgesReverseMap_Transaction mirrors the
// account cases for the transaction namespace, whose reverse-map key layout
// ends in a fixed 8-byte txID rather than a variable-width account address. It
// exercises same-batch range-purge behavior for transactions specifically.
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
		b.wb.SetEventSequence(1)
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
		// range tombstone must cover the in-flight PUT too.
		b.wb.Init(b.readStore.NewBatch())
		b.wb.SetEventSequence(1)
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
		b.wb.SetEventSequence(1)
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedTransactionMetadata(tx1, removedKey, 1)))
		require.NoError(t, b.wb.Flush())

		b.wb.Init(b.readStore.NewBatch())
		b.wb.SetEventSequence(2)
		require.NoError(t, b.indexSavedMetadata(b.kb, cfg, ledger, savedTransactionMetadata(tx2, removedKey, 2)))
		removeRole(t, b, cfg)
		require.NoError(t, b.wb.Flush())

		require.Equal(t, 0, countReverseMapRows(t, b, ledger, ns, removedKey))
	})
}

func TestPurgeReverseMapForKeyIsFieldBoundedAcrossVersions(t *testing.T) {
	t.Parallel()

	const (
		ledger      = "test"
		otherLedger = "other-ledger"
		removedKey  = "status"
		keepKey     = "team"
	)
	encoded := readstore.EncodeMetadataValue(nil, commonpb.NewStringValue("open"))

	tests := []struct {
		name       string
		ns         string
		entityID   []byte
		key        func(*Builder, string, string, uint32) []byte
		otherNSKey func(*Builder) []byte
	}{
		{
			name:     "account",
			ns:       readstore.NamespaceAccount,
			entityID: []byte("users:1"),
			key: func(b *Builder, ledgerName, field string, version uint32) []byte {
				return cloneBytes(readstore.AccountReverseMapKeyV(b.kb, ledgerName, "users:1", field, version))
			},
			otherNSKey: func(b *Builder) []byte {
				return cloneBytes(readstore.TransactionReverseMapKeyV(b.kb, ledger, 1, removedKey, 1))
			},
		},
		{
			name:     "transaction",
			ns:       readstore.NamespaceTransaction,
			entityID: []byte{0, 0, 0, 0, 0, 0, 0, 1},
			key: func(b *Builder, ledgerName, field string, version uint32) []byte {
				return cloneBytes(readstore.TransactionReverseMapKeyV(b.kb, ledgerName, 1, field, version))
			},
			otherNSKey: func(b *Builder) []byte {
				return cloneBytes(readstore.AccountReverseMapKeyV(b.kb, ledger, "users:1", removedKey, 1))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newTestBuilderWithStore(t)
			removedV1 := tt.key(b, ledger, removedKey, 1)
			removedV3 := tt.key(b, ledger, removedKey, 3)
			removedInFlightV2 := tt.key(b, ledger, removedKey, 2)
			keepField := tt.key(b, ledger, keepKey, 1)
			keepLedger := tt.key(b, otherLedger, removedKey, 1)
			keepNamespace := tt.otherNSKey(b)

			seed := b.readStore.NewBatch()
			for _, key := range [][]byte{removedV1, removedV3, keepField, keepLedger, keepNamespace} {
				require.NoError(t, seed.SetBytes(key, encoded))
			}
			require.NoError(t, seed.Commit())

			b.wb.Init(b.readStore.NewBatch())
			b.wb.SetEventSequence(2)
			require.NoError(t, b.wb.ReplaceMetadataIndexV(
				b.kb, removedInFlightV2,
				ledger, tt.ns, removedKey, 2,
				encoded, nil, tt.entityID,
			))
			require.NoError(t, b.purgeReverseMapForKey(b.kb, ledger, tt.ns, removedKey))
			require.NoError(t, b.wb.Flush())

			for _, key := range [][]byte{removedV1, removedInFlightV2, removedV3} {
				assertReadStoreMissing(t, b, key)
			}
			for _, key := range [][]byte{keepField, keepLedger, keepNamespace} {
				assertReadStoreValue(t, b, key, encoded)
			}
		})
	}
}

// TestHandleRemovedMetadataFieldType_NoBatchFailsLoudly pins invariant #7 on the
// purge entry point. Every path into handleRemovedMetadataFieldType runs
// initBatch first, so an unbound batch is impossible by design — but the old
// `return nil` reported success while skipping all three limbs at once: the
// forward-index range delete, the entity-exists range delete and the reverse-map
// point deletes. Neither 0x01 nor 0x02 has a detector of its own, so a refactor
// that dropped an initBatch would have produced a fully unindexed removal with no
// signal anywhere. It now mirrors bumpPendingVersion and fails loudly.
func TestHandleRemovedMetadataFieldType_NoBatchFailsLoudly(t *testing.T) {
	t.Parallel()

	const (
		ledger     = "L1"
		removedKey = "role"
	)

	b := newTestBuilderWithStore(t)

	id := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, removedKey)
	cfg := newLedgerIndexConfig()
	cfg.byCanonical[indexes.Canonical(id)] = &commonpb.Index{Id: id}

	// No initBatch: b.wb holds no session, so Batch() returns nil.
	b.wb.Reset()
	require.Nil(t, b.wb.Batch(), "precondition: the write batch must be unbound")

	err := b.handleRemovedMetadataFieldType(b.kb, cfg, ledger, &commonpb.RemovedMetadataFieldTypeLog{
		DroppedIndex: id,
	})

	require.Error(t, err, "a missing write batch must not be reported as a completed removal")
	require.Contains(t, err.Error(), "invariant:")
	require.Contains(t, err.Error(), ledger)
	require.Contains(t, err.Error(), removedKey)
}
