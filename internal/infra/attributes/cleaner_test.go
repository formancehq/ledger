package attributes

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func TestCleanupOldEntries_DeletesStaleEntries(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	keyA := []byte("ledger\x00alice\x00USD")
	keyB := []byte("ledger\x00bob\x00USD")

	// Write multiple entries per key at various raft indexes.
	batch := store.NewBatch()
	require.NoError(t, attrs.Volume.Set(batch, 5, keyA, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 10, keyA, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(200),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 15, keyA, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(300),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 3, keyB, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(500),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 8, keyB, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(600),
	}))
	require.NoError(t, batch.Commit())

	// Clean up with maxRaftIndex=20 (all entries are eligible).
	deleteOps, err := CleanupOldEntries(store, 20)
	require.NoError(t, err)
	require.Equal(t, 2, deleteOps, "should issue one delete-range per group with >1 entry")

	// Verify that the latest entry for each key survives.
	scanA, err := attrs.Volume.ScanEntries(store, keyA)
	require.NoError(t, err)
	require.Equal(t, 1, scanA.TotalEntries, "keyA should have only 1 entry left")
	require.Equal(t, uint64(15), scanA.LatestBaseIndex)

	scanB, err := attrs.Volume.ScanEntries(store, keyB)
	require.NoError(t, err)
	require.Equal(t, 1, scanB.TotalEntries, "keyB should have only 1 entry left")
	require.Equal(t, uint64(8), scanB.LatestBaseIndex)

	// Values should still be correct.
	valA, err := attrs.Volume.ComputeValue(store, ^uint64(0), keyA)
	require.NoError(t, err)
	require.Equal(t, int64(300), valA.GetInput().ToBigInt().Int64())

	valB, err := attrs.Volume.ComputeValue(store, ^uint64(0), keyB)
	require.NoError(t, err)
	require.Equal(t, int64(600), valB.GetInput().ToBigInt().Int64())
}

func TestCleanupOldEntries_SkipsEntriesAboveMaxRaftIndex(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	key := []byte("ledger\x00alice\x00USD")

	batch := store.NewBatch()
	require.NoError(t, attrs.Volume.Set(batch, 5, key, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 10, key, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(200),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 50, key, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(999),
	}))
	require.NoError(t, batch.Commit())

	// maxRaftIndex=20: entries at 5 and 10 are eligible, entry at 50 is not.
	// Latest eligible is 10, so 5 should be deleted. Entry at 50 should remain.
	deleteOps, err := CleanupOldEntries(store, 20)
	require.NoError(t, err)
	require.Equal(t, 1, deleteOps)

	scan, err := attrs.Volume.ScanEntries(store, key)
	require.NoError(t, err)
	require.Equal(t, 2, scan.TotalEntries, "entry at 10 and 50 should remain")
	require.Equal(t, uint64(50), scan.LatestBaseIndex)
}

func TestCleanupOldEntries_NoOpsOnSingleEntries(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	key := []byte("ledger\x00single\x00USD")

	batch := store.NewBatch()
	require.NoError(t, attrs.Volume.Set(batch, 5, key, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, batch.Commit())

	deleteOps, err := CleanupOldEntries(store, 100)
	require.NoError(t, err)
	require.Equal(t, 0, deleteOps, "single entry should not trigger cleanup")

	scan, err := attrs.Volume.ScanEntries(store, key)
	require.NoError(t, err)
	require.Equal(t, 1, scan.TotalEntries)
}

func TestCleanupOldEntries_EmptyStore(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)

	deleteOps, err := CleanupOldEntries(store, 100)
	require.NoError(t, err)
	require.Equal(t, 0, deleteOps)
}

func TestCleanupOldEntries_MultipleAttributeTypes(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	volKey := []byte("ledger\x00alice\x00USD")
	metaKey := []byte("ledger\x00alice\x00status")

	batch := store.NewBatch()
	// Volume: 2 entries
	require.NoError(t, attrs.Volume.Set(batch, 1, volKey, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 5, volKey, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(200),
	}))
	// Metadata: 2 entries
	require.NoError(t, attrs.Metadata.Set(batch, 2, metaKey, commonpb.NewStringValue("draft")))
	require.NoError(t, attrs.Metadata.Set(batch, 6, metaKey, commonpb.NewStringValue("active")))
	require.NoError(t, batch.Commit())

	deleteOps, err := CleanupOldEntries(store, 100)
	require.NoError(t, err)
	require.Equal(t, 2, deleteOps, "should clean up both volume and metadata groups")

	// Values should still be correct.
	vol, err := attrs.Volume.ComputeValue(store, ^uint64(0), volKey)
	require.NoError(t, err)
	require.Equal(t, int64(200), vol.GetInput().ToBigInt().Int64())

	meta, err := attrs.Metadata.ComputeValue(store, ^uint64(0), metaKey)
	require.NoError(t, err)
	require.Equal(t, "active", commonpb.MetadataValueToString(meta))
}

func TestCleanupOldEntries_Idempotent(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	key := []byte("ledger\x00alice\x00USD")

	batch := store.NewBatch()
	require.NoError(t, attrs.Volume.Set(batch, 1, key, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 5, key, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(200),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 10, key, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(300),
	}))
	require.NoError(t, batch.Commit())

	// First cleanup removes stale entries.
	deleteOps, err := CleanupOldEntries(store, 100)
	require.NoError(t, err)
	require.Equal(t, 1, deleteOps)

	// Second cleanup is a no-op — only one entry left per group.
	deleteOps, err = CleanupOldEntries(store, 100)
	require.NoError(t, err)
	require.Equal(t, 0, deleteOps)

	// Value unchanged.
	val, err := attrs.Volume.ComputeValue(store, ^uint64(0), key)
	require.NoError(t, err)
	require.Equal(t, int64(300), val.GetInput().ToBigInt().Int64())
}

func TestCleanupOldEntries_AllEntriesAboveMaxRaftIndex(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	key := []byte("ledger\x00future\x00USD")

	batch := store.NewBatch()
	require.NoError(t, attrs.Volume.Set(batch, 50, key, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 60, key, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(200),
	}))
	require.NoError(t, batch.Commit())

	// maxRaftIndex=10: no entries are eligible.
	deleteOps, err := CleanupOldEntries(store, 10)
	require.NoError(t, err)
	require.Equal(t, 0, deleteOps)

	// Both entries must survive.
	scan, err := attrs.Volume.ScanEntries(store, key)
	require.NoError(t, err)
	require.Equal(t, 2, scan.TotalEntries)
}

func TestCleanupOldEntries_IncrementalMaxRaftIndex(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	key := []byte("ledger\x00incremental\x00USD")

	batch := store.NewBatch()
	for i := uint64(1); i <= 5; i++ {
		require.NoError(t, attrs.Volume.Set(batch, i*10, key, &raftcmdpb.VolumePair{
			Input: commonpb.NewUint256FromUint64(i * 100),
		}))
	}
	require.NoError(t, batch.Commit()) // entries at 10, 20, 30, 40, 50

	// Cleanup with maxRaftIndex=25: eligible entries are 10, 20.
	// Latest eligible is 20, so 10 is deleted. Remaining: 20, 30, 40, 50.
	deleteOps, err := CleanupOldEntries(store, 25)
	require.NoError(t, err)
	require.Equal(t, 1, deleteOps)

	scan, err := attrs.Volume.ScanEntries(store, key)
	require.NoError(t, err)
	require.Equal(t, 4, scan.TotalEntries)

	// Cleanup with maxRaftIndex=45: eligible entries are 20, 30, 40.
	// Latest eligible is 40, so 20 and 30 are deleted. Remaining: 40, 50.
	deleteOps, err = CleanupOldEntries(store, 45)
	require.NoError(t, err)
	require.Equal(t, 1, deleteOps)

	scan, err = attrs.Volume.ScanEntries(store, key)
	require.NoError(t, err)
	require.Equal(t, 2, scan.TotalEntries)

	// Value at maxIndex=45 should be 400 (entry at index 40).
	val, err := attrs.Volume.ComputeValue(store, 45, key)
	require.NoError(t, err)
	require.Equal(t, int64(400), val.GetInput().ToBigInt().Int64())

	// Value at unbounded should be 500 (entry at index 50).
	val, err = attrs.Volume.ComputeValue(store, ^uint64(0), key)
	require.NoError(t, err)
	require.Equal(t, int64(500), val.GetInput().ToBigInt().Int64())
}

func TestCleanupOldEntries_SameCanonicalKeyDifferentAttrTypes(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	// Same canonical key but different attribute types → different groups.
	canonicalKey := []byte("ledger\x00alice\x00USD")

	batch := store.NewBatch()
	// Volume: 3 entries
	require.NoError(t, attrs.Volume.Set(batch, 1, canonicalKey, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(10),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 2, canonicalKey, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(20),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 3, canonicalKey, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(30),
	}))
	// Metadata: 2 entries on the same canonical key
	require.NoError(t, attrs.Metadata.Set(batch, 1, canonicalKey, commonpb.NewStringValue("draft")))
	require.NoError(t, attrs.Metadata.Set(batch, 4, canonicalKey, commonpb.NewStringValue("active")))
	require.NoError(t, batch.Commit())

	deleteOps, err := CleanupOldEntries(store, 100)
	require.NoError(t, err)
	require.Equal(t, 2, deleteOps, "volume group and metadata group cleaned independently")

	// Volume: only entry at 3 remains.
	scanVol, err := attrs.Volume.ScanEntries(store, canonicalKey)
	require.NoError(t, err)
	require.Equal(t, 1, scanVol.TotalEntries)
	require.Equal(t, uint64(3), scanVol.LatestBaseIndex)

	// Metadata: only entry at 4 remains.
	scanMeta, err := attrs.Metadata.ScanEntries(store, canonicalKey)
	require.NoError(t, err)
	require.Equal(t, 1, scanMeta.TotalEntries)
	require.Equal(t, uint64(4), scanMeta.LatestBaseIndex)
}

func TestCleanupOldEntries_AllSixAttributeTypes(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	ledgerKey := []byte("myledger")
	volumeKey := []byte("myledger\x00alice\x00USD")
	metadataKey := []byte("myledger\x00alice\x00status")
	idempotencyKey := []byte("idem-key-1")
	referenceKey := []byte("\x00\x00\x00\x01ref-1")
	boundaryKey := []byte("myledger")

	batch := store.NewBatch()
	// Write 2 entries per attribute type.
	require.NoError(t, attrs.Volume.Set(batch, 1, volumeKey, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 2, volumeKey, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(200),
	}))
	require.NoError(t, attrs.Metadata.Set(batch, 1, metadataKey, commonpb.NewStringValue("old")))
	require.NoError(t, attrs.Metadata.Set(batch, 2, metadataKey, commonpb.NewStringValue("new")))
	require.NoError(t, attrs.IdempotencyKeys.Set(batch, 1, idempotencyKey, &commonpb.IdempotencyKeyValue{LogSequence: 10}))
	require.NoError(t, attrs.IdempotencyKeys.Set(batch, 2, idempotencyKey, &commonpb.IdempotencyKeyValue{LogSequence: 20}))
	require.NoError(t, attrs.References.Set(batch, 1, referenceKey, &commonpb.TransactionReferenceValue{TransactionId: 42}))
	require.NoError(t, attrs.References.Set(batch, 2, referenceKey, &commonpb.TransactionReferenceValue{TransactionId: 99}))
	require.NoError(t, attrs.Ledger.Set(batch, 1, ledgerKey, &commonpb.LedgerInfo{Name: "old"}))
	require.NoError(t, attrs.Ledger.Set(batch, 2, ledgerKey, &commonpb.LedgerInfo{Name: "myledger"}))
	require.NoError(t, attrs.Boundary.Set(batch, 1, boundaryKey, &raftcmdpb.LedgerBoundaries{NextTransactionId: 1}))
	require.NoError(t, attrs.Boundary.Set(batch, 2, boundaryKey, &raftcmdpb.LedgerBoundaries{NextTransactionId: 10}))
	require.NoError(t, batch.Commit())

	deleteOps, err := CleanupOldEntries(store, 100)
	require.NoError(t, err)
	require.Equal(t, 6, deleteOps, "one delete-range per attribute type group")

	// Verify latest values survive for all types.
	vol, err := attrs.Volume.ComputeValue(store, ^uint64(0), volumeKey)
	require.NoError(t, err)
	require.Equal(t, int64(200), vol.GetInput().ToBigInt().Int64())

	meta, err := attrs.Metadata.ComputeValue(store, ^uint64(0), metadataKey)
	require.NoError(t, err)
	require.Equal(t, "new", commonpb.MetadataValueToString(meta))

	idem, err := attrs.IdempotencyKeys.ComputeValue(store, ^uint64(0), idempotencyKey)
	require.NoError(t, err)
	require.Equal(t, uint64(20), idem.GetLogSequence())

	ref, err := attrs.References.ComputeValue(store, ^uint64(0), referenceKey)
	require.NoError(t, err)
	require.Equal(t, uint64(99), ref.GetTransactionId())

	ledger, err := attrs.Ledger.ComputeValue(store, ^uint64(0), ledgerKey)
	require.NoError(t, err)
	require.Equal(t, "myledger", ledger.GetName())

	boundary, err := attrs.Boundary.ComputeValue(store, ^uint64(0), boundaryKey)
	require.NoError(t, err)
	require.Equal(t, uint64(10), boundary.GetNextTransactionId())
}

func TestCleanupOldEntries_ManyEntriesPerGroup(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	key := []byte("ledger\x00heavy\x00USD")

	batch := store.NewBatch()
	for i := uint64(1); i <= 100; i++ {
		require.NoError(t, attrs.Volume.Set(batch, i, key, &raftcmdpb.VolumePair{
			Input: commonpb.NewUint256FromUint64(i),
		}))
	}
	require.NoError(t, batch.Commit())

	deleteOps, err := CleanupOldEntries(store, 100)
	require.NoError(t, err)
	require.Equal(t, 1, deleteOps)

	scan, err := attrs.Volume.ScanEntries(store, key)
	require.NoError(t, err)
	require.Equal(t, 1, scan.TotalEntries, "only the latest entry should survive")
	require.Equal(t, uint64(100), scan.LatestBaseIndex)

	val, err := attrs.Volume.ComputeValue(store, ^uint64(0), key)
	require.NoError(t, err)
	require.Equal(t, int64(100), val.GetInput().ToBigInt().Int64())
}

func TestCleanupOldEntries_MixedSingleAndMultiEntryGroups(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	batch := store.NewBatch()

	// 5 canonical keys: some with 1 entry, some with multiple.
	for i := range 5 {
		key := fmt.Appendf(nil, "ledger\x00account%d\x00USD", i)
		entryCount := i + 1 // account0: 1, account1: 2, ..., account4: 5

		for j := 1; j <= entryCount; j++ {
			require.NoError(t, attrs.Volume.Set(batch, uint64(j), key, &raftcmdpb.VolumePair{
				Input: commonpb.NewUint256FromUint64(uint64(j * 100)),
			}))
		}
	}

	require.NoError(t, batch.Commit())

	deleteOps, err := CleanupOldEntries(store, 100)
	require.NoError(t, err)
	// Groups with >1 entry: account1(2), account2(3), account3(4), account4(5) = 4 groups.
	require.Equal(t, 4, deleteOps)

	// Every group should have exactly 1 entry after cleanup.
	for i := range 5 {
		key := fmt.Appendf(nil, "ledger\x00account%d\x00USD", i)

		scan, err := attrs.Volume.ScanEntries(store, key)
		require.NoError(t, err)
		require.Equal(t, 1, scan.TotalEntries, "account%d should have 1 entry", i)
		require.Equal(t, uint64(i+1), scan.LatestBaseIndex)
	}
}

func TestCleanupOldEntries_MaxRaftIndexZero(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	key := []byte("ledger\x00zero\x00USD")

	batch := store.NewBatch()
	require.NoError(t, attrs.Volume.Set(batch, 1, key, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 5, key, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(200),
	}))
	require.NoError(t, batch.Commit())

	// maxRaftIndex=0: no entries have raftIndex <= 0, so nothing eligible.
	deleteOps, err := CleanupOldEntries(store, 0)
	require.NoError(t, err)
	require.Equal(t, 0, deleteOps)

	scan, err := attrs.Volume.ScanEntries(store, key)
	require.NoError(t, err)
	require.Equal(t, 2, scan.TotalEntries, "all entries should survive")
}

func TestCleanupOldEntries_OnlyOneEligibleEntry(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	key := []byte("ledger\x00oneeligible\x00USD")

	batch := store.NewBatch()
	// Only one entry at or below maxRaftIndex, the rest are above.
	require.NoError(t, attrs.Volume.Set(batch, 5, key, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 50, key, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(200),
	}))
	require.NoError(t, attrs.Volume.Set(batch, 60, key, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(300),
	}))
	require.NoError(t, batch.Commit())

	// maxRaftIndex=10: only entry at 5 is eligible → count=1, no delete.
	deleteOps, err := CleanupOldEntries(store, 10)
	require.NoError(t, err)
	require.Equal(t, 0, deleteOps, "single eligible entry should not trigger cleanup")

	scan, err := attrs.Volume.ScanEntries(store, key)
	require.NoError(t, err)
	require.Equal(t, 3, scan.TotalEntries, "all entries should survive")
}
