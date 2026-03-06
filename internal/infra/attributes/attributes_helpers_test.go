package attributes

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

func TestAttrTypeFromKey(t *testing.T) {
	t.Parallel()

	t.Run("valid key", func(t *testing.T) {
		t.Parallel()
		// Build a key: [0xF1][canonical][attrType][raftIndex 8B][entryType 1B]
		// SuffixLen = 10, so key must be > 1 + 10 = 11 bytes
		key := make([]byte, 20)
		key[0] = dal.KeyPrefixAttributes
		key[len(key)-SuffixLen] = 0x42 // attr type
		attrType, ok := AttrTypeFromKey(key)
		require.True(t, ok)
		require.Equal(t, byte(0x42), attrType)
	})

	t.Run("too short key", func(t *testing.T) {
		t.Parallel()

		key := make([]byte, 5) // less than 1+SuffixLen
		_, ok := AttrTypeFromKey(key)
		require.False(t, ok)
	})

	t.Run("exactly boundary length", func(t *testing.T) {
		t.Parallel()

		key := make([]byte, 1+SuffixLen) // exactly 11 bytes
		_, ok := AttrTypeFromKey(key)
		require.False(t, ok) // must be strictly greater than 1+SuffixLen
	})
}

func TestCanonicalKeyFromPebbleKey(t *testing.T) {
	t.Parallel()

	t.Run("valid key extracts canonical", func(t *testing.T) {
		t.Parallel()

		canonical := []byte("ledger:account:USD")
		// Build: [prefix(1)][canonical][suffix(SuffixLen)]
		key := make([]byte, 1+len(canonical)+SuffixLen)
		key[0] = dal.KeyPrefixAttributes
		copy(key[1:], canonical)

		result := CanonicalKeyFromPebbleKey(key)
		require.Equal(t, canonical, result)
	})

	t.Run("too short key returns nil", func(t *testing.T) {
		t.Parallel()

		key := make([]byte, 5)
		result := CanonicalKeyFromPebbleKey(key)
		require.Nil(t, result)
	})

	t.Run("exactly boundary returns nil", func(t *testing.T) {
		t.Parallel()

		key := make([]byte, 1+SuffixLen)
		result := CanonicalKeyFromPebbleKey(key)
		require.Nil(t, result)
	})
}

func TestIncrementBytes(t *testing.T) {
	t.Parallel()

	t.Run("simple increment", func(t *testing.T) {
		t.Parallel()

		result := IncrementBytes([]byte{0x00, 0x01})
		require.Equal(t, []byte{0x00, 0x02}, result)
	})

	t.Run("carry propagation", func(t *testing.T) {
		t.Parallel()

		result := IncrementBytes([]byte{0x00, 0xFF})
		require.Equal(t, []byte{0x01, 0x00}, result)
	})

	t.Run("all 0xFF overflow returns nil", func(t *testing.T) {
		t.Parallel()

		result := IncrementBytes([]byte{0xFF, 0xFF, 0xFF})
		require.Nil(t, result)
	})

	t.Run("single byte increment", func(t *testing.T) {
		t.Parallel()

		result := IncrementBytes([]byte{0x42})
		require.Equal(t, []byte{0x43}, result)
	})

	t.Run("single byte 0xFF overflow", func(t *testing.T) {
		t.Parallel()

		result := IncrementBytes([]byte{0xFF})
		require.Nil(t, result)
	})

	t.Run("empty input", func(t *testing.T) {
		t.Parallel()

		result := IncrementBytes([]byte{})
		require.Nil(t, result)
	})

	t.Run("does not mutate original", func(t *testing.T) {
		t.Parallel()

		original := []byte{0x01, 0x02}
		_ = IncrementBytes(original)
		require.Equal(t, []byte{0x01, 0x02}, original)
	})
}

func TestDeleteOldest(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("ledger\x00oldest-test\x00USD")

	// Write entries at indexes 5, 10, 15, 20
	batch := store.NewBatch()
	require.NoError(t, attrs.Volume.SetBase(batch, 5, testKey, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 10, testKey, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(200),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 15, testKey, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(300),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 20, testKey, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(400),
	}))
	require.NoError(t, batch.Commit())

	// Verify all entries exist
	scan, err := attrs.Volume.ScanEntries(store, testKey)
	require.NoError(t, err)
	require.Equal(t, 4, scan.TotalEntries)

	// Delete oldest entries (strictly before index 15)
	batch = store.NewBatch()
	require.NoError(t, attrs.Volume.DeleteOldest(batch, 15, testKey))
	require.NoError(t, batch.Commit())

	// Verify entries at 5 and 10 are removed, entries at 15 and 20 remain
	scan, err = attrs.Volume.ScanEntries(store, testKey)
	require.NoError(t, err)
	require.Equal(t, 2, scan.TotalEntries)
	require.True(t, scan.HasDiff)
	require.Equal(t, uint64(20), scan.LatestDiffIndex)

	// Computed value should still work (latest diff = 400)
	result, err := attrs.Volume.ComputeValue(store, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Equal(t, int64(400), result.GetInputKnown().ToBigInt().Int64())
}

func TestAccumulator(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	// Write volume data for two canonical keys
	keyA := []byte("ledger\x00alice\x00USD")
	keyB := []byte("ledger\x00bob\x00USD")

	batch := store.NewBatch()
	require.NoError(t, attrs.Volume.SetBase(batch, 1, keyA, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(1000),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 2, keyA, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(500),
	}))
	require.NoError(t, attrs.Volume.SetBase(batch, 1, keyB, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(2000),
	}))
	require.NoError(t, batch.Commit())

	// Use ForEachInPrefix to iterate and verify results
	var results []ComputedEntry[*raftcmdpb.VolumePair]

	err := attrs.Volume.ForEachInPrefix(store, ^uint64(0), []byte("ledger\x00"), func(entry ComputedEntry[*raftcmdpb.VolumePair]) error {
		results = append(results, entry)

		return nil
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
}

func TestAccumulatorFeedAndFlush(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	// Write metadata entries for two keys
	keyA := []byte("ledger\x00alice\x00field1")
	keyB := []byte("ledger\x00bob\x00field1")

	batch := store.NewBatch()
	require.NoError(t, attrs.Metadata.Set(batch, 1, keyA, commonpb.NewStringValue("alice-val")))
	require.NoError(t, attrs.Metadata.Set(batch, 1, keyB, commonpb.NewStringValue("bob-val")))
	require.NoError(t, batch.Commit())

	// Use ComputeAllForPrefix to get all entries under the ledger prefix
	entries, err := attrs.Metadata.ComputeAllForPrefix(store, ^uint64(0), []byte("ledger\x00"))
	require.NoError(t, err)
	require.Len(t, entries, 2)

	// Verify computed values
	foundAlice := false
	foundBob := false

	for _, entry := range entries {
		canonical := string(entry.CanonicalKey)
		if canonical == string(keyA) {
			foundAlice = true

			require.Equal(t, "alice-val", commonpb.MetadataValueToString(entry.Value))
		}

		if canonical == string(keyB) {
			foundBob = true

			require.Equal(t, "bob-val", commonpb.MetadataValueToString(entry.Value))
		}
	}

	require.True(t, foundAlice)
	require.True(t, foundBob)
}

func TestAccumulatorEmptyFlush(t *testing.T) {
	t.Parallel()

	attrs := New()
	acc := attrs.Volume.NewAccumulator()

	// Flush with no data fed should return empty
	results := acc.Flush()
	require.Empty(t, results)
}

func TestAccumulatorPrefix(t *testing.T) {
	t.Parallel()

	attrs := New()
	acc := attrs.Volume.NewAccumulator()
	require.Equal(t, dal.AttributePrefixVolume, acc.Prefix())

	metaAcc := attrs.Metadata.NewAccumulator()
	require.Equal(t, dal.AttributePrefixMetadata, metaAcc.Prefix())
}

func TestComputeValueMaxIndex(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("ledger\x00maxidx\x00USD")

	batch := store.NewBatch()
	require.NoError(t, attrs.Volume.SetBase(batch, 5, testKey, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 10, testKey, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(200),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 20, testKey, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(300),
	}))
	require.NoError(t, batch.Commit())

	// Query at max index 15 should only see base at 5 and diff at 10
	result, err := attrs.Volume.ComputeValue(store, 15, testKey)
	require.NoError(t, err)
	require.Equal(t, int64(300), result.GetInputKnown().ToBigInt().Int64())
	// Actually: base=100, diff at 10=200 -> 100+200=300. Correct.

	// Query at max index 7 should only see base at 5
	result, err = attrs.Volume.ComputeValue(store, 7, testKey)
	require.NoError(t, err)
	require.Equal(t, int64(100), result.GetInputKnown().ToBigInt().Int64())

	// Query at max index 3 should see nothing (base is at index 5)
	result, err = attrs.Volume.ComputeValue(store, 3, testKey)
	require.NoError(t, err)
	// No base or diffs found - returns zero
	require.Equal(t, int64(0), result.GetInputKnown().ToBigInt().Int64())
}

func TestForEachInPrefixMaxIndex(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	keyA := []byte("test\x00a\x00USD")
	keyB := []byte("test\x00b\x00USD")

	batch := store.NewBatch()
	require.NoError(t, attrs.Volume.SetBase(batch, 5, keyA, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 50, keyA, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(500),
	}))
	require.NoError(t, attrs.Volume.SetBase(batch, 3, keyB, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(200),
	}))
	require.NoError(t, batch.Commit())

	// ForEachInPrefix with maxIndex=10 should filter out diff at index 50
	var results []ComputedEntry[*raftcmdpb.VolumePair]

	err := attrs.Volume.ForEachInPrefix(store, 10, []byte("test\x00"), func(entry ComputedEntry[*raftcmdpb.VolumePair]) error {
		results = append(results, entry)

		return nil
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
}

func TestIdempotencyKeysAttribute(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("idem-key-1")

	// Set a value, then overwrite with a later Set — latest wins
	batch := store.NewBatch()
	require.NoError(t, attrs.IdempotencyKeys.Set(batch, 1, testKey, &commonpb.IdempotencyKeyValue{
		LogSequence: 10,
	}))
	require.NoError(t, batch.Commit())

	result, err := attrs.IdempotencyKeys.ComputeValue(store, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Equal(t, uint64(10), result.GetLogSequence())

	// Overwrite with a later Set
	batch = store.NewBatch()
	require.NoError(t, attrs.IdempotencyKeys.Set(batch, 2, testKey, &commonpb.IdempotencyKeyValue{
		LogSequence: 20,
	}))
	require.NoError(t, batch.Commit())

	result, err = attrs.IdempotencyKeys.ComputeValue(store, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Equal(t, uint64(20), result.GetLogSequence())
}

func TestReferenceAttribute(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("\x00\x00\x00\x01ref-1")

	// Set a value, then overwrite with a later Set — latest wins
	batch := store.NewBatch()
	require.NoError(t, attrs.References.Set(batch, 1, testKey, &commonpb.TransactionReferenceValue{
		TransactionId: 42,
	}))
	require.NoError(t, batch.Commit())

	result, err := attrs.References.ComputeValue(store, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Equal(t, uint64(42), result.GetTransactionId())

	// Overwrite with a later Set
	batch = store.NewBatch()
	require.NoError(t, attrs.References.Set(batch, 2, testKey, &commonpb.TransactionReferenceValue{
		TransactionId: 99,
	}))
	require.NoError(t, batch.Commit())

	result, err = attrs.References.ComputeValue(store, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Equal(t, uint64(99), result.GetTransactionId())
}

func TestModuleNew(t *testing.T) {
	t.Parallel()

	attrs := New()
	require.NotNil(t, attrs.Volume)
	require.NotNil(t, attrs.Metadata)
	require.NotNil(t, attrs.IdempotencyKeys)
	require.NotNil(t, attrs.References)
	require.NotNil(t, attrs.Ledger)
	require.NotNil(t, attrs.Boundary)
}

func TestLedgerAttribute(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("my-ledger")

	// Set a value
	batch := store.NewBatch()
	require.NoError(t, attrs.Ledger.Set(batch, 1, testKey, &commonpb.LedgerInfo{
		Name: "my-ledger",
	}))
	require.NoError(t, batch.Commit())

	result, err := attrs.Ledger.ComputeValue(store, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Equal(t, "my-ledger", result.GetName())

	// Overwrite with a later Set — latest wins
	batch = store.NewBatch()
	require.NoError(t, attrs.Ledger.Set(batch, 2, testKey, &commonpb.LedgerInfo{
		Name: "my-ledger-renamed",
	}))
	require.NoError(t, batch.Commit())

	result, err = attrs.Ledger.ComputeValue(store, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Equal(t, "my-ledger-renamed", result.GetName())
}

func TestBoundaryAttribute(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("boundary-ledger")

	batch := store.NewBatch()
	require.NoError(t, attrs.Boundary.Set(batch, 1, testKey, &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 10,
		NextLogId:         20,
	}))
	require.NoError(t, batch.Commit())

	result, err := attrs.Boundary.ComputeValue(store, ^uint64(0), testKey)
	require.NoError(t, err)
	require.Equal(t, uint64(10), result.GetNextTransactionId())
	require.Equal(t, uint64(20), result.GetNextLogId())
}

// makePebbleKey builds a pebble attribute key for testing:
// [KeyPrefixAttributes(1B)][canonical][attrType(1B)][raftIndex(8B BE)][entryType(1B)].
func makePebbleKey(canonical []byte, attrType byte, raftIndex uint64, entryType byte) []byte {
	key := make([]byte, 1+len(canonical)+1+8+1)
	key[0] = dal.KeyPrefixAttributes
	copy(key[1:], canonical)
	key[1+len(canonical)] = attrType
	binary.BigEndian.PutUint64(key[1+len(canonical)+1:], raftIndex)
	key[len(key)-1] = entryType

	return key
}

func TestAccumulatorFeedPublicMethod(t *testing.T) {
	t.Parallel()

	attrs := New()
	acc := attrs.Metadata.NewAccumulator()

	canonical := []byte("ledger\x00account\x00field")

	// Feed a base entry
	baseValue := commonpb.NewStringValue("base-val")
	baseBytes, err := proto.Marshal(baseValue)
	require.NoError(t, err)

	pebbleKey := makePebbleKey(canonical, dal.AttributePrefixMetadata, 1, 0) // entryType=0 is base

	matched, feedErr := acc.Feed(pebbleKey, baseBytes)
	require.NoError(t, feedErr)
	require.True(t, matched)

	// Feed a second base entry for the same canonical key (overwrites first)
	overwriteValue := commonpb.NewStringValue("overwrite-val")
	overwriteBytes, err := proto.Marshal(overwriteValue)
	require.NoError(t, err)

	overwriteKey := makePebbleKey(canonical, dal.AttributePrefixMetadata, 5, 0) // entryType=0 is base

	matched, feedErr = acc.Feed(overwriteKey, overwriteBytes)
	require.NoError(t, feedErr)
	require.True(t, matched)

	// Feed a different canonical key to trigger boundary computation of the previous key
	canonical2 := []byte("ledger\x00account2\x00field")
	base2Value := commonpb.NewStringValue("base2-val")
	base2Bytes, err := proto.Marshal(base2Value)
	require.NoError(t, err)

	pebbleKey2 := makePebbleKey(canonical2, dal.AttributePrefixMetadata, 1, 0)

	matched, feedErr = acc.Feed(pebbleKey2, base2Bytes)
	require.NoError(t, feedErr)
	require.True(t, matched)

	// Flush to get all results
	results := acc.Flush()
	require.Len(t, results, 2) // Both canonical keys
}

func TestAccumulatorFeedNonMatchingPrefix(t *testing.T) {
	t.Parallel()

	attrs := New()
	acc := attrs.Metadata.NewAccumulator()

	// Build a key with a different attribute prefix (volume instead of metadata)
	canonical := []byte("ledger\x00account\x00USD")
	baseValue := &raftcmdpb.VolumePair{InputKnown: commonpb.NewUint256FromUint64(100)}
	baseBytes, err := proto.Marshal(baseValue)
	require.NoError(t, err)

	pebbleKey := makePebbleKey(canonical, dal.AttributePrefixVolume, 1, 0)

	matched, feedErr := acc.Feed(pebbleKey, baseBytes)
	require.NoError(t, feedErr)
	require.False(t, matched, "volume key should not match metadata accumulator")
}

func TestAccumulatorFeedTooShortKey(t *testing.T) {
	t.Parallel()

	attrs := New()
	acc := attrs.Metadata.NewAccumulator()

	// Key shorter than 1+SuffixLen
	shortKey := make([]byte, 5)
	matched, feedErr := acc.Feed(shortKey, []byte("value"))
	require.NoError(t, feedErr)
	require.False(t, matched)
}

func TestCompactAllForBackup(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	keyA := []byte("ledger\x00alice\x00USD")
	keyB := []byte("ledger\x00bob\x00USD")

	batch := store.NewBatch()
	require.NoError(t, attrs.Volume.SetBase(batch, 1, keyA, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 5, keyA, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(200),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 10, keyA, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(300),
	}))
	require.NoError(t, attrs.Volume.SetBase(batch, 1, keyB, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(500),
	}))
	// Also add metadata entries
	require.NoError(t, attrs.Metadata.Set(batch, 1, []byte("ledger\x00alice\x00field"), commonpb.NewStringValue("val")))
	require.NoError(t, batch.Commit())

	// Run compaction - CompactAllForBackup creates its own batch and Attributes internally
	err := CompactAllForBackup(store)
	require.NoError(t, err)

	// After compaction, the values should still be readable using a fresh Attributes
	freshAttrs := New()
	result, err := freshAttrs.Volume.ComputeValue(store, ^uint64(0), keyA)
	require.NoError(t, err)
	require.NotNil(t, result)

	result, err = freshAttrs.Volume.ComputeValue(store, ^uint64(0), keyB)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// TestCompactAllForBackupAllTypes verifies that CompactAllForBackup correctly
// compacts all 6 attribute types to index 0 with exact value verification,
// and resets lastAppliedIndex.
func TestCompactAllForBackupAllTypes(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	ledgerKey := []byte("myledger")
	volumeKey := []byte("myledger\x00alice\x00USD")
	metadataKey := []byte("myledger\x00alice\x00status")
	idempotencyKey := []byte("idem-key-123")
	referenceKey := []byte("\x00\x00\x00\x01ref-abc")
	boundaryKey := []byte("myledger")

	// Write data at high raft indexes
	batch := store.NewBatch()
	require.NoError(t, attrs.Volume.SetBase(batch, 100, volumeKey, &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(1000),
		OutputKnown: commonpb.NewUint256FromUint64(500),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 200, volumeKey, &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(300),
		OutputKnown: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Metadata.Set(batch, 50, metadataKey, commonpb.NewStringValue("active")))
	require.NoError(t, attrs.IdempotencyKeys.Set(batch, 75, idempotencyKey, &commonpb.IdempotencyKeyValue{
		LogSequence: 42,
	}))
	require.NoError(t, attrs.References.Set(batch, 80, referenceKey, &commonpb.TransactionReferenceValue{
		TransactionId: 99,
	}))
	require.NoError(t, attrs.Ledger.Set(batch, 60, ledgerKey, &commonpb.LedgerInfo{
		Name: "myledger",
	}))
	require.NoError(t, attrs.Boundary.Set(batch, 90, boundaryKey, &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 10,
		NextLogId:         5,
	}))

	// Set lastAppliedIndex to a high value
	idxBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(idxBuf, 200)
	require.NoError(t, batch.SetBytes([]byte{dal.KeyPrefixLastAppliedIndex}, idxBuf))
	require.NoError(t, batch.Commit())

	// Verify data is NOT visible at index 0 before compaction
	vol, err := attrs.Volume.ComputeValue(store, 0, volumeKey)
	require.NoError(t, err)
	require.Equal(t, int64(0), vol.GetInputKnown().ToBigInt().Int64(), "volume should not be visible at index 0 before compaction")

	// Run compaction
	require.NoError(t, CompactAllForBackup(store))

	// Verify all types are readable at index 0 with correct values
	freshAttrs := New()

	vol, err = freshAttrs.Volume.ComputeValue(store, 0, volumeKey)
	require.NoError(t, err)
	require.NotNil(t, vol)
	require.Equal(t, int64(1300), vol.GetInputKnown().ToBigInt().Int64(), "volume input: base 1000 + diff 300")
	require.Equal(t, int64(600), vol.GetOutputKnown().ToBigInt().Int64(), "volume output: base 500 + diff 100")

	meta, err := freshAttrs.Metadata.ComputeValue(store, 0, metadataKey)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, "active", commonpb.MetadataValueToString(meta))

	idem, err := freshAttrs.IdempotencyKeys.ComputeValue(store, 0, idempotencyKey)
	require.NoError(t, err)
	require.NotNil(t, idem)
	require.Equal(t, uint64(42), idem.GetLogSequence())

	ref, err := freshAttrs.References.ComputeValue(store, 0, referenceKey)
	require.NoError(t, err)
	require.NotNil(t, ref)
	require.Equal(t, uint64(99), ref.GetTransactionId())

	ledger, err := freshAttrs.Ledger.ComputeValue(store, 0, ledgerKey)
	require.NoError(t, err)
	require.NotNil(t, ledger)
	require.Equal(t, "myledger", ledger.GetName())

	boundary, err := freshAttrs.Boundary.ComputeValue(store, 0, boundaryKey)
	require.NoError(t, err)
	require.NotNil(t, boundary)
	require.Equal(t, uint64(10), boundary.GetNextTransactionId())
	require.Equal(t, uint64(5), boundary.GetNextLogId())

	// Verify lastAppliedIndex was reset to 0
	lastIdx, err := readLastAppliedIndex(store)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastIdx, "lastAppliedIndex should be 0 after compaction")
}

// TestCompactAllForBackupMultiKeyPerType verifies that single-pass compaction
// correctly handles multiple canonical keys per attribute type. Each key should
// be compacted independently with the correct accumulated value.
func TestCompactAllForBackupMultiKeyPerType(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	keyAlice := []byte("ledger\x00alice\x00USD")
	keyBob := []byte("ledger\x00bob\x00USD")
	keyCharlie := []byte("ledger\x00charlie\x00USD")

	batch := store.NewBatch()
	// Alice: base=100, diff=+50 → 150
	require.NoError(t, attrs.Volume.SetBase(batch, 1, keyAlice, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 5, keyAlice, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(50),
	}))
	// Bob: base only = 200
	require.NoError(t, attrs.Volume.SetBase(batch, 3, keyBob, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(200),
	}))
	// Charlie: two diffs, no explicit base → diff=300 (last diff wins)
	require.NoError(t, attrs.Volume.AddDiff(batch, 2, keyCharlie, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(100),
	}))
	require.NoError(t, attrs.Volume.AddDiff(batch, 7, keyCharlie, &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(300),
	}))
	require.NoError(t, batch.Commit())

	require.NoError(t, CompactAllForBackup(store))

	freshAttrs := New()

	alice, err := freshAttrs.Volume.ComputeValue(store, 0, keyAlice)
	require.NoError(t, err)
	require.NotNil(t, alice)
	require.Equal(t, int64(150), alice.GetInputKnown().ToBigInt().Int64(), "alice: base 100 + diff 50")

	bob, err := freshAttrs.Volume.ComputeValue(store, 0, keyBob)
	require.NoError(t, err)
	require.NotNil(t, bob)
	require.Equal(t, int64(200), bob.GetInputKnown().ToBigInt().Int64(), "bob: base only 200")

	charlie, err := freshAttrs.Volume.ComputeValue(store, 0, keyCharlie)
	require.NoError(t, err)
	require.NotNil(t, charlie)
	require.Equal(t, int64(300), charlie.GetInputKnown().ToBigInt().Int64(), "charlie: last diff 300")
}

// TestCompactAllForBackupEmpty verifies that compacting an empty store succeeds
// without errors and resets lastAppliedIndex.
func TestCompactAllForBackupEmpty(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)

	// Set a non-zero lastAppliedIndex
	batch := store.NewBatch()
	idxBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(idxBuf, 42)
	require.NoError(t, batch.SetBytes([]byte{dal.KeyPrefixLastAppliedIndex}, idxBuf))
	require.NoError(t, batch.Commit())

	require.NoError(t, CompactAllForBackup(store))

	lastIdx, err := readLastAppliedIndex(store)
	require.NoError(t, err)
	require.Equal(t, uint64(0), lastIdx, "lastAppliedIndex should be 0 even on empty store")
}
