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

func errOnly(_ []byte, err error) error { return err }

func TestAttrTypeFromKey(t *testing.T) {
	t.Parallel()

	t.Run("valid key", func(t *testing.T) {
		t.Parallel()
		// Build a key: [0xF1][attrType][canonical...]
		key := make([]byte, 20)
		key[0] = dal.KeyPrefixAttributes
		key[1] = 0x42 // attr type at fixed position 1
		attrType, ok := AttrTypeFromKey(key)
		require.True(t, ok)
		require.Equal(t, byte(0x42), attrType)
	})

	t.Run("too short key", func(t *testing.T) {
		t.Parallel()

		key := make([]byte, 1) // less than 1+SuffixLen
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
		// Build: [prefix(1)][attrType(1)][canonical]
		key := make([]byte, 2+len(canonical))
		key[0] = dal.KeyPrefixAttributes
		key[1] = 0x42 // attr type
		copy(key[2:], canonical)

		result := CanonicalKeyFromPebbleKey(key)
		require.Equal(t, canonical, result)
	})

	t.Run("too short key returns nil", func(t *testing.T) {
		t.Parallel()

		key := make([]byte, 1)
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

func TestAccumulator(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	// Write volume data for two canonical keys
	keyA := []byte("ledger\x00alice\x00USD")
	keyB := []byte("ledger\x00bob\x00USD")

	batch := store.NewBatch()
	require.NoError(t, errOnly(attrs.Volume.Set(batch, keyA, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(1000),
	})))
	require.NoError(t, errOnly(attrs.Volume.Set(batch, keyA, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(500),
	})))
	require.NoError(t, errOnly(attrs.Volume.Set(batch, keyB, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(2000),
	})))
	require.NoError(t, batch.Commit())

	// Use StreamingIter to iterate and verify results
	var results []ComputedEntry[*raftcmdpb.VolumePair]

	iter, err := attrs.Volume.NewStreamingIter(store, []byte("ledger\x00"))
	require.NoError(t, err)

	for iter.Next() {
		results = append(results, iter.Entry())
	}

	require.NoError(t, iter.Close())
	require.NoError(t, iter.Err())
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
	require.NoError(t, errOnly(attrs.Metadata.Set(batch, keyA, commonpb.NewStringValue("alice-val"))))
	require.NoError(t, errOnly(attrs.Metadata.Set(batch, keyB, commonpb.NewStringValue("bob-val"))))
	require.NoError(t, batch.Commit())

	// Use ComputeAllForPrefix to get all entries under the ledger prefix
	entries, err := attrs.Metadata.ComputeAllForPrefix(store, []byte("ledger\x00"))
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
	require.Equal(t, dal.AttributeCodeVolume, acc.Prefix())

	metaAcc := attrs.Metadata.NewAccumulator()
	require.Equal(t, dal.AttributeCodeMetadata, metaAcc.Prefix())
}

func TestGetReturnsLatestSet(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("ledger\x00maxidx\x00USD")

	// Multiple Sets overwrite in place — last Set wins
	batch := store.NewBatch()
	require.NoError(t, errOnly(attrs.Volume.Set(batch, testKey, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(100),
	})))
	require.NoError(t, errOnly(attrs.Volume.Set(batch, testKey, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(200),
	})))
	require.NoError(t, errOnly(attrs.Volume.Set(batch, testKey, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(300),
	})))
	require.NoError(t, batch.Commit())

	// Get returns the latest Set value = 300
	result, err := attrs.Volume.Get(store, testKey)
	require.NoError(t, err)
	require.Equal(t, int64(300), result.GetInput().ToBigInt().Int64())
}

func TestComputeAllForPrefixMaxIndex(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	keyA := []byte("test\x00a\x00USD")
	keyB := []byte("test\x00b\x00USD")

	// Each Set overwrites in place, so only the last Set per key survives.
	batch := store.NewBatch()
	require.NoError(t, errOnly(attrs.Volume.Set(batch, keyA, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(500),
	})))
	require.NoError(t, errOnly(attrs.Volume.Set(batch, keyB, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(200),
	})))
	require.NoError(t, batch.Commit())

	// StreamingIter scans all entries and groups by canonical key.
	var results []ComputedEntry[*raftcmdpb.VolumePair]

	iter, err := attrs.Volume.NewStreamingIter(store, []byte("test\x00"))
	require.NoError(t, err)

	for iter.Next() {
		results = append(results, iter.Entry())
	}

	require.NoError(t, iter.Close())
	require.NoError(t, iter.Err())
	require.Len(t, results, 2)

	// keyA has value 500, keyB has value 200.
	for _, r := range results {
		if string(r.CanonicalKey) == string(keyA) {
			require.Equal(t, int64(500), r.Value.GetInput().ToBigInt().Int64())
		} else {
			require.Equal(t, int64(200), r.Value.GetInput().ToBigInt().Int64())
		}
	}
}

func TestReferenceAttribute(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("\x00\x00\x00\x01ref-1")

	// Set a value, then overwrite with a later Set — latest wins
	batch := store.NewBatch()
	require.NoError(t, errOnly(attrs.References.Set(batch, testKey, &commonpb.TransactionReferenceValue{
		TransactionId: 42,
	})))
	require.NoError(t, batch.Commit())

	result, err := attrs.References.Get(store, testKey)
	require.NoError(t, err)
	require.Equal(t, uint64(42), result.GetTransactionId())

	// Overwrite with a later Set
	batch = store.NewBatch()
	require.NoError(t, errOnly(attrs.References.Set(batch, testKey, &commonpb.TransactionReferenceValue{
		TransactionId: 99,
	})))
	require.NoError(t, batch.Commit())

	result, err = attrs.References.Get(store, testKey)
	require.NoError(t, err)
	require.Equal(t, uint64(99), result.GetTransactionId())
}

func TestModuleNew(t *testing.T) {
	t.Parallel()

	attrs := New()
	require.NotNil(t, attrs.Volume)
	require.NotNil(t, attrs.Metadata)
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
	require.NoError(t, errOnly(attrs.Ledger.Set(batch, testKey, &commonpb.LedgerInfo{
		Name: "my-ledger",
	})))
	require.NoError(t, batch.Commit())

	result, err := attrs.Ledger.Get(store, testKey)
	require.NoError(t, err)
	require.Equal(t, "my-ledger", result.GetName())

	// Overwrite with a later Set — latest wins
	batch = store.NewBatch()
	require.NoError(t, errOnly(attrs.Ledger.Set(batch, testKey, &commonpb.LedgerInfo{
		Name: "my-ledger-renamed",
	})))
	require.NoError(t, batch.Commit())

	result, err = attrs.Ledger.Get(store, testKey)
	require.NoError(t, err)
	require.Equal(t, "my-ledger-renamed", result.GetName())
}

func TestBoundaryAttribute(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("boundary-ledger")

	batch := store.NewBatch()
	require.NoError(t, errOnly(attrs.Boundary.Set(batch, testKey, &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 10,
		NextLogId:         20,
	})))
	require.NoError(t, batch.Commit())

	result, err := attrs.Boundary.Get(store, testKey)
	require.NoError(t, err)
	require.Equal(t, uint64(10), result.GetNextTransactionId())
	require.Equal(t, uint64(20), result.GetNextLogId())
}

// makePebbleKey builds a pebble attribute key for testing:
// [KeyPrefixAttributes(1B)][attrType(1B)][canonical].
func makePebbleKey(canonical []byte, attrType byte) []byte {
	key := make([]byte, 2+len(canonical))
	key[0] = dal.KeyPrefixAttributes
	key[1] = attrType
	copy(key[2:], canonical)

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

	pebbleKey := makePebbleKey(canonical, dal.AttributeCodeMetadata)

	matched, feedErr := acc.Feed(pebbleKey, baseBytes)
	require.NoError(t, feedErr)
	require.True(t, matched)

	// Feed a different canonical key
	canonical2 := []byte("ledger\x00account2\x00field")
	base2Value := commonpb.NewStringValue("base2-val")
	base2Bytes, err := proto.Marshal(base2Value)
	require.NoError(t, err)

	pebbleKey2 := makePebbleKey(canonical2, dal.AttributeCodeMetadata)

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
	baseValue := &raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(100)}
	baseBytes, err := proto.Marshal(baseValue)
	require.NoError(t, err)

	pebbleKey := makePebbleKey(canonical, dal.AttributeCodeVolume)

	matched, feedErr := acc.Feed(pebbleKey, baseBytes)
	require.NoError(t, feedErr)
	require.False(t, matched, "volume key should not match metadata accumulator")
}

func TestAccumulatorFeedTooShortKey(t *testing.T) {
	t.Parallel()

	attrs := New()
	acc := attrs.Metadata.NewAccumulator()

	// Key shorter than 1+SuffixLen (need <= 2 bytes to be too short)
	shortKey := make([]byte, 1)
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
	require.NoError(t, errOnly(attrs.Volume.Set(batch, keyA, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(300),
	})))
	require.NoError(t, errOnly(attrs.Volume.Set(batch, keyB, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(500),
	})))
	// Also add metadata entries
	require.NoError(t, errOnly(attrs.Metadata.Set(batch, []byte("ledger\x00alice\x00field"), commonpb.NewStringValue("val"))))
	require.NoError(t, batch.Commit())

	// Run compaction - CompactAllForBackup creates its own batch and Attributes internally
	err := CompactAllForBackup(store)
	require.NoError(t, err)

	// After compaction, the values should still be readable using a fresh Attributes
	freshAttrs := New()
	result, err := freshAttrs.Volume.Get(store, keyA)
	require.NoError(t, err)
	require.NotNil(t, result)

	result, err = freshAttrs.Volume.Get(store, keyB)
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
	referenceKey := []byte("\x00\x00\x00\x01ref-abc")
	boundaryKey := []byte("myledger")

	// Write data
	batch := store.NewBatch()
	require.NoError(t, errOnly(attrs.Volume.Set(batch, volumeKey, &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(300),
		Output: commonpb.NewUint256FromUint64(100),
	})))
	require.NoError(t, errOnly(attrs.Metadata.Set(batch, metadataKey, commonpb.NewStringValue("active"))))
	require.NoError(t, errOnly(attrs.References.Set(batch, referenceKey, &commonpb.TransactionReferenceValue{
		TransactionId: 99,
	})))
	require.NoError(t, errOnly(attrs.Ledger.Set(batch, ledgerKey, &commonpb.LedgerInfo{
		Name: "myledger",
	})))
	require.NoError(t, errOnly(attrs.Boundary.Set(batch, boundaryKey, &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 10,
		NextLogId:         5,
	})))

	// Set lastAppliedIndex to a high value
	idxBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(idxBuf, 200)
	require.NoError(t, batch.SetBytes([]byte{dal.KeyPrefixLastAppliedIndex}, idxBuf))
	require.NoError(t, batch.Commit())

	// Run compaction
	require.NoError(t, CompactAllForBackup(store))

	// Verify all types are readable with correct values
	freshAttrs := New()

	vol, err := freshAttrs.Volume.Get(store, volumeKey)
	require.NoError(t, err)
	require.NotNil(t, vol)
	require.Equal(t, int64(300), vol.GetInput().ToBigInt().Int64(), "volume input: last Set wins (300)")
	require.Equal(t, int64(100), vol.GetOutput().ToBigInt().Int64(), "volume output: last Set wins (100)")

	meta, err := freshAttrs.Metadata.Get(store, metadataKey)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, "active", commonpb.MetadataValueToString(meta))

	ref, err := freshAttrs.References.Get(store, referenceKey)
	require.NoError(t, err)
	require.NotNil(t, ref)
	require.Equal(t, uint64(99), ref.GetTransactionId())

	ledger, err := freshAttrs.Ledger.Get(store, ledgerKey)
	require.NoError(t, err)
	require.NotNil(t, ledger)
	require.Equal(t, "myledger", ledger.GetName())

	boundary, err := freshAttrs.Boundary.Get(store, boundaryKey)
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
	// Alice: last Set wins = 50
	require.NoError(t, errOnly(attrs.Volume.Set(batch, keyAlice, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(50),
	})))
	// Bob: single Set = 200
	require.NoError(t, errOnly(attrs.Volume.Set(batch, keyBob, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(200),
	})))
	// Charlie: last Set wins = 300
	require.NoError(t, errOnly(attrs.Volume.Set(batch, keyCharlie, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(300),
	})))
	require.NoError(t, batch.Commit())

	require.NoError(t, CompactAllForBackup(store))

	freshAttrs := New()

	alice, err := freshAttrs.Volume.Get(store, keyAlice)
	require.NoError(t, err)
	require.NotNil(t, alice)
	require.Equal(t, int64(50), alice.GetInput().ToBigInt().Int64(), "alice: last Set wins (50)")

	bob, err := freshAttrs.Volume.Get(store, keyBob)
	require.NoError(t, err)
	require.NotNil(t, bob)
	require.Equal(t, int64(200), bob.GetInput().ToBigInt().Int64(), "bob: single Set 200")

	charlie, err := freshAttrs.Volume.Get(store, keyCharlie)
	require.NoError(t, err)
	require.NotNil(t, charlie)
	require.Equal(t, int64(300), charlie.GetInput().ToBigInt().Int64(), "charlie: last Set wins (300)")
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
