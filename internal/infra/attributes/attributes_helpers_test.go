package attributes

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func errOnly(_ []byte, err error) error { return err }

func TestAttrTypeFromKey(t *testing.T) {
	t.Parallel()

	t.Run("valid key", func(t *testing.T) {
		t.Parallel()
		// Build a key: [0xF1][attrType][canonical...]
		key := make([]byte, 20)
		key[0] = dal.ZoneAttributes
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
		key[0] = dal.ZoneAttributes
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

	batch := store.OpenWriteSession()
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

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	iter, err := attrs.Volume.NewStreamingIter(handle, []byte("ledger\x00"))
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

	batch := store.OpenWriteSession()
	require.NoError(t, errOnly(attrs.Metadata.Set(batch, keyA, commonpb.NewStringValue("alice-val"))))
	require.NoError(t, errOnly(attrs.Metadata.Set(batch, keyB, commonpb.NewStringValue("bob-val"))))
	require.NoError(t, batch.Commit())

	// Use ComputeAllForPrefix to get all entries under the ledger prefix
	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	entries, err := attrs.Metadata.ComputeAllForPrefix(handle, []byte("ledger\x00"))
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
	require.Equal(t, dal.SubAttrVolume, acc.Prefix())

	metaAcc := attrs.Metadata.NewAccumulator()
	require.Equal(t, dal.SubAttrMetadata, metaAcc.Prefix())
}

func TestGetReturnsLatestSet(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	attrs := New()

	testKey := []byte("ledger\x00maxidx\x00USD")

	// Multiple Sets overwrite in place — last Set wins
	batch := store.OpenWriteSession()
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
	batch := store.OpenWriteSession()
	require.NoError(t, errOnly(attrs.Volume.Set(batch, keyA, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(500),
	})))
	require.NoError(t, errOnly(attrs.Volume.Set(batch, keyB, &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(200),
	})))
	require.NoError(t, batch.Commit())

	// StreamingIter scans all entries and groups by canonical key.
	var results []ComputedEntry[*raftcmdpb.VolumePair]

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	iter, err := attrs.Volume.NewStreamingIter(handle, []byte("test\x00"))
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
	batch := store.OpenWriteSession()
	require.NoError(t, errOnly(attrs.References.Set(batch, testKey, &commonpb.TransactionReferenceValue{
		TransactionId: 42,
	})))
	require.NoError(t, batch.Commit())

	result, err := attrs.References.Get(store, testKey)
	require.NoError(t, err)
	require.Equal(t, uint64(42), result.GetTransactionId())

	// Overwrite with a later Set
	batch = store.OpenWriteSession()
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
	batch := store.OpenWriteSession()
	require.NoError(t, errOnly(attrs.Ledger.Set(batch, testKey, &commonpb.LedgerInfo{
		Name: "my-ledger",
	})))
	require.NoError(t, batch.Commit())

	result, err := attrs.Ledger.Get(store, testKey)
	require.NoError(t, err)
	require.Equal(t, "my-ledger", result.GetName())

	// Overwrite with a later Set — latest wins
	batch = store.OpenWriteSession()
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

	batch := store.OpenWriteSession()
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
	key[0] = dal.ZoneAttributes
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

	pebbleKey := makePebbleKey(canonical, dal.SubAttrMetadata)

	matched, feedErr := acc.Feed(pebbleKey, baseBytes)
	require.NoError(t, feedErr)
	require.True(t, matched)

	// Feed a different canonical key
	canonical2 := []byte("ledger\x00account2\x00field")
	base2Value := commonpb.NewStringValue("base2-val")
	base2Bytes, err := proto.Marshal(base2Value)
	require.NoError(t, err)

	pebbleKey2 := makePebbleKey(canonical2, dal.SubAttrMetadata)

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

	pebbleKey := makePebbleKey(canonical, dal.SubAttrVolume)

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
