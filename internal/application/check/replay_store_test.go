package check

import (
	"math/big"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func newTestReplayStore(t *testing.T) *replayStore {
	t.Helper()

	rs, err := newReplayStore()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	return rs
}

func strMeta(key, value string) *commonpb.Metadata {
	return &commonpb.Metadata{
		Key:   key,
		Value: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: value}},
	}
}

// readVolume reads the merged VolumePair for the given canonical key.
func readVolume(t *testing.T, rs *replayStore, canonicalKey []byte) *raftcmdpb.VolumePair {
	t.Helper()

	val, closer, err := rs.db.Get(replayKey(replayPrefixVolume, canonicalKey))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()

	var pair raftcmdpb.VolumePair
	require.NoError(t, pair.UnmarshalVT(val))

	return &pair
}

// readTransaction reads the merged TransactionState for the given canonical key.
func readTransaction(t *testing.T, rs *replayStore, canonicalKey []byte) *commonpb.TransactionState {
	t.Helper()

	val, closer, err := rs.db.Get(replayKey(replayPrefixTransaction, canonicalKey))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()

	// Values are prefixed with txOpFinalized tag from the merger's Finish output.
	require.NotEmpty(t, val)
	require.Equal(t, byte(txOpFinalized), val[0], "expected txOpFinalized prefix")

	var state commonpb.TransactionState
	require.NoError(t, state.UnmarshalVT(val[1:]))

	return &state
}

func TestReplayStoreVolumeSingleDelta(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00account\x00USD")

	require.NoError(t, rs.addVolumeDelta(key, big.NewInt(100), big.NewInt(0)))

	pair := readVolume(t, rs, key)
	require.Equal(t, "100", pair.GetInput().ToBigInt().String())
	require.Equal(t, "0", pair.GetOutput().ToBigInt().String())
}

func TestReplayStoreVolumeAccumulation(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00account\x00USD")

	require.NoError(t, rs.addVolumeDelta(key, big.NewInt(100), big.NewInt(0)))
	require.NoError(t, rs.addVolumeDelta(key, big.NewInt(50), big.NewInt(30)))
	require.NoError(t, rs.addVolumeDelta(key, big.NewInt(0), big.NewInt(70)))

	pair := readVolume(t, rs, key)
	require.Equal(t, "150", pair.GetInput().ToBigInt().String())
	require.Equal(t, "100", pair.GetOutput().ToBigInt().String())
}

func TestReplayStoreVolumeMultipleKeys(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	keyUSD := []byte("ledger\x00account\x00USD")
	keyEUR := []byte("ledger\x00account\x00EUR")

	require.NoError(t, rs.addVolumeDelta(keyUSD, big.NewInt(100), big.NewInt(50)))
	require.NoError(t, rs.addVolumeDelta(keyEUR, big.NewInt(200), big.NewInt(0)))

	pairUSD := readVolume(t, rs, keyUSD)
	require.Equal(t, "100", pairUSD.GetInput().ToBigInt().String())
	require.Equal(t, "50", pairUSD.GetOutput().ToBigInt().String())

	pairEUR := readVolume(t, rs, keyEUR)
	require.Equal(t, "200", pairEUR.GetInput().ToBigInt().String())
	require.Equal(t, "0", pairEUR.GetOutput().ToBigInt().String())
}

func TestReplayStoreMetadataSetAndRead(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00account\x00role")

	require.NoError(t, rs.setMetadata(key, "admin"))

	val, closer, err := rs.db.Get(replayKey(replayPrefixMetadata, key))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()

	require.Equal(t, byte(metaFlagSet), val[0])
	require.Equal(t, "admin", string(val[1:]))
}

func TestReplayStoreMetadataOverwrite(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00account\x00role")

	require.NoError(t, rs.setMetadata(key, "user"))
	require.NoError(t, rs.setMetadata(key, "admin"))

	val, closer, err := rs.db.Get(replayKey(replayPrefixMetadata, key))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()

	require.Equal(t, "admin", string(val[1:]))
}

func TestReplayStoreMetadataDelete(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00account\x00role")

	require.NoError(t, rs.setMetadata(key, "admin"))
	require.NoError(t, rs.deleteMetadata(key))

	val, closer, err := rs.db.Get(replayKey(replayPrefixMetadata, key))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()

	require.Equal(t, byte(metaFlagDeleted), val[0])
	require.Len(t, val, 1)
}

func TestReplayStoreTransactionCreate(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00tx:1")

	meta := &commonpb.MetadataSet{
		Metadata: []*commonpb.Metadata{
			strMeta("env", "prod"),
		},
	}

	require.NoError(t, rs.createTransaction(key, 42, meta))

	state := readTransaction(t, rs, key)
	require.Equal(t, uint64(42), state.GetCreatedByLog())
	require.Len(t, state.GetMetadata().GetMetadata(), 1)
	require.Equal(t, "env", state.GetMetadata().GetMetadata()[0].GetKey())
	require.Equal(t, "prod", state.GetMetadata().GetMetadata()[0].GetValue().GetStringValue())
}

func TestReplayStoreTransactionCreateWithoutMetadata(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00tx:1")

	require.NoError(t, rs.createTransaction(key, 10, nil))

	state := readTransaction(t, rs, key)
	require.Equal(t, uint64(10), state.GetCreatedByLog())
	require.Nil(t, state.GetMetadata())
}

func TestReplayStoreTransactionRevertedBy(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00tx:1")

	require.NoError(t, rs.createTransaction(key, 5, nil))
	require.NoError(t, rs.setRevertedBy(key, 99))

	state := readTransaction(t, rs, key)
	require.Equal(t, uint64(5), state.GetCreatedByLog())
	require.Equal(t, uint64(99), state.GetRevertedByTransaction())
}

func TestReplayStoreTransactionSaveMeta(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00tx:1")

	require.NoError(t, rs.createTransaction(key, 1, nil))
	require.NoError(t, rs.saveTxMetadata(key, []*commonpb.Metadata{
		strMeta("env", "staging"),
	}))

	state := readTransaction(t, rs, key)
	require.Len(t, state.GetMetadata().GetMetadata(), 1)
	require.Equal(t, "staging", state.GetMetadata().GetMetadata()[0].GetValue().GetStringValue())
}

func TestReplayStoreTransactionSaveMetaOverwrite(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00tx:1")

	meta := &commonpb.MetadataSet{
		Metadata: []*commonpb.Metadata{
			strMeta("env", "dev"),
		},
	}

	require.NoError(t, rs.createTransaction(key, 1, meta))
	require.NoError(t, rs.saveTxMetadata(key, []*commonpb.Metadata{
		strMeta("env", "prod"),
	}))

	state := readTransaction(t, rs, key)
	require.Len(t, state.GetMetadata().GetMetadata(), 1)
	require.Equal(t, "prod", state.GetMetadata().GetMetadata()[0].GetValue().GetStringValue())
}

func TestReplayStoreTransactionDeleteMeta(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00tx:1")

	meta := &commonpb.MetadataSet{
		Metadata: []*commonpb.Metadata{
			strMeta("env", "prod"),
			strMeta("region", "eu"),
		},
	}

	require.NoError(t, rs.createTransaction(key, 1, meta))
	require.NoError(t, rs.deleteTxMetadata(key, "env"))

	state := readTransaction(t, rs, key)
	require.Len(t, state.GetMetadata().GetMetadata(), 1)
	require.Equal(t, "region", state.GetMetadata().GetMetadata()[0].GetKey())
}

func TestReplayStoreTransactionFullLifecycle(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00tx:1")

	// Create with initial metadata
	require.NoError(t, rs.createTransaction(key, 1, &commonpb.MetadataSet{
		Metadata: []*commonpb.Metadata{
			strMeta("type", "payment"),
		},
	}))

	// Add metadata
	require.NoError(t, rs.saveTxMetadata(key, []*commonpb.Metadata{
		strMeta("status", "pending"),
	}))

	// Overwrite metadata
	require.NoError(t, rs.saveTxMetadata(key, []*commonpb.Metadata{
		strMeta("status", "completed"),
	}))

	// Delete metadata
	require.NoError(t, rs.deleteTxMetadata(key, "type"))

	// Revert
	require.NoError(t, rs.setRevertedBy(key, 42))

	state := readTransaction(t, rs, key)
	require.Equal(t, uint64(1), state.GetCreatedByLog())
	require.Equal(t, uint64(42), state.GetRevertedByTransaction())
	require.Len(t, state.GetMetadata().GetMetadata(), 1)
	require.Equal(t, "status", state.GetMetadata().GetMetadata()[0].GetKey())
	require.Equal(t, "completed", state.GetMetadata().GetMetadata()[0].GetValue().GetStringValue())
}

func TestReplayStorePrefixIter(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	// Write data across all three prefixes.
	require.NoError(t, rs.addVolumeDelta([]byte("k1"), big.NewInt(10), big.NewInt(0)))
	require.NoError(t, rs.addVolumeDelta([]byte("k2"), big.NewInt(20), big.NewInt(0)))
	require.NoError(t, rs.setMetadata([]byte("m1"), "v1"))
	require.NoError(t, rs.createTransaction([]byte("t1"), 1, nil))

	// Volume prefix should yield exactly 2 entries.
	iter, err := rs.newPrefixIter(replayPrefixVolume)
	require.NoError(t, err)

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	require.NoError(t, iter.Close())
	require.Equal(t, 2, count)

	// Metadata prefix should yield 1 entry.
	iter, err = rs.newPrefixIter(replayPrefixMetadata)
	require.NoError(t, err)

	count = 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	require.NoError(t, iter.Close())
	require.Equal(t, 1, count)

	// Transaction prefix should yield 1 entry.
	iter, err = rs.newPrefixIter(replayPrefixTransaction)
	require.NoError(t, err)

	count = 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	require.NoError(t, iter.Close())
	require.Equal(t, 1, count)
}

func TestReplayStoreMetadataNotFound(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	_, _, err := rs.db.Get(replayKey(replayPrefixMetadata, []byte("nonexistent")))
	require.ErrorIs(t, err, pebble.ErrNotFound)
}
