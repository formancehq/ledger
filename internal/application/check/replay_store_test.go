package check

import (
	"math/big"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func newTestReplayStore(t *testing.T) *replayStore {
	t.Helper()

	rs, err := newReplayStore()
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	return rs
}

func strMetaMap(entries ...string) map[string]*commonpb.MetadataValue {
	m := make(map[string]*commonpb.MetadataValue, len(entries)/2)
	for i := 0; i < len(entries); i += 2 {
		m[entries[i]] = &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: entries[i+1]}}
	}

	return m
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

	require.NoError(t, rs.AddVolumeDelta(key, big.NewInt(100), big.NewInt(0)))

	pair := readVolume(t, rs, key)
	require.Equal(t, "100", pair.GetInput().ToBigInt().String())
	require.Equal(t, "0", pair.GetOutput().ToBigInt().String())
}

func TestReplayStoreVolumeAccumulation(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00account\x00USD")

	require.NoError(t, rs.AddVolumeDelta(key, big.NewInt(100), big.NewInt(0)))
	require.NoError(t, rs.AddVolumeDelta(key, big.NewInt(50), big.NewInt(30)))
	require.NoError(t, rs.AddVolumeDelta(key, big.NewInt(0), big.NewInt(70)))

	pair := readVolume(t, rs, key)
	require.Equal(t, "150", pair.GetInput().ToBigInt().String())
	require.Equal(t, "100", pair.GetOutput().ToBigInt().String())
}

func TestReplayStoreVolumeMultipleKeys(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	keyUSD := []byte("ledger\x00account\x00USD")
	keyEUR := []byte("ledger\x00account\x00EUR")

	require.NoError(t, rs.AddVolumeDelta(keyUSD, big.NewInt(100), big.NewInt(50)))
	require.NoError(t, rs.AddVolumeDelta(keyEUR, big.NewInt(200), big.NewInt(0)))

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

	require.NoError(t, rs.SetMetadata(key, "admin"))

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

	require.NoError(t, rs.SetMetadata(key, "user"))
	require.NoError(t, rs.SetMetadata(key, "admin"))

	val, closer, err := rs.db.Get(replayKey(replayPrefixMetadata, key))
	require.NoError(t, err)
	defer func() { _ = closer.Close() }()

	require.Equal(t, "admin", string(val[1:]))
}

func TestReplayStoreMetadataDelete(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00account\x00role")

	require.NoError(t, rs.SetMetadata(key, "admin"))
	require.NoError(t, rs.DeleteMetadata(key))

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

	meta := strMetaMap("env", "prod")

	require.NoError(t, rs.CreateTransaction(key, 42, 0, meta, nil))

	state := readTransaction(t, rs, key)
	require.Equal(t, uint64(42), state.GetCreatedByLog())
	require.Len(t, state.GetMetadata(), 1)
	require.Equal(t, "prod", state.GetMetadata()["env"].GetStringValue())
}

func TestReplayStoreTransactionCreateWithTimestamp(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00tx:1")

	ts := uint64(1_700_000_000_000_000)
	require.NoError(t, rs.CreateTransaction(key, 7, ts, nil, nil))

	state := readTransaction(t, rs, key)
	require.Equal(t, uint64(7), state.GetCreatedByLog())
	require.NotEqual(t, uint64(0), state.GetTimestamp())
	require.Equal(t, uint64(1_700_000_000_000_000), state.GetTimestamp())
}

func TestReplayStoreTransactionCreateWithUnsetTimestamp(t *testing.T) {
	t.Parallel()

	// After inlining Timestamp into a fixed64 scalar, 0 is the unset sentinel;
	// there is no longer a distinction between "no timestamp" and
	// "Timestamp{Data:0}" — the HLC never emits 0 so this collapse is safe.
	// The replay store's merger must round-trip 0 back to 0.
	rs := newTestReplayStore(t)

	key := []byte("ledger\x00tx:1")
	require.NoError(t, rs.CreateTransaction(key, 1, 0, nil, nil))
	require.Equal(t, uint64(0), readTransaction(t, rs, key).GetTimestamp())
}

func TestReplayStoreTransactionCreateWithoutMetadata(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00tx:1")

	require.NoError(t, rs.CreateTransaction(key, 10, 0, nil, nil))

	state := readTransaction(t, rs, key)
	require.Equal(t, uint64(10), state.GetCreatedByLog())
	require.Empty(t, state.GetMetadata())
}

func TestReplayStoreTransactionRevertedBy(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00tx:1")

	require.NoError(t, rs.CreateTransaction(key, 5, 0, nil, nil))
	require.NoError(t, rs.SetRevertedBy(key, 99))

	state := readTransaction(t, rs, key)
	require.Equal(t, uint64(5), state.GetCreatedByLog())
	require.Equal(t, uint64(99), state.GetRevertedByTransaction())
}

func TestReplayStoreTransactionSaveMeta(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00tx:1")

	require.NoError(t, rs.CreateTransaction(key, 1, 0, nil, nil))
	require.NoError(t, rs.SaveTxMetadata(key, strMetaMap("env", "staging")))

	state := readTransaction(t, rs, key)
	require.Len(t, state.GetMetadata(), 1)
	require.Equal(t, "staging", state.GetMetadata()["env"].GetStringValue())
}

func TestReplayStoreTransactionSaveMetaOverwrite(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00tx:1")

	meta := strMetaMap("env", "dev")

	require.NoError(t, rs.CreateTransaction(key, 1, 0, meta, nil))
	require.NoError(t, rs.SaveTxMetadata(key, strMetaMap("env", "prod")))

	state := readTransaction(t, rs, key)
	require.Len(t, state.GetMetadata(), 1)
	require.Equal(t, "prod", state.GetMetadata()["env"].GetStringValue())
}

func TestReplayStoreTransactionDeleteMeta(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00tx:1")

	meta := strMetaMap("env", "prod", "region", "eu")

	require.NoError(t, rs.CreateTransaction(key, 1, 0, meta, nil))
	require.NoError(t, rs.DeleteTxMetadata(key, "env"))

	state := readTransaction(t, rs, key)
	require.Len(t, state.GetMetadata(), 1)
	require.Equal(t, "eu", state.GetMetadata()["region"].GetStringValue())
}

func TestReplayStoreTransactionFullLifecycle(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)
	key := []byte("ledger\x00tx:1")

	// Create with initial metadata
	require.NoError(t, rs.CreateTransaction(key, 1, 0, strMetaMap("type", "payment"), nil))

	// Add metadata
	require.NoError(t, rs.SaveTxMetadata(key, strMetaMap("status", "pending")))

	// Overwrite metadata
	require.NoError(t, rs.SaveTxMetadata(key, strMetaMap("status", "completed")))

	// Delete metadata
	require.NoError(t, rs.DeleteTxMetadata(key, "type"))

	// Revert
	require.NoError(t, rs.SetRevertedBy(key, 42))

	state := readTransaction(t, rs, key)
	require.Equal(t, uint64(1), state.GetCreatedByLog())
	require.Equal(t, uint64(42), state.GetRevertedByTransaction())
	require.Len(t, state.GetMetadata(), 1)
	require.Equal(t, "completed", state.GetMetadata()["status"].GetStringValue())
}

func TestReplayStorePrefixIter(t *testing.T) {
	t.Parallel()

	rs := newTestReplayStore(t)

	// Write data across all three prefixes.
	require.NoError(t, rs.AddVolumeDelta([]byte("k1"), big.NewInt(10), big.NewInt(0)))
	require.NoError(t, rs.AddVolumeDelta([]byte("k2"), big.NewInt(20), big.NewInt(0)))
	require.NoError(t, rs.SetMetadata([]byte("m1"), "v1"))
	require.NoError(t, rs.CreateTransaction([]byte("t1"), 1, 0, nil, nil))

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
