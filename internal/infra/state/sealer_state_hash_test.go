package state

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestComputeStateHash_Deterministic asserts the state hash is byte-stable
// even when attribute values contain protobuf map fields whose Go map
// iteration order is randomized across calls.
//
// TransactionState.Metadata is a map[string]*MetadataValue. Before #173
// computeStateHash marshaled it via MarshalVT, which iterates the Go map
// in randomized order — so two runs on the same store could produce
// different state hashes. The leader-only-computes design papered over
// this in production (no follower ever recompares), but a future cross-
// node verification would have been silently wrong on any ledger with
// multi-field transaction metadata.
//
// With MarshalDeterministicVT the marshal sorts map keys, so the hash
// is reproducible across runs and across nodes.
func TestComputeStateHash_Deterministic(t *testing.T) {
	t.Parallel()

	store := createSealerTestStore(t)

	attrs := attributes.New()
	batch := store.OpenWriteSession()

	// Populate a TransactionState with enough metadata keys to make
	// Go's randomized map iteration order observable. 16 keys gives
	// 16! ≈ 2 × 10^13 possible orderings — two runs ordering them
	// identically by coincidence is vanishingly unlikely.
	txState := &commonpb.TransactionState{
		CreatedByLog: 42,
		Metadata:     make(map[string]*commonpb.MetadataValue, 16),
	}
	for i := range 16 {
		key := "key-" + string(rune('a'+i))
		txState.Metadata[key] = commonpb.NewStringValue("value-" + key)
	}

	_, err := attrs.Transaction.Set(batch, []byte("l\x00\x00\x00\x00\x01"), txState)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())

	// Compute the hash multiple times. Each call iterates the map
	// fresh; with MarshalDeterministicVT the result must be identical.
	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	const runs = 20

	hashes := make([][]byte, runs)
	for i := range runs {
		h, err := computeStateHash(handle, attrs)
		require.NoError(t, err)
		hashes[i] = h
	}

	for i := 1; i < runs; i++ {
		require.Equal(t, hashes[0], hashes[i],
			"state hash drifted between run 0 and run %d (was non-deterministic): %x vs %x",
			i, hashes[0], hashes[i])
	}
}

// TestComputeStateHash_CoversAllThreeStores writes one entry per store
// (Volume, Metadata, Transaction) and asserts the hash changes when any
// one of them changes. Guards against a refactor that accidentally drops
// a store from the iteration.
func TestComputeStateHash_CoversAllThreeStores(t *testing.T) {
	t.Parallel()

	mkStore := func(t *testing.T, vol uint64, meta string, tx uint64) []byte {
		t.Helper()

		store := createSealerTestStore(t)
		attrs := attributes.New()

		batch := store.OpenWriteSession()
		_, err := attrs.Volume.Set(batch, []byte("l\x00a\x00USD"),
			&raftcmdpb.VolumePair{Input: commonpb.NewUint256FromUint64(vol)})
		require.NoError(t, err)
		_, err = attrs.Metadata.Set(batch, []byte("l\x00a\x00key"),
			commonpb.NewStringValue(meta))
		require.NoError(t, err)
		_, err = attrs.Transaction.Set(batch, []byte("l\x00\x00\x00\x00\x01"),
			&commonpb.TransactionState{CreatedByLog: tx})
		require.NoError(t, err)
		require.NoError(t, batch.Commit())

		handle, err := store.NewReadHandle()
		require.NoError(t, err)
		defer func() { _ = handle.Close() }()

		h, err := computeStateHash(handle, attrs)
		require.NoError(t, err)

		return h
	}

	baseline := mkStore(t, 100, "active", 1)

	require.NotEqual(t, baseline, mkStore(t, 999, "active", 1),
		"changing Volume must change the hash")
	require.NotEqual(t, baseline, mkStore(t, 100, "different", 1),
		"changing Metadata must change the hash")
	require.NotEqual(t, baseline, mkStore(t, 100, "active", 42),
		"changing TransactionState must change the hash")
}

// TestHashAttribute_EmptyStore guards that the iteration handles an
// empty attribute store without erroring — the loop simply doesn't fire,
// Close + Err return nil, and the hasher state is left unchanged.
func TestHashAttribute_EmptyStore(t *testing.T) {
	t.Parallel()

	store := createSealerTestStore(t)
	attrs := attributes.New()
	hasher := blake3.New()
	beforeSum := hasher.Sum(nil)

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	var buf []byte
	require.NoError(t, hashAttribute(handle, attrs.Volume, hasher, &buf, "volume"))

	require.Equal(t, beforeSum, hasher.Sum(nil),
		"hashAttribute on an empty store must not advance the hasher")
}
