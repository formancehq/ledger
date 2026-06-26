package state

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func newDigestStore(t *testing.T, deterministic bool) *dal.Store {
	t.Helper()

	cfg := dal.DefaultConfig()
	cfg.DeterministicEncoding = deterministic

	store, err := dal.NewStore(t.TempDir(), logging.Testing(), noop.Meter{}, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return store
}

// TestWriteSession_Repr_SameWrites_SameBytes verifies that two
// WriteSessions receiving the same logical writes in the same order produce
// identical batch.Repr() bytes — the core invariant the digest relies on.
// If the FSM hot path's insertion-order contract (cf. WriteSet.Merge
// doc-block, EN-1325) holds, two nodes will hash identical bytes.
func TestWriteSession_Repr_SameWrites_SameBytes(t *testing.T) {
	t.Parallel()

	storeA := newDigestStore(t, true)
	storeB := newDigestStore(t, true)

	writeFixedSequence := func(sess *dal.WriteSession) {
		// Sorted-by-key writes to mirror the post-EN-1325 invariant: the
		// FSM hot path always emits in monotonic zone+sub-prefix order.
		writes := []struct {
			key   []byte
			value []byte
		}{
			{[]byte{0x01, 0x01, 0xAA, 0xBB}, []byte("epsilon")},
			{[]byte{0x06, 0x01, 0x01}, []byte("alpha")},
			{[]byte{0x06, 0x01, 0x02}, []byte("beta")},
			{[]byte{0x06, 0x01, 0x03}, []byte("gamma")},
			{[]byte{0x06, 0x02, 0x01}, []byte("delta")},
		}

		for _, w := range writes {
			require.NoError(t, sess.SetBytes(w.key, w.value))
		}
	}

	sessA := storeA.OpenWriteSession()
	defer func() { _ = sessA.Cancel() }()
	writeFixedSequence(sessA)

	sessB := storeB.OpenWriteSession()
	defer func() { _ = sessB.Cancel() }()
	writeFixedSequence(sessB)

	reprA := sessA.Repr()
	reprB := sessB.Repr()

	require.NotEmpty(t, reprA)
	require.Equal(t, reprA, reprB,
		"batch.Repr() must be byte-identical for sessions that received the same writes in the same order — this is what makes the cross-node digest comparable without canonicalisation")
}

// TestChainFSMDigest_DependsOnAllInputs sanity-checks that the chained
// digest reacts to every input component: previous digest, snapshot index,
// applied index, and the batch repr itself. A change in any one of
// these must produce a different digest — otherwise the chain is broken.
func TestChainFSMDigest_DependsOnAllInputs(t *testing.T) {
	t.Parallel()

	gen := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3, "cluster-1")

	baseline := func() []byte {
		_, d := chainFSMDigest(gen, nil, []byte{0x01, 0x02}, 7, 42, []byte("repr-A"))

		return append([]byte(nil), d...)
	}

	original := baseline()

	t.Run("different_previous_digest", func(t *testing.T) {
		t.Parallel()
		_, d := chainFSMDigest(gen, nil, []byte{0x99}, 7, 42, []byte("repr-A"))
		require.NotEqual(t, original, d)
	})

	t.Run("different_snapshot_index", func(t *testing.T) {
		t.Parallel()
		_, d := chainFSMDigest(gen, nil, []byte{0x01, 0x02}, 8, 42, []byte("repr-A"))
		require.NotEqual(t, original, d)
	})

	t.Run("different_applied_index", func(t *testing.T) {
		t.Parallel()
		_, d := chainFSMDigest(gen, nil, []byte{0x01, 0x02}, 7, 43, []byte("repr-A"))
		require.NotEqual(t, original, d)
	})

	t.Run("different_repr", func(t *testing.T) {
		t.Parallel()
		_, d := chainFSMDigest(gen, nil, []byte{0x01, 0x02}, 7, 42, []byte("repr-B"))
		require.NotEqual(t, original, d)
	})

	t.Run("identical_inputs_match", func(t *testing.T) {
		t.Parallel()
		_, d := chainFSMDigest(gen, nil, []byte{0x01, 0x02}, 7, 42, []byte("repr-A"))
		require.Equal(t, original, d,
			"identical inputs must yield identical digest (deterministic)")
	})
}

// TestChainFSMDigest_DifferentClusterIDs_DifferentDigests verifies that two
// clusters with the same writes but different ClusterIDs produce different
// digests, thanks to the per-cluster keyed HashGenerator. This is what
// prevents an attacker from replaying a digest from one cluster against
// another.
func TestChainFSMDigest_DifferentClusterIDs_DifferentDigests(t *testing.T) {
	t.Parallel()

	a := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3, "cluster-a")
	b := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3, "cluster-b")

	_, da := chainFSMDigest(a, nil, nil, 0, 1, []byte("repr"))
	_, db := chainFSMDigest(b, nil, nil, 0, 1, []byte("repr"))

	require.NotEqual(t, da, db,
		"per-cluster keying must make the digest cluster-bound")
}

// TestEncodeFSMDigestValue verifies the round-trip layout used in
// SubGlobFSMDigest: u64(appliedIndex) || u64(snapshotIndex) || digest.
func TestEncodeFSMDigestValue(t *testing.T) {
	t.Parallel()

	digest := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	v := encodeFSMDigestValue(nil, 0xDEADBEEFCAFEBABE, 0x1122334455667788, digest)

	require.Len(t, v, 8+8+len(digest))
	// Big-endian appliedIndex.
	require.Equal(t,
		[]byte{0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE},
		v[0:8])
	// Big-endian snapshotIndex.
	require.Equal(t,
		[]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88},
		v[8:16])
	// Digest trailer.
	require.Equal(t, digest, v[16:])
}
