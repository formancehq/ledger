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

// TestCanonicalBatchPayload_OrderIndependent verifies that two WriteSessions
// receiving the same logical writes in DIFFERENT insertion orders produce
// identical canonical payloads. This is the core invariant of the cross-node
// digest: Pebble batches are insertion-ordered, but the canonical sort
// neutralises that.
func TestCanonicalBatchPayload_OrderIndependent(t *testing.T) {
	t.Parallel()

	store := newDigestStore(t, true)

	writeOps := func(sess *dal.WriteSession, reverse bool) {
		keys := [][]byte{
			{0x06, 0x01, 0x01},
			{0x06, 0x01, 0x02},
			{0x06, 0x01, 0x03},
			{0x06, 0x02, 0x01},
			{0x01, 0x01, 0xAA, 0xBB},
		}
		values := [][]byte{
			[]byte("alpha"),
			[]byte("beta"),
			[]byte("gamma"),
			[]byte("delta"),
			[]byte("epsilon"),
		}

		idx := []int{0, 1, 2, 3, 4}
		if reverse {
			idx = []int{4, 3, 2, 1, 0}
		}

		for _, i := range idx {
			require.NoError(t, sess.SetBytes(keys[i], values[i]))
		}
	}

	sessFwd := store.OpenWriteSession()
	defer func() { _ = sessFwd.Cancel() }()
	writeOps(sessFwd, false)

	sessRev := store.OpenWriteSession()
	defer func() { _ = sessRev.Cancel() }()
	writeOps(sessRev, true)

	var (
		opsBuf []bufferedOp
		fwdBuf []byte
		revBuf []byte
		fwdPay []byte
		revPay []byte
		err    error
	)

	fwdPay, opsBuf, err = canonicalBatchPayload(fwdBuf, opsBuf, sessFwd)
	require.NoError(t, err)

	revPay, _, err = canonicalBatchPayload(revBuf, opsBuf, sessRev)
	require.NoError(t, err)

	require.Equal(t, fwdPay, revPay,
		"canonical payloads must be byte-identical regardless of insertion order")
}

// TestChainFSMDigest_DependsOnAllInputs sanity-checks that the chained
// digest reacts to every input component: previous digest, snapshot index,
// applied index, and the canonical payload itself. A change in any one of
// these must produce a different digest — otherwise the chain is broken.
func TestChainFSMDigest_DependsOnAllInputs(t *testing.T) {
	t.Parallel()

	gen := processing.NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3, "cluster-1")

	baseline := func() []byte {
		_, d := chainFSMDigest(gen, nil, []byte{0x01, 0x02}, 7, 42, []byte("payload-A"))

		return append([]byte(nil), d...)
	}

	original := baseline()

	t.Run("different_previous_digest", func(t *testing.T) {
		t.Parallel()
		_, d := chainFSMDigest(gen, nil, []byte{0x99}, 7, 42, []byte("payload-A"))
		require.NotEqual(t, original, d)
	})

	t.Run("different_snapshot_index", func(t *testing.T) {
		t.Parallel()
		_, d := chainFSMDigest(gen, nil, []byte{0x01, 0x02}, 8, 42, []byte("payload-A"))
		require.NotEqual(t, original, d)
	})

	t.Run("different_applied_index", func(t *testing.T) {
		t.Parallel()
		_, d := chainFSMDigest(gen, nil, []byte{0x01, 0x02}, 7, 43, []byte("payload-A"))
		require.NotEqual(t, original, d)
	})

	t.Run("different_payload", func(t *testing.T) {
		t.Parallel()
		_, d := chainFSMDigest(gen, nil, []byte{0x01, 0x02}, 7, 42, []byte("payload-B"))
		require.NotEqual(t, original, d)
	})

	t.Run("identical_inputs_match", func(t *testing.T) {
		t.Parallel()
		_, d := chainFSMDigest(gen, nil, []byte{0x01, 0x02}, 7, 42, []byte("payload-A"))
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

	_, da := chainFSMDigest(a, nil, nil, 0, 1, []byte("payload"))
	_, db := chainFSMDigest(b, nil, nil, 0, 1, []byte("payload"))

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

// TestWriteSession_IterateBufferedOps verifies that the iterator yields every
// buffered op exactly once, in insertion order (the digest path then sorts
// them — see canonicalBatchPayload).
func TestWriteSession_IterateBufferedOps(t *testing.T) {
	t.Parallel()

	store := newDigestStore(t, true)
	sess := store.OpenWriteSession()
	defer func() { _ = sess.Cancel() }()

	require.NoError(t, sess.SetBytes([]byte("key-1"), []byte("val-1")))
	require.NoError(t, sess.SetBytes([]byte("key-2"), []byte("val-2")))
	require.NoError(t, sess.DeleteKey([]byte("key-3")))

	type op struct {
		kind  uint8
		key   string
		value string
	}

	var collected []op

	err := sess.IterateBufferedOps(func(kind uint8, key, value []byte) error {
		collected = append(collected, op{kind: kind, key: string(key), value: string(value)})

		return nil
	})
	require.NoError(t, err)

	require.Len(t, collected, 3)
	require.Equal(t, "key-1", collected[0].key)
	require.Equal(t, "val-1", collected[0].value)
	require.Equal(t, "key-2", collected[1].key)
	require.Equal(t, "key-3", collected[2].key)
	require.Empty(t, collected[2].value, "delete op carries no value")
	// The set/delete kinds must differ — exact numeric value is Pebble's
	// internal constant; we only assert they don't collide.
	require.NotEqual(t, collected[0].kind, collected[2].kind)
}
