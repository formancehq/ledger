package processing

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func testOrders() []*raftcmdpb.Order {
	return []*raftcmdpb.Order{
		{Type: &raftcmdpb.Order_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Name: "test"}}},
		{Type: &raftcmdpb.Order_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Name: "test2"}}},
	}
}

func TestHashGenerator_BLAKE3_OutputSize(t *testing.T) {
	t.Parallel()

	g := NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, "cluster-A")

	_, hash := g.Compute(nil, nil, testOrders())
	require.Len(t, hash, 32, "BLAKE3 should produce 32-byte hash")
	require.Equal(t, commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, g.Algorithm())
}

func TestHashGenerator_XXH3_OutputSize(t *testing.T) {
	t.Parallel()

	g := NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3, "cluster-A")

	_, hash := g.Compute(nil, nil, testOrders()[:1])
	require.Len(t, hash, 16, "XXH3 should produce 16-byte hash")
	require.Equal(t, commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3, g.Algorithm())
}

func TestHashGenerator_Chaining(t *testing.T) {
	t.Parallel()

	g := NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, "cluster-A")
	orders := testOrders()

	_, hash1 := g.Compute(nil, nil, orders)
	_, hash2 := g.Compute(nil, []byte("prev-hash"), orders)
	require.NotEqual(t, hash1, hash2, "different lastAuditHash should produce different hash")
}

func TestHashGenerator_EmptyOrders(t *testing.T) {
	t.Parallel()

	g := NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, "cluster-A")

	_, hash := g.Compute(nil, nil, nil)
	require.Len(t, hash, 32, "empty orders should still produce a hash")
}

func TestHashGenerator_Deterministic(t *testing.T) {
	t.Parallel()

	g := NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, "cluster-A")
	orders := testOrders()
	lastHash := []byte("chain")

	_, hash1 := g.Compute(nil, lastHash, orders)
	_, hash2 := g.Compute(nil, lastHash, orders)
	require.Equal(t, hash1, hash2, "same inputs must produce same hash")
}

// TestHashGenerator_PerClusterKey pins the security property: two
// clusters with different ClusterIDs produce different audit hashes
// for the same orders. An attacker with knowledge of the inputs but
// not the ClusterID cannot forge a chain entry.
func TestHashGenerator_PerClusterKey(t *testing.T) {
	t.Parallel()

	for _, algo := range []commonpb.HashAlgorithm{
		commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3,
		commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3,
	} {
		gA := NewHashGenerator(algo, "cluster-A")
		gB := NewHashGenerator(algo, "cluster-B")

		_, hashA := gA.Compute(nil, nil, testOrders())
		_, hashB := gB.Compute(nil, nil, testOrders())
		require.NotEqual(t, hashA, hashB,
			"algo %s: same inputs under different ClusterIDs must produce different hashes", algo)
	}
}

// TestHashGenerator_DomainSeparation pins that the XXH3 and BLAKE3
// derivations don't share a key value for the same ClusterID — the
// per-algorithm context strings keep them independent.
func TestHashGenerator_DomainSeparation(t *testing.T) {
	t.Parallel()

	xxh3Gen := newXXH3HashGenerator("cluster-A")
	blake3Gen := newBLAKE3HashGenerator("cluster-A")

	// Compare the first 8 bytes of the BLAKE3 key against the XXH3 seed
	// (which is itself the first 8 bytes of its derived material). They
	// must differ because the domain-separator strings differ.
	var blakePrefix [8]byte
	copy(blakePrefix[:], blake3Gen.key[:8])

	var xxh3Seed [8]byte
	for i := range 8 {
		xxh3Seed[i] = byte(xxh3Gen.seed >> (8 * (7 - i)))
	}
	require.NotEqual(t, blakePrefix, xxh3Seed,
		"XXH3 seed and BLAKE3 key must be domain-separated for the same ClusterID")
}

// TestHashGenerator_UnknownAlgorithmFallsBackToBLAKE3 pins that any
// HashVersion the binary doesn't recognize (future enum value, stale
// data) verifies under BLAKE3 instead of panicking — the chain stays
// inspectable so the checker can flag a mismatch loudly.
func TestHashGenerator_UnknownAlgorithmFallsBackToBLAKE3(t *testing.T) {
	t.Parallel()

	g := NewHashGenerator(commonpb.HashAlgorithm(99), "cluster-A")
	require.Equal(t, commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, g.Algorithm())

	_, hash := g.Compute(nil, nil, testOrders())
	require.Len(t, hash, 32)
}

// TestHashGenerator_MixedAlgorithmChain pins that the chain remains
// verifiable across an algorithm change mid-cluster-lifetime: each
// entry verifies under a generator constructed from its own
// HashVersion + the shared ClusterID.
func TestHashGenerator_MixedAlgorithmChain(t *testing.T) {
	t.Parallel()

	const clusterID = "cluster-A"
	xxh3Gen := NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3, clusterID)
	blake3Gen := NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, clusterID)

	// Entry 1 under XXH3.
	_, hash1 := xxh3Gen.Compute(nil, nil, testOrders())
	require.Len(t, hash1, 16)

	// Entry 2 under BLAKE3, chained on hash1.
	_, hash2 := blake3Gen.Compute(nil, hash1, testOrders()[:1])
	require.Len(t, hash2, 32)

	// Re-verifying entry 2 with the same generator + the same lastHash
	// must reproduce hash2 exactly. This is the path the checker walks.
	verifyGen := NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, clusterID)
	_, recomputed := verifyGen.Compute(nil, hash1, testOrders()[:1])
	require.Equal(t, hash2, recomputed)
}
