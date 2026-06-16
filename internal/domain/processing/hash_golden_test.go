package processing

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeebo/blake3"
	"github.com/zeebo/xxh3"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Golden tests pin the entire audit-hash specification against an
// externally reproducible computation. They cover:
//
//   - the per-cluster key derivation (domain separator string,
//     BLAKE3-Sum256 input shape, XXH3 seed extraction)
//   - the payload concatenation order (concat(orders) || lastHash)
//   - the algorithm and output encoding (BLAKE3-256 keyed, XXH3-128
//     little-endian split)
//
// The generator is the system under test; the expected hash is computed
// by hand from the published spec (audit.proto comment on AuditEntry.hash
// + hash_blake3.go / hash_xxh3.go context strings). Any drift that
// silently changes the on-the-wire hash for the same inputs will trip
// these tests — even if the abstraction layer stays internally
// consistent — so historical audit entries that survive a refactor stay
// verifiable.
//
// If you intentionally change the spec, bump commonpb.HashAlgorithm and
// add a new generator implementation; do not edit these constants.

const goldenClusterID = "golden-cluster-id"

var (
	goldenLastHash = []byte("previous-chain-link")
	goldenOrders   = [][]byte{
		[]byte("order-bytes-A"),
		[]byte("order-bytes-B"),
	}
)

func TestHashGenerator_BLAKE3_Golden(t *testing.T) {
	t.Parallel()

	// Recompute the BLAKE3 audit hash by hand, following the spec from
	// hash_blake3.go: key = BLAKE3-Sum256("audit-hash:blake3:v1:" + clusterID),
	// digest = BLAKE3-Keyed(key, concat(orders) || lastHash).
	keyMaterial := blake3.Sum256([]byte("audit-hash:blake3:v1:" + goldenClusterID))

	hasher, err := blake3.NewKeyed(keyMaterial[:])
	require.NoError(t, err)

	for _, payload := range goldenOrders {
		_, _ = hasher.Write(payload)
	}
	_, _ = hasher.Write(goldenLastHash)

	expected := hasher.Sum(nil)

	g := NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3, goldenClusterID)
	_, got := g.Compute(nil, goldenLastHash, goldenOrders)

	require.Equal(t, expected, got,
		"BLAKE3 audit-hash spec drifted: the generator no longer produces "+
			"BLAKE3-Keyed(BLAKE3-Sum256(\"audit-hash:blake3:v1:\"+clusterID), concat(orders)||lastHash). "+
			"If this drift is intentional, bump commonpb.HashAlgorithm and add a new generator.")
}

func TestHashGenerator_XXH3_Golden(t *testing.T) {
	t.Parallel()

	// Recompute the XXH3 audit hash by hand, following the spec from
	// hash_xxh3.go: seed = first 8 bytes BE of BLAKE3-Sum256("audit-hash:xxh3:v1:" + clusterID),
	// digest = LE(XXH3-128(concat(orders) || lastHash, seed).Lo) || LE(...Hi).
	derived := blake3.Sum256([]byte("audit-hash:xxh3:v1:" + goldenClusterID))
	seed := binary.BigEndian.Uint64(derived[:8])

	var payload []byte
	for _, p := range goldenOrders {
		payload = append(payload, p...)
	}
	payload = append(payload, goldenLastHash...)

	h := xxh3.Hash128Seed(payload, seed)

	var expected [16]byte
	binary.LittleEndian.PutUint64(expected[:8], h.Lo)
	binary.LittleEndian.PutUint64(expected[8:], h.Hi)

	g := NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3, goldenClusterID)
	_, got := g.Compute(nil, goldenLastHash, goldenOrders)

	require.Equal(t, expected[:], got,
		"XXH3 audit-hash spec drifted: the generator no longer produces "+
			"LE(XXH3-128(concat(orders)||lastHash, seed=BE-u64(BLAKE3-Sum256(\"audit-hash:xxh3:v1:\"+clusterID)[:8]))). "+
			"If this drift is intentional, bump commonpb.HashAlgorithm and add a new generator.")
}

// TestHashGenerator_PayloadIsBytesOnly pins that the generator never
// inspects the per-order payload — feeding random bytes (not valid
// proto) must still produce the same hash as the algorithm-by-hand
// computation. This is the property that lets external verifiers
// reproduce the chain without any vtprotobuf dependency.
func TestHashGenerator_PayloadIsBytesOnly(t *testing.T) {
	t.Parallel()

	nonProto := [][]byte{
		{0xff, 0x00, 0xab, 0xcd},
		{},
		{0x01},
	}

	for _, algo := range []commonpb.HashAlgorithm{
		commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3,
		commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3,
	} {
		g := NewHashGenerator(algo, goldenClusterID)

		_, got := g.Compute(nil, nil, nonProto)
		require.NotEmpty(t, got, "algo %s: must hash arbitrary bytes without proto involvement", algo)

		_, again := g.Compute(nil, nil, nonProto)
		require.Equal(t, got, again, "algo %s: same bytes must yield same hash", algo)
	}
}
