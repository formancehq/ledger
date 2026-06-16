package processing

import (
	"github.com/zeebo/blake3"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// blake3AuditKeyContext domain-separates the BLAKE3 audit-hash key from
// the XXH3 derivation and any future per-cluster keys. The "v1" suffix
// reserves room for key rotation should the derivation ever change.
const blake3AuditKeyContext = "audit-hash:blake3:v1:"

type blake3HashGenerator struct {
	key [32]byte
}

func newBLAKE3HashGenerator(clusterID string) *blake3HashGenerator {
	return &blake3HashGenerator{
		key: blake3.Sum256([]byte(blake3AuditKeyContext + clusterID)),
	}
}

func (g *blake3HashGenerator) Compute(buf []byte, lastHash []byte, slices [][]byte) ([]byte, []byte) {
	buf = serializeAuditPayload(buf, lastHash, slices)

	// NewKeyed only errors on key-length mismatch, impossible here
	// because g.key is a [32]byte and Sum256 always produces 32 bytes.
	hasher, _ := blake3.NewKeyed(g.key[:])
	_, _ = hasher.Write(buf)

	return buf, hasher.Sum(nil)
}

func (g *blake3HashGenerator) Algorithm() commonpb.HashAlgorithm {
	return commonpb.HashAlgorithm_HASH_ALGORITHM_BLAKE3
}
