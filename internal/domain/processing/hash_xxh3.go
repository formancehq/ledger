package processing

import (
	"encoding/binary"

	"github.com/zeebo/blake3"
	"github.com/zeebo/xxh3"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// xxh3AuditKeyContext domain-separates the XXH3 audit-hash seed from
// any other use of BLAKE3-Sum256 keyed by clusterID elsewhere in the
// codebase. The "v1" suffix reserves room for a key-rotation bump
// should the derivation ever change.
const xxh3AuditKeyContext = "audit-hash:xxh3:v1:"

type xxh3HashGenerator struct {
	seed uint64
}

func newXXH3HashGenerator(clusterID string) *xxh3HashGenerator {
	derived := blake3.Sum256([]byte(xxh3AuditKeyContext + clusterID))

	return &xxh3HashGenerator{
		seed: binary.BigEndian.Uint64(derived[:8]),
	}
}

func (g *xxh3HashGenerator) Compute(buf []byte, lastHash []byte, orders []*raftcmdpb.Order) ([]byte, []byte) {
	buf = serializeAuditPayload(buf, lastHash, orders)

	h := xxh3.Hash128Seed(buf, g.seed)

	var out [16]byte
	binary.LittleEndian.PutUint64(out[:8], h.Lo)
	binary.LittleEndian.PutUint64(out[8:], h.Hi)

	return buf, out[:]
}

func (g *xxh3HashGenerator) Algorithm() commonpb.HashAlgorithm {
	return commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3
}
