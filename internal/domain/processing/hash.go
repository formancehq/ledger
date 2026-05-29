package processing

import (
	"encoding/binary"

	"github.com/zeebo/blake3"
	"github.com/zeebo/xxh3"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// ComputeAuditHash computes a batch-level integrity hash over the proposal's
// orders. Orders are the source of truth — logs are a deterministic derivation.
// The hash covers what was attempted regardless of success or failure.
func ComputeAuditHash(algo commonpb.HashAlgorithm, buf []byte, lastAuditHash []byte, orders []*raftcmdpb.Order) (resBuf []byte, hash []byte) {
	buf = buf[:0]
	for _, order := range orders {
		buf = order.MarshalDeterministicVT(buf)
	}

	if len(lastAuditHash) > 0 {
		buf = append(buf, lastAuditHash...)
	}

	switch algo {
	case commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3:
		return buf, computeXXH3(buf)
	default:
		return buf, computeBLAKE3(buf)
	}
}

// ComputeAuditHashByVersion computes an audit hash using the algorithm indicated
// by hash_version. Used by the checker to verify existing audit entries.
func ComputeAuditHashByVersion(hashVersion uint32, buf []byte, lastAuditHash []byte, orders []*raftcmdpb.Order) (resBuf []byte, hash []byte) {
	buf = buf[:0]
	for _, order := range orders {
		buf = order.MarshalDeterministicVT(buf)
	}

	if len(lastAuditHash) > 0 {
		buf = append(buf, lastAuditHash...)
	}

	switch commonpb.HashAlgorithm(hashVersion) {
	case commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3:
		return buf, computeXXH3(buf)
	default:
		return buf, computeBLAKE3(buf)
	}
}

func computeBLAKE3(data []byte) []byte {
	h := blake3.Sum256(data)

	return h[:]
}

func computeXXH3(data []byte) []byte {
	h := xxh3.Hash128(data)

	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[:8], h.Lo)
	binary.LittleEndian.PutUint64(buf[8:], h.Hi)

	return buf[:]
}
