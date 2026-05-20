package processing

import (
	"encoding/binary"

	"github.com/zeebo/blake3"
	"github.com/zeebo/xxh3"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// ComputeAuditHash computes a batch-level integrity hash for an audit entry.
// It deterministically marshals each log, concatenates the bytes, appends the
// previous audit hash for chaining, and hashes the result.
// The hasher is reused across calls to avoid per-call allocation (may be nil).
func ComputeAuditHash(algo commonpb.HashAlgorithm, hasher *blake3.Hasher, buf []byte, lastAuditHash []byte, logs []*commonpb.Log) (resBuf []byte, hash []byte) {
	buf = buf[:0]
	for _, log := range logs {
		buf = log.MarshalDeterministicVT(buf)
	}

	if len(lastAuditHash) > 0 {
		buf = append(buf, lastAuditHash...)
	}

	switch algo {
	case commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3:
		return buf, computeXXH3(buf)
	default:
		return buf, computeBLAKE3(hasher, buf)
	}
}

// ComputeAuditHashForFailure computes an integrity hash for a failure audit entry.
// Since failure entries produce no logs, the hash covers the serialized audit entry
// itself (with hash/hash_version zeroed) chained to the previous audit hash.
func ComputeAuditHashForFailure(algo commonpb.HashAlgorithm, hasher *blake3.Hasher, buf []byte, lastAuditHash []byte, entry *auditpb.AuditEntry) (resBuf []byte, hash []byte) {
	savedHash := entry.GetHash()
	savedHashVersion := entry.GetHashVersion()
	entry.Hash = nil
	entry.HashVersion = 0

	buf = entry.MarshalDeterministicVT(buf[:0])

	entry.Hash = savedHash
	entry.HashVersion = savedHashVersion

	if len(lastAuditHash) > 0 {
		buf = append(buf, lastAuditHash...)
	}

	switch algo {
	case commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3:
		return buf, computeXXH3(buf)
	default:
		return buf, computeBLAKE3(hasher, buf)
	}
}

// ComputeAuditHashByVersion computes an audit hash using the algorithm indicated
// by hash_version. Used by the checker to verify existing audit entries.
func ComputeAuditHashByVersion(hashVersion uint32, buf []byte, lastAuditHash []byte, logs []*commonpb.Log) (resBuf []byte, hash []byte) {
	buf = buf[:0]
	for _, log := range logs {
		buf = log.MarshalDeterministicVT(buf)
	}

	if len(lastAuditHash) > 0 {
		buf = append(buf, lastAuditHash...)
	}

	switch commonpb.HashAlgorithm(hashVersion) {
	case commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3:
		return buf, computeXXH3(buf)
	default:
		return buf, computeBLAKE3(nil, buf)
	}
}

// ComputeAuditHashForFailureByVersion computes a failure audit hash using the
// algorithm indicated by hash_version. Used by the checker.
func ComputeAuditHashForFailureByVersion(hashVersion uint32, buf []byte, lastAuditHash []byte, entry *auditpb.AuditEntry) (resBuf []byte, hash []byte) {
	savedHash := entry.GetHash()
	savedHashVersion := entry.GetHashVersion()
	entry.Hash = nil
	entry.HashVersion = 0

	buf = entry.MarshalDeterministicVT(buf[:0])

	entry.Hash = savedHash
	entry.HashVersion = savedHashVersion

	if len(lastAuditHash) > 0 {
		buf = append(buf, lastAuditHash...)
	}

	switch commonpb.HashAlgorithm(hashVersion) {
	case commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3:
		return buf, computeXXH3(buf)
	default:
		return buf, computeBLAKE3(nil, buf)
	}
}

func computeBLAKE3(hasher *blake3.Hasher, data []byte) []byte {
	if hasher == nil {
		h := blake3.Sum256(data)

		return h[:]
	}

	hasher.Reset()
	_, _ = hasher.Write(data)

	var h [32]byte
	_, _ = hasher.Digest().Read(h[:])

	return h[:]
}

func computeXXH3(data []byte) []byte {
	h := xxh3.Hash128(data)

	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[:8], h.Lo)
	binary.LittleEndian.PutUint64(buf[8:], h.Hi)

	return buf[:]
}
