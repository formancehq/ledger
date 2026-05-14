package processing

import (
	"encoding/binary"

	"github.com/zeebo/blake3"
	"github.com/zeebo/xxh3"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// computeLogHash computes a hash for log chaining using the specified algorithm.
//
// The log's Hash, HashVersion, Signature, Receipt, and ResponseSignature
// fields are excluded (they are populated after the hash is computed).
// The blake3Hasher is reused across calls to avoid per-call allocation (may be nil).
func computeLogHash(algo commonpb.HashAlgorithm, blake3Hasher *blake3.Hasher, hashBuf []byte, lastHash []byte, log *commonpb.Log) (buf []byte, hash []byte) {
	hashBuf = serializeLogForHash(hashBuf, lastHash, log)
	log.HashVersion = uint32(algo)

	switch algo {
	case commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3:
		return hashBuf, computeXXH3(hashBuf)
	default:
		return hashBuf, computeBLAKE3(blake3Hasher, hashBuf)
	}
}

// ComputeLogHashByVersion computes a hash using the algorithm indicated by hash_version.
// Used by the checker to verify existing logs.
func ComputeLogHashByVersion(hashVersion uint32, hashBuf []byte, lastHash []byte, log *commonpb.Log) (buf []byte, hash []byte) {
	hashBuf = serializeLogForHash(hashBuf, lastHash, log)

	switch commonpb.HashAlgorithm(hashVersion) {
	case commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3:
		return hashBuf, computeXXH3(hashBuf)
	default:
		return hashBuf, computeBLAKE3(nil, hashBuf)
	}
}

// serializeLogForHash produces the deterministic byte representation for hashing.
func serializeLogForHash(hashBuf []byte, lastHash []byte, log *commonpb.Log) []byte {
	savedHash := log.GetHash()
	savedHashVersion := log.GetHashVersion()
	savedSig := log.GetSignature()
	savedReceipt := log.GetReceipt()
	savedRespSig := log.GetResponseSignature()

	log.Hash = nil
	log.HashVersion = 0
	log.Signature = nil
	log.Receipt = ""
	log.ResponseSignature = nil

	hashBuf = log.MarshalDeterministicVT(hashBuf[:0])
	if len(lastHash) > 0 {
		hashBuf = append(hashBuf, lastHash...)
	}

	log.Hash = savedHash
	log.HashVersion = savedHashVersion
	log.Signature = savedSig
	log.Receipt = savedReceipt
	log.ResponseSignature = savedRespSig

	return hashBuf
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
