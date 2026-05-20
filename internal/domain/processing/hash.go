package processing

import (
	"encoding/binary"

	"github.com/zeebo/blake3"
	"github.com/zeebo/xxh3"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/protowireutil"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// computeLogHash computes a hash for log chaining using the specified algorithm.
//
// The log's Hash, HashVersion, Signature, Receipt, and ResponseSignature
// fields are excluded (they are populated after the hash is computed).
// The blake3Hasher is reused across calls to avoid per-call allocation (may be nil).
//
// logBytesLen is the length of the deterministic log marshal within buf
// (before lastHash is appended). Callers can use buf[:logBytesLen] as a
// pre-marshaled base for Pebble persistence via AppendLogFieldsForPersist.
func computeLogHash(algo commonpb.HashAlgorithm, blake3Hasher *blake3.Hasher, hashBuf []byte, lastHash []byte, log *commonpb.Log) (buf []byte, hash []byte, logBytesLen int) {
	hashBuf, logBytesLen = serializeLogForHash(hashBuf, lastHash, log)
	log.HashVersion = uint32(algo)

	switch algo {
	case commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3:
		return hashBuf, computeXXH3(hashBuf), logBytesLen
	default:
		return hashBuf, computeBLAKE3(blake3Hasher, hashBuf), logBytesLen
	}
}

// ComputeLogHashByVersion computes a hash using the algorithm indicated by hash_version.
// Used by the checker to verify existing logs.
func ComputeLogHashByVersion(hashVersion uint32, hashBuf []byte, lastHash []byte, log *commonpb.Log) (buf []byte, hash []byte) {
	hashBuf, _ = serializeLogForHash(hashBuf, lastHash, log)

	switch commonpb.HashAlgorithm(hashVersion) {
	case commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3:
		return hashBuf, computeXXH3(hashBuf)
	default:
		return hashBuf, computeBLAKE3(nil, hashBuf)
	}
}

// serializeLogForHash produces the deterministic byte representation for hashing.
// Returns the buffer (with lastHash appended) and the length of the log bytes
// before lastHash was appended.
func serializeLogForHash(hashBuf []byte, lastHash []byte, log *commonpb.Log) ([]byte, int) {
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
	logBytesLen := len(hashBuf)

	if len(lastHash) > 0 {
		hashBuf = append(hashBuf, lastHash...)
	}

	log.Hash = savedHash
	log.HashVersion = savedHashVersion
	log.Signature = savedSig
	log.Receipt = savedReceipt
	log.ResponseSignature = savedRespSig

	return hashBuf, logBytesLen
}

// AppendLogFieldsForPersist appends the hash, hash_version, and optional
// signature fields to deterministic log base bytes, producing a complete
// protobuf-encoded Log suitable for Pebble storage. This avoids a second
// full marshal of the entire Log message.
//
// The base bytes must come from MarshalDeterministicVT with Hash, HashVersion,
// Signature, Receipt, and ResponseSignature zeroed (as produced by
// serializeLogForHash). These fields are then appended here.
func AppendLogFieldsForPersist(base []byte, log *commonpb.Log) []byte {
	// Field 4: hash (bytes)
	if h := log.GetHash(); len(h) > 0 {
		base = protowireutil.AppendBytes(base, 4, h)
	}

	// Field 8: hash_version (uint32, varint)
	if hv := log.GetHashVersion(); hv != 0 {
		base = protowireutil.AppendVarint(base, 8, uint64(hv))
	}

	// Field 5: signature (embedded message)
	if sig := log.GetSignature(); sig != nil {
		base = protowireutil.AppendMessage(base, 5, sig)
	}

	// Fields 6 (receipt) and 7 (response_signature) are empty/nil at
	// persist time in the FSM hot path — proto3 omits them.

	return base
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
