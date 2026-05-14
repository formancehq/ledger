package processing

import (
	"github.com/zeebo/blake3"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// ComputeLogHash computes a blake3 hash for log chaining.
//
// The log's Hash, HashVersion, Signature, Receipt, and ResponseSignature
// fields are excluded (they are populated after the hash is computed).
// The hasher is reused across calls to avoid per-call allocation.
func ComputeLogHash(hasher *blake3.Hasher, hashBuf []byte, lastHash []byte, log *commonpb.Log) (buf []byte, hash []byte) {
	// Nil out fields that are populated after hashing, then restore.
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

	if hasher == nil {
		h := blake3.Sum256(hashBuf)
		return hashBuf, h[:]
	}

	hasher.Reset()
	_, _ = hasher.Write(hashBuf)

	var h [32]byte
	_, _ = hasher.Digest().Read(h[:])

	return hashBuf, h[:]
}
