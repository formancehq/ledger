package domain

import (
	"encoding/binary"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// RevertTargetDigest binds what a producer observed of a revert's target
// transaction when it derived that order's volume coverage.
//
// Admission resolves a revert's original postings from the local store with no
// read barrier, so a target that is committed but not yet applied here reads as
// absent and the order declares no volume keys. Apply then reads the real
// postings through the coverage gate and touches volumes the plan never
// declared. Binding the observation lets apply detect that divergence and reject
// the order before the gate fires, so a coverage miss keeps meaning what it is
// documented to mean: an admission bug.
//
// found distinguishes "looked and saw no transaction" from "saw these postings",
// and is part of the digest: an absent observation must not collide with a
// present one, whatever the postings are.
//
// The encoding is length-delimited and count-prefixed, so it is injective over
// the field *values* it is given. Account names, assets and colors are arbitrary
// caller bytes, so plain separators would let a crafted posting set collide with
// a different one and evade detection. Posting order is preserved rather than
// sorted: it is the stored order both sides read, and reordering is itself a
// divergence.
//
// Amounts are hashed as their four limbs, a fixed 32-byte little-endian block,
// so they need no length prefix and cost no allocation on the apply path. The
// limbs are read through the nil-safe getters, so a nil amount digests exactly
// like an explicit zero. That is deliberate, not a gap in the injectivity
// argument: the two sides read the target from different places (admission
// from the Transaction attribute in the store, apply from the cache), and nil
// versus zero is a representation detail of one amount — it changes neither the
// reversed posting nor the volume coverage. Adding a presence byte would turn
// that representation difference into a spurious stale-observation rejection.
func RevertTargetDigest(postings []*commonpb.Posting, found bool) []byte {
	h := NewObservationHasher()

	if !found {
		h.WriteField([]byte("absent"))

		return h.Sum()
	}

	h.WriteField([]byte("present"))
	h.WriteCount(len(postings))

	for _, p := range postings {
		h.WriteField([]byte(p.GetSource()))
		h.WriteField([]byte(p.GetDestination()))
		h.WriteField([]byte(p.GetAsset()))
		h.WriteField([]byte(p.GetColor()))

		amount := p.GetAmount()

		var limbs [32]byte
		binary.LittleEndian.PutUint64(limbs[0:8], amount.GetV0())
		binary.LittleEndian.PutUint64(limbs[8:16], amount.GetV1())
		binary.LittleEndian.PutUint64(limbs[16:24], amount.GetV2())
		binary.LittleEndian.PutUint64(limbs[24:32], amount.GetV3())
		h.WriteRaw(limbs[:])
	}

	return h.Sum()
}
