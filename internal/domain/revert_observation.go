package domain

import (
	"encoding/binary"

	"github.com/zeebo/blake3"

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
// The encoding is length-delimited and count-prefixed so it is injective.
// Account names, assets and colors are arbitrary caller bytes, so plain
// separators would let a crafted posting set collide with a different one and
// evade detection. Posting order is preserved rather than sorted: it is the
// stored order both sides read, and reordering is itself a divergence.
func RevertTargetDigest(postings []*commonpb.Posting, found bool) []byte {
	h := blake3.New()

	// Every h.Write below is intentionally unchecked: blake3.Hasher satisfies
	// hash.Hash, whose Write never returns an error, and it writes to memory
	// with no capacity bound. Checking would add a branch that cannot be taken
	// and cannot be tested.
	writeField := func(b []byte) {
		var lenBuf [binary.MaxVarintLen64]byte

		n := binary.PutUvarint(lenBuf[:], uint64(len(b)))
		_, _ = h.Write(lenBuf[:n])
		_, _ = h.Write(b)
	}

	if !found {
		writeField([]byte("absent"))

		return h.Sum(nil)
	}

	writeField([]byte("present"))

	var cntBuf [binary.MaxVarintLen64]byte

	n := binary.PutUvarint(cntBuf[:], uint64(len(postings)))
	_, _ = h.Write(cntBuf[:n])

	for _, p := range postings {
		writeField([]byte(p.GetSource()))
		writeField([]byte(p.GetDestination()))
		writeField([]byte(p.GetAsset()))
		writeField([]byte(p.GetColor()))
		writeField(p.GetAmount().ToBigInt().Bytes())
	}

	return h.Sum(nil)
}
