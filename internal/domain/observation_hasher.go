package domain

import (
	"encoding/binary"

	"github.com/zeebo/blake3"
)

// ObservationHasher builds a deterministic BLAKE3 digest over a set of
// admission-observed values. It is the shared primitive behind both the
// revert-target observation (RevertTargetDigest) and the Numscript inputs
// resolution hash (RecordingStore.Hash): both bind what admission read into
// OrderTechnical so the FSM can detect a divergence before touching dependent
// state.
//
// The encoding is length-delimited and count-prefixed throughout, making it
// injective over the values it is given: arbitrary byte fields (account names,
// asset names, metadata values) cannot produce collisions by embedding
// separator bytes.
//
// Every Write call is intentionally unchecked: blake3.Hasher satisfies
// hash.Hash, whose Write never returns an error, and it writes to memory with
// no capacity bound. Checking would add a branch that cannot be taken and
// cannot be tested.
type ObservationHasher struct {
	h *blake3.Hasher
}

// NewObservationHasher returns a fresh hasher.
func NewObservationHasher() ObservationHasher {
	return ObservationHasher{h: blake3.New()}
}

// WriteField appends a length-prefixed byte slice.
func (o ObservationHasher) WriteField(b []byte) {
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(b)))
	_, _ = o.h.Write(lenBuf[:n])
	_, _ = o.h.Write(b)
}

// WriteStringField appends a length-prefixed string without allocating.
func (o ObservationHasher) WriteStringField(s string) {
	var lenBuf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(lenBuf[:], uint64(len(s)))
	_, _ = o.h.Write(lenBuf[:n])
	_, _ = o.h.WriteString(s)
}

// WriteCount appends a uvarint-encoded count — use before a repeated section
// to make it count-prefixed and position-independent.
func (o ObservationHasher) WriteCount(n int) {
	var cntBuf [binary.MaxVarintLen64]byte
	written := binary.PutUvarint(cntBuf[:], uint64(n))
	_, _ = o.h.Write(cntBuf[:written])
}

// WriteRaw appends raw bytes without a length prefix — use for fixed-size
// blocks (e.g. a 32-byte amount limb encoding) that need no framing.
func (o ObservationHasher) WriteRaw(b []byte) {
	_, _ = o.h.Write(b)
}

// Sum returns the digest. The hasher can still be written to after this call.
func (o ObservationHasher) Sum() []byte {
	return o.h.Sum(nil)
}
