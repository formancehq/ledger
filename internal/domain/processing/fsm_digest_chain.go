package processing

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// FSMDigestChain is the per-entry chain advance used by the rolling
// cross-node FSM digest persisted under dal.SubGlobFSMDigest.
//
// It wraps a HashGenerator with a reusable scratch buffer so the typical
// steady-state allocation count is zero — the FSM hot path applies a
// chain step at the end of every Raft entry.
//
// Chain semantics: Advance(prevHash, entryOps) = H(entryOps || prevHash)
// (the HashGenerator's canonical serialization). XXH3-128 keyed by
// ClusterID is the algorithm of choice for divergence detection: ~10 GB/s
// on modern CPUs, plenty strong for an in-cluster integrity check, and
// the per-cluster seed prevents accidental collisions across test clusters
// that share image build tags.
type FSMDigestChain struct {
	gen HashGenerator
	buf []byte
}

// NewFSMDigestChain constructs a chain backed by an XXH3-128 hash
// generator seeded from clusterID.
func NewFSMDigestChain(clusterID string) *FSMDigestChain {
	return &FSMDigestChain{
		gen: NewHashGenerator(commonpb.HashAlgorithm_HASH_ALGORITHM_XXH3, clusterID),
		buf: make([]byte, 0, 1024),
	}
}

// Advance returns hash(entryOps || prevHash). The returned slice is
// valid until the next call to Advance — callers that retain it must
// copy. The FSM hot path's WriteSession copies into its persistent
// digestHash buffer immediately after each Advance, so the reuse is
// safe in practice.
func (c *FSMDigestChain) Advance(prevHash, entryOps []byte) []byte {
	buf, hash := c.gen.Compute(c.buf, prevHash, [][]byte{entryOps})
	c.buf = buf

	return hash
}
