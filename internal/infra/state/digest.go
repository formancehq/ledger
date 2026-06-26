package state

import (
	"encoding/binary"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// fsmDigestDomain is the domain-separation prefix mixed into every cross-node
// FSM digest. Bumping the version (e.g. "fsm-digest:v2") invalidates every
// existing persisted digest — only do so when the canonical byte layout
// changes. The prefix prevents an attacker from substituting an audit-chain
// seed into a digest payload (and vice versa) on a HashGenerator that is
// shared between the two paths.
const fsmDigestDomain = "fsm-digest:v1"

// chainFSMDigest computes the next rolling FSM digest:
//
//	digest_n = H_seed( "fsm-digest:v1" || u64(snapshotIndex) || u64(appliedIndex)
//	                   || batch.Repr() || digest_{n-1} )
//
// batch.Repr() is the in-memory Pebble batch's on-wire representation —
// insertion-ordered. The FSM hot path's insertion order is deterministic by
// construction (the doc-block in front of WriteSet.Merge enforces monotonic
// zone+sub-prefix order, see EN-1325; helpers like appendLogs / appendAuditEntries
// iterate sorted slices; AppliedProposal writes a single Pebble entry with a
// deterministically-encoded value). Two nodes applying the same Raft entries
// therefore produce a byte-identical batch.Repr(), which produces an identical
// digest. If a future write violates the insertion-order contract, the digest
// diverges cross-node and the comparison surfaces it — which is exactly the
// signal the digest exists for.
//
// The snapshotIndex term re-anchors the chain after a snapshot install: a
// follower restored from a leader checkpoint at index N starts from
// digest_N read in the restored Pebble (SubGlobFSMDigest is included
// verbatim in the checkpoint). Computing the same anchor on the leader
// at checkpoint-creation time is implicit — the value persisted at that
// moment IS the anchor.
//
// hashBuf is reused across calls (the HashGenerator returns it after
// growing); pass it back on the next call to amortize allocations.
func chainFSMDigest(
	gen processing.HashGenerator,
	hashBuf []byte,
	previousDigest []byte,
	snapshotIndex, appliedIndex uint64,
	batchRepr []byte,
) (resBuf []byte, newDigest []byte) {
	var indexBytes [16]byte

	binary.BigEndian.PutUint64(indexBytes[0:8], snapshotIndex)
	binary.BigEndian.PutUint64(indexBytes[8:16], appliedIndex)

	return gen.Compute(hashBuf, previousDigest, [][]byte{
		[]byte(fsmDigestDomain),
		indexBytes[:],
		batchRepr,
	})
}

// encodeFSMDigestValue lays out the value persisted under
// [ZoneGlobal][SubGlobFSMDigest]: `u64(appliedIndex) || u64(snapshotIndex)
// || digest`. dst is reused.
func encodeFSMDigestValue(dst []byte, appliedIndex, snapshotIndex uint64, digest []byte) []byte {
	dst = dst[:0]
	dst = binary.BigEndian.AppendUint64(dst, appliedIndex)
	dst = binary.BigEndian.AppendUint64(dst, snapshotIndex)
	dst = append(dst, digest...)

	return dst
}

// fsmDigestKey is the Pebble key for the rolling digest. Computed once at
// package init since the prefix never changes.
var fsmDigestKey = []byte{dal.ZoneGlobal, dal.SubGlobFSMDigest}
