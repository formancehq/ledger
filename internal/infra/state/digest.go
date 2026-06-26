package state

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

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

// bufferedOp is one (kind, key, value) record produced by
// dal.WriteSession.IterateBufferedOps. A reusable slice of bufferedOp is
// pooled on the Machine to avoid per-batch allocations on the digest path.
type bufferedOp struct {
	kind  uint8
	key   []byte
	value []byte
}

// canonicalBatchPayload streams the WriteSession's buffered ops into the
// supplied byte slice in canonical order (sorted by key then by op kind)
// using the wire format
// `kind(1B) || uvarint(len(key)) || key || uvarint(len(value)) || value`.
//
// The Pebble batch is insertion-ordered (legitimate apply paths may emit
// ops in different per-node orders — map iteration, etc.), so a canonical
// sort is mandatory before hashing: two nodes must produce the same byte
// stream for the same logical write set. dst is reused across calls; the
// returned slice may alias dst's backing array.
func canonicalBatchPayload(
	dst []byte,
	ops []bufferedOp,
	session *dal.WriteSession,
) ([]byte, []bufferedOp, error) {
	ops = ops[:0]

	err := session.IterateBufferedOps(func(kind uint8, key, value []byte) error {
		ops = append(ops, bufferedOp{kind: kind, key: key, value: value})

		return nil
	})
	if err != nil {
		return nil, ops, fmt.Errorf("iterating batch ops: %w", err)
	}

	sort.Slice(ops, func(a, b int) bool {
		if cmp := bytes.Compare(ops[a].key, ops[b].key); cmp != 0 {
			return cmp < 0
		}

		return ops[a].kind < ops[b].kind
	})

	dst = dst[:0]

	var lenBuf [binary.MaxVarintLen64]byte

	for i := range ops {
		dst = append(dst, ops[i].kind)
		n := binary.PutUvarint(lenBuf[:], uint64(len(ops[i].key)))
		dst = append(dst, lenBuf[:n]...)
		dst = append(dst, ops[i].key...)
		n = binary.PutUvarint(lenBuf[:], uint64(len(ops[i].value)))
		dst = append(dst, lenBuf[:n]...)
		dst = append(dst, ops[i].value...)
	}

	return dst, ops, nil
}

// chainFSMDigest computes the next rolling FSM digest:
//
//	digest_n = H_seed( "fsm-digest:v1" || u64(snapshotIndex) || u64(appliedIndex)
//	                   || canonicalBatchPayload || digest_{n-1} )
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
	canonicalPayload []byte,
) (resBuf []byte, newDigest []byte) {
	var indexBytes [16]byte

	binary.BigEndian.PutUint64(indexBytes[0:8], snapshotIndex)
	binary.BigEndian.PutUint64(indexBytes[8:16], appliedIndex)

	return gen.Compute(hashBuf, previousDigest, [][]byte{
		[]byte(fsmDigestDomain),
		indexBytes[:],
		canonicalPayload,
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
