// Command eventually_fsm_digest_matches asserts the cross-node FSM digest
// invariant: every replica, applying the same Raft entries, advances the
// rolling digest persisted under SubGlobFSMDigest by the same per-entry
// chain steps — the value at the same applied index must therefore match
// across all peers.
//
// This is the runtime oracle for invariant #1 ("the in-memory cache must
// NEVER diverge between nodes") and invariant #2 ("FSM must be deterministic").
// Per-node sentinels check each node against its own Pebble; this command is
// the only one that compares two nodes against each other at the digest level.
//
// Why eventually_ and not parallel_/serial_:
//
// Comparing digests at DIFFERENT applied indices is meaningless (two honest
// nodes may legitimately be at different applied indices in a live cluster).
// eventually_ gives genuine quiescence: the platform stops fault injection
// and terminates all other drivers, so once the cluster goes idle it STAYS
// idle. With the cluster quiesced we can pin a common applied index via
// Barrier and ask every node for GetFSMDigest at that index.
//
// We pin a common applied index via Barrier (a Raft round-trip that returns
// the current commit index after the barrier itself is committed) and ask
// each node for GetFSMDigest(index=<barrier>, wait_ms=long) — the server
// blocks until its FSM has applied the barrier, then returns the persisted
// (appliedIndex, hash) tuple stored under dal.SubGlobFSMDigest. Any node
// that cannot catch up in time is logged and skipped (legitimately behind,
// not a divergence).
//
// Soundness argument:
//  1. Barrier → commitIndex B is a Raft-committed entry; B is identical on
//     every node by Raft consensus.
//  2. The rolling digest is chained per ENTRY in the FSM apply path
//     (Machine.PrepareEntries calls WriteSession.AdvanceDigest after each
//     applyProposal) and persisted atomically with the batch commit
//     (WriteSession.CommitWithRollingDigest writes SubGlobFSMDigest into
//     the same Pebble batch). So at applied index N the persisted
//     (appliedIndex=N, hash=H_N) tuple is coherent by construction — no
//     race between snapshot capture and index reporting.
//  3. The chain advances by ONE link per Raft entry regardless of how Raft
//     groups entries into MsgApp batches: leader (1 entry / batch) and
//     follower (N entries / batch) both produce N chain links over the
//     same N entries, feeding the same per-entry filtered op stream into
//     the same XXH3-128 keyed generator. So H_N matches cross-node by
//     construction, modulo the FSM determinism invariant the oracle is
//     here to test.
//
// Skip cases (do not assert):
//   - The cluster was bootstrapped with fsm_determinism_enabled=false: every
//     node returns FAILED_PRECONDITION; we log once and exit. The feature is
//     off, the oracle is meaningless.
//   - Fewer than 2 nodes converge to the target index within
//     digestConvergeTimeout: not enough peers to compare. assert.Sometimes
//     records the case so the run lookat picks it up if it never converges.
package main

import (
	"bytes"
	"context"
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

const (
	// digestConvergeTimeoutMs is how long each node may take to catch up to
	// the barrier's commit index before we give up on it. Mirrors the
	// applied-convergence budget of eventually_cross_node_identity.
	digestConvergeTimeoutMs = 60_000
	// overallTimeout bounds the whole command (bounded exit on SUT hang).
	overallTimeout = 5 * time.Minute
	// quiescenceAttempts bounds the barrier retry loop on transient errors.
	quiescenceAttempts = 20
)

func main() {
	log.Println("composer: eventually_fsm_digest_matches")

	ctx, cancel := context.WithTimeout(context.Background(), overallTimeout)
	defer cancel()

	conns, err := internal.DialPerNode(ctx)
	if err != nil || len(conns) == 0 {
		log.Printf("composer: could not dial per-node connections: %v", err)

		return
	}
	defer conns.Close()

	// Drive Barrier through the first reachable node; Barrier forwards to the
	// leader regardless of which peer we hit, so any conn works.
	driver := conns[0].Bucket

	commitIndex := barrierWithRetry(ctx, driver)
	assert.Sometimes(commitIndex > 0, "fsm digest oracle reaches a barrier commit index", nil)

	if commitIndex == 0 {
		log.Println("composer: barrier never returned a commit index, skipping")

		return
	}

	type nodeDigest struct {
		conn   *internal.PerNodeConn
		digest *clusterpb.FSMDigest
	}

	var (
		responses       []nodeDigest
		featureDisabled bool
	)

	for _, c := range conns {
		// Stale consistency: the request stays on the receiving node (no
		// leader forwarding), so the digest is attributable to that node.
		// WaitMs lets the server block until its FSM has applied commitIndex.
		req := &clusterpb.GetFSMDigestRequest{
			Index:  commitIndex,
			WaitMs: digestConvergeTimeoutMs,
		}

		d, err := c.Cluster.GetFSMDigest(internal.WithStaleConsistency(ctx), req)
		if err != nil {
			if status.Code(err) == codes.FailedPrecondition {
				// fsm_determinism_enabled=false on this cluster — the oracle
				// is meaningless. Record once and bail.
				featureDisabled = true

				break
			}

			if internal.IsTransient(err) {
				log.Printf("composer: GetFSMDigest(%s) transient: %s", c.Addr, err)

				continue
			}

			if status.Code(err) == codes.DeadlineExceeded {
				log.Printf("composer: node %s did not reach applied=%d within %dms, skipping", c.Addr, commitIndex, digestConvergeTimeoutMs)

				continue
			}

			log.Printf("composer: GetFSMDigest(%s) unexpected error: %s", c.Addr, err)

			continue
		}

		responses = append(responses, nodeDigest{conn: c, digest: d})
	}

	if featureDisabled {
		log.Println("composer: fsm_determinism_enabled=false on cluster, oracle disabled")

		return
	}

	assert.Sometimes(len(responses) >= 2, "at least two nodes return a digest at the common applied index", internal.Details{
		"index":      commitIndex,
		"responses":  len(responses),
		"totalNodes": len(conns),
	})

	if len(responses) < 2 {
		log.Printf("composer: fewer than two nodes responded with a digest at index=%d, nothing to compare", commitIndex)

		return
	}

	ref := responses[0]

	for _, n := range responses[1:] {
		equal := bytes.Equal(ref.digest.GetDigest(), n.digest.GetDigest()) &&
			ref.digest.GetAppliedIndex() == n.digest.GetAppliedIndex() &&
			ref.digest.GetSnapshotIndex() == n.digest.GetSnapshotIndex() &&
			ref.digest.GetHashVersion() == n.digest.GetHashVersion()

		assert.Always(equal, "fsm digest is byte-identical across nodes at a common applied index", internal.Details{
			"index":            commitIndex,
			"refNode":          ref.conn.Addr,
			"refNodeID":        ref.conn.NodeID,
			"refApplied":       ref.digest.GetAppliedIndex(),
			"refSnapshot":      ref.digest.GetSnapshotIndex(),
			"refDigestHex":     toHex(ref.digest.GetDigest()),
			"refHashVersion":   ref.digest.GetHashVersion(),
			"node":             n.conn.Addr,
			"nodeID":           n.conn.NodeID,
			"nodeApplied":      n.digest.GetAppliedIndex(),
			"nodeSnapshot":     n.digest.GetSnapshotIndex(),
			"nodeDigestHex":    toHex(n.digest.GetDigest()),
			"nodeHashVersion":  n.digest.GetHashVersion(),
		})
	}

	// Re-confirm quiescence post-comparison. If the commit index advanced
	// past commitIndex while we were reading, a late proposal slipped in
	// — but eventually_ guarantees no new writes, so this is a logging
	// curiosity, not an assertion.
	if after := singleBarrier(ctx, driver); after > commitIndex+1 {
		log.Printf("composer: WARNING commit index advanced %d→%d during digest comparison; window was not fully quiescent", commitIndex, after)
	}

	log.Printf("composer: fsm digest check done at index %d across %d nodes", commitIndex, len(responses))
}

// barrierWithRetry calls Barrier and returns its commit index, retrying on
// transient errors. Returns 0 if all attempts fail.
//
// Unlike eventually_cross_node_identity's two-barrier quiescence proof, the
// digest oracle does not need to prove a one-commit gap: GetFSMDigest pins
// the index server-side via WaitForApplied, so we just need a single index
// that every node will eventually catch up to.
func barrierWithRetry(ctx context.Context, client servicepb.BucketServiceClient) uint64 {
	for attempt := 1; attempt <= quiescenceAttempts; attempt++ {
		resp, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
		if err != nil {
			if internal.IsTransient(err) {
				continue
			}

			assert.Unreachable("fsm digest oracle barrier unexpected error", internal.Details{"error": err, "attempt": attempt})

			return 0
		}

		return resp.GetCommitIndex()
	}

	return 0
}

// singleBarrier issues one Barrier and returns its commit index (0 on error).
// Used as a best-effort post-comparison quiescence check.
func singleBarrier(ctx context.Context, client servicepb.BucketServiceClient) uint64 {
	resp, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
	if err != nil {
		return 0
	}

	return resp.GetCommitIndex()
}

// toHex renders a digest for assertion details. Digests are ~16-32 bytes;
// the hex form is the right shape for triage logs.
func toHex(b []byte) string {
	const hexdigits = "0123456789abcdef"

	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2] = hexdigits[c>>4]
		out[i*2+1] = hexdigits[c&0x0f]
	}

	return string(out)
}
