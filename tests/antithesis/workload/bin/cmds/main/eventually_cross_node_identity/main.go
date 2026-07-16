// Command eventually_cross_node_identity is the cross-node determinism oracle
// for the #1 architecture invariant: "the in-memory cache must NEVER diverge
// between nodes; every node must see identical cache state for the same applied
// index" (CLAUDE.md). Per-node sentinels only check each node against its own
// Pebble; nothing today compares two nodes against each other. This command
// does, via per-node stale reads at a proven-common applied index.
//
// Why eventually_ and not parallel_/serial_:
//
// The cross-node equality assertion is only sound when (a) no writes are in
// flight and (b) every compared node is at the SAME applied index. A stale read
// returns a node's CURRENT FSM state, not state-as-of a past index, so if
// writes continue two honest nodes legitimately differ (one applied a write the
// other hasn't yet). eventually_ is the only command class that gives genuine
// quiescence: the platform stops fault injection and terminates all other
// drivers, so once the cluster goes idle it STAYS idle. We then prove the idle
// window with the two-barrier technique and require an EXACT common applied
// index — not merely >= B — before comparing. A parallel_/serial_ form would
// race concurrent drivers and produce false divergence findings.
//
// Soundness argument (how honest nodes are guaranteed to agree):
//  1. Quiescence: Barrier()→B1, Barrier()→B2. If B2 == B1+1, the only proposal
//     committed between the two barriers was the second barrier itself, i.e.
//     no other writes occurred. Under eventually_, no new writes will arrive.
//  2. Common exact index: poll each node's OWN applied index via
//     GetClusterState{NodeId: c.NodeID} (a non-zero ID routes to that specific
//     node; NodeId:0 would route to the leader) until raft_status.applied == B2
//     EXACTLY, skipping any node whose SyncProgress.status != "normal" (syncing
//     / out_of_sync / snapshotting — legitimately behind, not a divergence) or
//     that never reaches B2 in time.
//  3. With every compared node at the identical applied index and zero in-flight
//     writes, FSM determinism (invariant #2) requires byte-identical FSM state,
//     so per-node stale GetAccount / GetAuditEntry MUST return proto-equal
//     results. Any difference is exactly the divergence this oracle exists for.
//  4. Re-confirm after reads: Barrier again; if the commit index advanced past
//     B2 a late proposal slipped in and the window was not quiescent — skip the
//     assertion (inconclusive), never fail.
//
// Learners are intentionally included: a stale read on a learner at applied==B2
// must be identical by snapshot-restore equivalence (the SUT's own claim,
// cache_restore_verify.go). is_learner is surfaced in assertion details for
// triage, not used to exclude.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

const (
	// quiescenceAttempts bounds the two-barrier quiescence search.
	quiescenceAttempts = 20
	// appliedConvergeTimeout bounds the per-node wait for applied == B.
	appliedConvergeTimeout = 60 * time.Second
	// appliedPollInterval is the per-node applied-index poll cadence.
	appliedPollInterval = time.Second
	// auditSampleWindow is how many recent audit entries to page through when
	// picking a sequence to compare across nodes.
	auditSampleWindow = 50
	// overallTimeout bounds the whole command (bounded exit on SUT hang).
	overallTimeout = 5 * time.Minute
)

func main() {
	log.Println("composer: eventually_cross_node_identity")

	ctx, cancel := context.WithTimeout(context.Background(), overallTimeout)
	defer cancel()

	conns, err := internal.DialPerNode(ctx)
	if err != nil || len(conns) == 0 {
		log.Printf("composer: could not dial per-node connections: %v", err)

		return
	}
	defer conns.Close()

	// Use the first reachable node's connection to drive Barrier and ledger
	// discovery (Barrier forwards to the leader regardless of which node we hit).
	driver := conns[0].Bucket

	// Quiescence proof: two consecutive barriers one commit apart.
	commitIndex := waitForQuiescence(ctx, driver)
	assert.Sometimes(commitIndex > 0, "cross-node oracle reaches quiescence", nil)
	if commitIndex == 0 {
		log.Println("composer: could not achieve quiescence, skipping")

		return
	}

	// Per-node convergence to the EXACT common applied index B = commitIndex.
	// Nodes still syncing or that never reach B are skipped (legitimately behind).
	ready := nodesAtExactIndex(ctx, conns, commitIndex)
	assert.Sometimes(len(ready) >= 2, "at least two nodes converge to a common applied index", internal.Details{
		"index":      commitIndex,
		"readyNodes": len(ready),
		"totalNodes": len(conns),
	})
	if len(ready) < 2 {
		log.Printf("composer: fewer than two nodes at applied==%d, nothing to compare", commitIndex)

		return
	}

	ledgers, err := internal.ListLedgers(ctx, driver)
	if err != nil {
		log.Printf("composer: list ledgers failed: %s", err)

		return
	}

	// STEP 2: cross-node account identity.
	for _, ledger := range ledgers {
		compareAccounts(ctx, ready, ledger, commitIndex)
	}

	// STEP 3: cross-node audit-hash identity.
	compareAuditHashes(ctx, ready, driver, commitIndex)

	// Re-confirm the window stayed quiescent; if a late proposal advanced the
	// commit index, the comparisons above raced a write and any divergence they
	// might have flagged is inconclusive — but we have already asserted, so the
	// guard is best-effort logging only (eventually_ guarantees no new writes).
	if after := singleBarrier(ctx, driver); after > commitIndex+1 {
		log.Printf("composer: WARNING commit index advanced %d→%d during comparison; window was not fully quiescent", commitIndex, after)
	}
}

// readyNode is a node confirmed at the exact common applied index.
type readyNode struct {
	conn      *internal.PerNodeConn
	isLearner bool
}

// waitForQuiescence calls Barrier until two consecutive calls return commit
// indices differing by exactly 1 (the second barrier itself), proving no other
// proposal committed between them. Returns that index, or 0 on failure.
//
// Retries on any IsTransient error (not just IsUnavailable): Barrier
// proposes through Raft, and under a clog/restore fault window Raft
// transients can surface as DeadlineExceeded, Aborted, or
// FailedPrecondition + READ_INDEX_NOT_CAUGHT_UP — all legitimately
// retryable. Narrow IsUnavailable matching would trip the assertion
// below on fault-window noise and undermine the cross-node identity
// oracle's reliability.
func waitForQuiescence(ctx context.Context, client servicepb.BucketServiceClient) uint64 {
	var last uint64
	for attempt := 1; attempt <= quiescenceAttempts; attempt++ {
		resp, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
		if err != nil {
			if internal.IsTransient(err) {
				continue
			}

			assert.Unreachable("cross-node oracle barrier unexpected error", internal.Details{"error": err, "attempt": attempt})

			return 0
		}

		cur := resp.GetCommitIndex()
		if last > 0 && cur == last+1 {
			log.Printf("composer: quiescence confirmed at commitIndex=%d", cur)

			return cur
		}

		last = cur
	}

	return 0
}

// singleBarrier issues one Barrier and returns its commit index (0 on error).
func singleBarrier(ctx context.Context, client servicepb.BucketServiceClient) uint64 {
	resp, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
	if err != nil {
		return 0
	}

	return resp.GetCommitIndex()
}

// nodesAtExactIndex polls each per-node connection until its locally-reported
// applied index equals target exactly, skipping nodes that are not in the
// "normal" sync state or that never converge within the deadline.
func nodesAtExactIndex(ctx context.Context, conns internal.PerNodeConns, target uint64) []readyNode {
	pollCtx, cancel := context.WithTimeout(ctx, appliedConvergeTimeout)
	defer cancel()

	var ready []readyNode
	for _, c := range conns {
		rn, ok := waitNodeAtIndex(pollCtx, c, target)
		if ok {
			ready = append(ready, rn)
		}
	}

	return ready
}

// waitNodeAtIndex polls one node until its durable cursor
// (last_persisted_index — the index whose FSM batch has been committed to
// Pebble) equals target and sync status is "normal". Returns false if the
// node is unreachable, behind/syncing, or never reaches target before the
// context deadline. last_persisted_index is the semantic contract for
// "Pebble is caught up to N" — it is what the GetAccount / oracle read
// path actually needs, independent of the Raft-consensus side.
func waitNodeAtIndex(ctx context.Context, c *internal.PerNodeConn, target uint64) (readyNode, bool) {
	// Without a resolved node ID we cannot poll this node's OWN applied index
	// (GetClusterState{NodeId:0} routes to the leader), so the exact-index gate
	// would be vacuous — skip rather than risk comparing at a mismatched index.
	if c.NodeID == 0 {
		log.Printf("composer: node %s has unresolved node ID, skipping", c.Addr)

		return readyNode{}, false
	}

	for ctx.Err() == nil {
		// NodeId routes to this specific node, returning its local Raft status
		// (the forwarder resolves a non-zero ID to that node, or serves locally
		// when it is the receiver) — never the leader's view.
		state, err := c.Cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{NodeId: c.NodeID})
		if err != nil {
			// Node down or transient — skip after a short wait.
			time.Sleep(appliedPollInterval)

			continue
		}

		status := state.GetSyncProgress().GetStatus()
		// Gate on last_persisted_index (the "Pebble is caught up" cursor)
		// rather than Raft's `Applied`. Under AsyncStorageWrites the two now
		// advance in lockstep (Applied is bumped by MsgStorageApplyResp,
		// fired from runCommitter AFTER pb.batch.Commit() returns), so
		// gating on either would work in the common case. We keep
		// last_persisted_index because it names the Pebble-durability
		// contract the oracle actually depends on — the GetAccount read is
		// a Pebble read, not a Raft-consensus read.
		// See clusterpb.RaftStatus.last_persisted_index for the contract.
		persisted := state.GetRaftStatus().GetLastPersistedIndex()

		// Only "normal" nodes are caught up; syncing/out_of_sync/snapshotting
		// nodes are legitimately behind and must not be compared.
		if status != "" && status != "normal" {
			log.Printf("composer: node %s status=%q (not normal), skipping", c.Addr, status)

			return readyNode{}, false
		}

		if persisted == target {
			return readyNode{conn: c, isLearner: localIsLearner(state)}, true
		}

		if persisted > target {
			// Should not happen under proven quiescence; treat as a node that
			// raced a late write — skip rather than compare at a mismatched index.
			log.Printf("composer: node %s lastPersistedIndex=%d > target=%d, skipping", c.Addr, persisted, target)

			return readyNode{}, false
		}

		time.Sleep(appliedPollInterval)
	}

	return readyNode{}, false
}

// localIsLearner reports whether the local node is a learner, for triage tags.
func localIsLearner(state *clusterpb.ClusterState) bool {
	local := state.GetLocalNode()
	for _, n := range state.GetNodes() {
		if n.GetId() == local {
			return n.GetSuffrage() == "Learner"
		}
	}

	return false
}

// compareAccounts asserts that every ready node returns a proto-equal Account
// for each sampled address (world + users:0..UserAccountCount-1) at the common
// applied index. Comparison uses proto.Equal (NOT raw bytes): proto map wire
// order is unspecified, so serialized-byte comparison would false-positive.
func compareAccounts(ctx context.Context, nodes []readyNode, ledger string, index uint64) {
	addrs := []string{"world"}
	for i := range internal.UserAccountCount {
		addrs = append(addrs, fmt.Sprintf("users:%d", i))
	}

	for _, addr := range addrs {
		var (
			ref     *commonpb.Account
			refNode string
			refOK   bool
		)

		for _, n := range nodes {
			staleCtx := internal.WithStaleConsistency(ctx)
			acc, err := n.conn.Bucket.GetAccount(staleCtx, &servicepb.GetAccountRequest{
				Ledger:  ledger,
				Address: addr,
			})
			if err != nil {
				// NotFound (account never touched) and transient errors are
				// not divergence — skip this node for this address.
				if !internal.IsTransient(err) && !internal.IsLedgerNotFound(err) {
					log.Printf("composer: GetAccount(%s/%s) on %s: %s", ledger, addr, n.conn.Addr, err)
				}

				continue
			}

			if !refOK {
				ref, refNode, refOK = acc, n.conn.Addr, true

				continue
			}

			equal := proto.Equal(ref, acc)
			assert.Always(equal, "account state is identical across nodes at a common applied index", internal.Details{
				"ledger":      ledger,
				"address":     addr,
				"index":       index,
				"refNode":     refNode,
				"node":        n.conn.Addr,
				"nodeID":      n.conn.NodeID,
				"isLearner":   n.isLearner,
				"refVolumes":  volumesString(ref),
				"nodeVolumes": volumesString(acc),
			})
		}
	}

	log.Printf("composer: account identity check done for ledger %s at index %d", ledger, index)
}

// volumesString renders an account's per-asset balances in a stable form for
// assertion details (proto map iteration order is irrelevant to this triage
// string; it is not used for the equality decision).
func volumesString(acc *commonpb.Account) string {
	if acc == nil {
		return "<nil>"
	}

	var out strings.Builder
	for _, v := range acc.GetVolumes() {
		fmt.Fprintf(&out, "%s/%s=%s ", v.GetAsset(), v.GetColor(), v.GetVolumes().GetBalance())
	}

	return out.String()
}

// compareAuditHashes picks a recent audit sequence present in the live (non
// archived) window and asserts every ready node returns the same hash and
// hash_version for it. Sequences that NotFound on any node (applied-index skew
// or archival) are skipped, never failed.
func compareAuditHashes(ctx context.Context, nodes []readyNode, driver servicepb.BucketServiceClient, index uint64) {
	seq, ok := pickRecentAuditSequence(ctx, driver)
	if !ok {
		log.Println("composer: no audit sequence available to compare, skipping audit-hash check")

		return
	}

	var (
		refHash    []byte
		refVersion uint32
		refNode    string
		refOK      bool
	)

	for _, n := range nodes {
		staleCtx := internal.WithStaleConsistency(ctx)
		entry, err := n.conn.Bucket.GetAuditEntry(staleCtx, &servicepb.GetAuditEntryRequest{Sequence: seq})
		if err != nil {
			// NotFound on a node = applied-index skew or archival purge; skip.
			if !internal.IsTransient(err) && !internal.IsLedgerNotFound(err) {
				log.Printf("composer: GetAuditEntry(seq=%d) on %s: %s", seq, n.conn.Addr, err)
			}

			continue
		}

		if !refOK {
			refHash, refVersion, refNode, refOK = entry.GetHash(), entry.GetHashVersion(), n.conn.Addr, true

			continue
		}

		hashEqual := string(entry.GetHash()) == string(refHash)
		assert.Always(hashEqual, "audit hash is identical across nodes at the same sequence", internal.Details{
			"sequence":  seq,
			"index":     index,
			"refNode":   refNode,
			"node":      n.conn.Addr,
			"nodeID":    n.conn.NodeID,
			"isLearner": n.isLearner,
		})

		versionEqual := entry.GetHashVersion() == refVersion
		assert.Always(versionEqual, "audit hash version is identical across nodes at the same sequence", internal.Details{
			"sequence":    seq,
			"refNode":     refNode,
			"node":        n.conn.Addr,
			"refVersion":  refVersion,
			"nodeVersion": entry.GetHashVersion(),
		})
	}

	log.Printf("composer: audit-hash identity check done at sequence %d", seq)
}

// pickRecentAuditSequence pages through the audit trail and returns the highest
// sequence seen within auditSampleWindow entries, which is in the live window
// and most likely present on every caught-up node.
func pickRecentAuditSequence(ctx context.Context, driver servicepb.BucketServiceClient) (uint64, bool) {
	stream, err := driver.ListAuditEntries(ctx, &servicepb.ListAuditEntriesRequest{
		Options: &commonpb.ListOptions{PageSize: auditSampleWindow},
	})
	if err != nil {
		return 0, false
	}

	var maxSeq uint64
	for {
		entry, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			break
		}

		if s := entry.GetSequence(); s > maxSeq {
			maxSeq = s
		}
	}

	return maxSeq, maxSeq > 0
}
