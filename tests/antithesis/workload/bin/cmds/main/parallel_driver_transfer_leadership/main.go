package main

import (
	"context"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: parallel_driver_transfer_leadership")

	ctx, cancel := internal.DriverContext()
	defer cancel()
	conn, err := internal.NewGRPCConn()
	if err != nil {
		log.Printf("error creating connection: %s", err)

		return
	}
	defer func() { _ = conn.Close() }()

	runTransfer(ctx, clusterpb.NewClusterServiceClient(conn), servicepb.NewBucketServiceClient(conn))
}

func runTransfer(ctx context.Context, client clusterpb.ClusterServiceClient, bucket servicepb.BucketServiceClient) {
	// 1. Get current cluster state to find leader and a non-leader voter.
	state, err := client.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
	if err != nil {
		if internal.IsTransient(err) {
			return
		}

		assert.Unreachable("GetClusterState should not fail", internal.Details{"error": err})

		return
	}

	currentLeader := state.GetLeader()
	if currentLeader == 0 {
		log.Println("no leader elected, skipping transfer")

		return
	}

	// Pick a non-leader voter as the transfer target.
	var targetID uint64

	for _, node := range state.GetNodes() {
		if node.GetId() != currentLeader && node.GetSuffrage() == "Voter" {
			targetID = node.GetId()

			break
		}
	}

	if targetID == 0 {
		log.Println("no non-leader voter found, skipping transfer")

		return
	}

	details := internal.Details{
		"currentLeader": currentLeader,
		"targetNode":    targetID,
	}

	// Every concurrent invocation owns its transaction, while the stable ledger
	// is excluded from generic writers. A successful transfer must preserve the
	// exact acknowledged business payload, not just return the requested leader.
	ledger := internal.PrefixSentinel.WithSuffix("leadership-transfer")
	if err := internal.CreateLedger(ctx, bucket, ledger); err != nil && !internal.IsAlreadyExists(err) {
		log.Printf("cannot create leadership-transfer witness ledger: %s", err)

		return
	}
	before, err := internal.PreCommitSentinel(ctx, bucket, ledger)
	if err != nil {
		log.Printf("cannot acknowledge pre-transfer witness: %s", err)

		return
	}

	// 2. Transfer leadership to the target node.
	resp, err := client.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
		Transferee: targetID,
	})

	assert.Sometimes(err == nil,
		"should be able to transfer leadership", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	assert.AlwaysOrUnreachable(resp.GetNewLeader() == targetID,
		"new leader should be the requested target",
		details.With(internal.Details{"newLeader": resp.GetNewLeader()}))

	assert.Reachable("leadership transfer completed", details)
	before.Verify(ctx, bucket, "after_leadership_transfer")

	// A leader ID alone does not establish progress: an elected node can still
	// be stuck accepting proposals. Require a fresh acknowledged business write
	// for coverage, then verify its exact content through the ordinary read path.
	// Faults are still active here, so a failed write is an exploration miss,
	// not a claim of broken liveness after recovery.
	after, err := internal.PreCommitSentinel(ctx, bucket, ledger)
	assert.Sometimes(err == nil, "business write commits after leadership transfer", details.With(internal.Details{"error": err}))
	if err != nil {
		log.Printf("post-transfer witness was not acknowledged: %s", err)

		return
	}
	after.Verify(ctx, bucket, "new_write_after_leadership_transfer")

	// Faults and other transfers are still active. A new election can remove
	// the leader immediately after our acknowledged write; record availability
	// coverage without turning that permitted CP outcome into a safety failure.
	stateAfter, err := client.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
	if err != nil {
		internal.LogCleanupError("get cluster state after leadership transfer", err)

		return
	}

	assert.Sometimes(stateAfter.GetLeader() != 0,
		"cluster should have a leader after transfer", details)

	log.Printf("Leadership transferred: %d -> %d (confirmed leader: %d)",
		currentLeader, targetID, stateAfter.GetLeader())
}
