package main

import (
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
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
	defer conn.Close()

	client := clusterpb.NewClusterServiceClient(conn)

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
	var targetID uint32

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

	// 2. Transfer leadership to the target node.
	resp, err := client.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{
		Transferee: targetID,
	})

	assert.Sometimes(internal.IsTolerated(err),
		"should be able to transfer leadership", details.With(internal.Details{"error": err}))
	if err != nil {
		return
	}

	assert.AlwaysOrUnreachable(resp.GetNewLeader() == targetID,
		"new leader should be the requested target",
		details.With(internal.Details{"newLeader": resp.GetNewLeader()}))

	assert.Reachable("leadership transfer completed", details)

	// 3. Verify the cluster is still functional by getting state again.
	//    A concurrent network fault may trigger a new election right after the
	//    transfer, so we retry a few times before giving up.
	var stateAfter *clusterpb.ClusterState
	for attempt := range 5 {
		if attempt > 0 {
			time.Sleep(200 * time.Millisecond)
		}

		stateAfter, err = client.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		if err != nil {
			internal.LogCleanupError("get cluster state after leadership transfer", err)
			return
		}

		if stateAfter.GetLeader() != 0 {
			break
		}
	}

	assert.AlwaysOrUnreachable(stateAfter.GetLeader() != 0,
		"cluster should have a leader after transfer", details)

	log.Printf("Leadership transferred: %d -> %d (confirmed leader: %d)",
		currentLeader, targetID, stateAfter.GetLeader())
}
