package main

import (
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	log.Println("composer: parallel_driver_query_checkpoints")

	ctx, cancel := internal.DriverContext()
	defer cancel()
	conn, err := internal.NewGRPCConn()
	if err != nil {
		log.Printf("error creating connection: %s", err)
		return
	}
	defer conn.Close()

	client := clusterpb.NewClusterServiceClient(conn)

	// 1. Create a query checkpoint.
	createResp, err := client.CreateQueryCheckpoint(ctx, &clusterpb.CreateQueryCheckpointRequest{})
	if err != nil {
		if internal.IsTransient(err) {
			log.Printf("CreateQueryCheckpoint transient: %v", err)
			return
		}

		assert.Unreachable("CreateQueryCheckpoint should not fail",
			internal.Details{"error": err})

		return
	}

	cpID := createResp.GetCheckpointId()
	maxSeq := createResp.GetMaxSequence()
	details := internal.Details{"checkpointId": cpID, "maxSequence": maxSeq}

	assert.Reachable("query checkpoint created", details)

	// 2. List query checkpoints — the one we just created should appear.
	listResp, err := client.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
	if err != nil {
		if internal.IsTransient(err) {
			return
		}

		assert.Unreachable("ListQueryCheckpoints should not fail",
			details.With(internal.Details{"error": err}))

		return
	}

	found := false
	for _, cp := range listResp.GetCheckpoints() {
		if cp.GetCheckpointId() == cpID {
			found = true

			break
		}
	}

	assert.Sometimes(found,
		"created checkpoint should appear in list", details)

	if !found {
		// Leader change between Create and List — the new leader may not
		// have committed the entry yet. Bail out gracefully.
		return
	}

	// 3. Get checkpoint info.
	infoResp, err := client.GetQueryCheckpointInfo(ctx, &clusterpb.GetQueryCheckpointInfoRequest{
		CheckpointId: cpID,
	})
	if err != nil {
		if internal.IsTransient(err) {
			return
		}

		assert.Unreachable("GetQueryCheckpointInfo should not fail",
			details.With(internal.Details{"error": err}))

		return
	}

	assert.AlwaysOrUnreachable(infoResp.GetCheckpointId() == cpID,
		"checkpoint info should match requested ID",
		details.With(internal.Details{"returnedId": infoResp.GetCheckpointId()}))

	assert.AlwaysOrUnreachable(infoResp.GetMaxSequence() == maxSeq,
		"checkpoint max sequence should be consistent",
		details.With(internal.Details{
			"expectedMaxSeq": maxSeq,
			"returnedMaxSeq": infoResp.GetMaxSequence(),
		}))

	// 4. Delete the checkpoint.
	_, err = client.DeleteQueryCheckpoint(ctx, &clusterpb.DeleteQueryCheckpointRequest{
		CheckpointId: cpID,
	})
	if err != nil {
		if internal.IsTransient(err) {
			return
		}

		assert.Unreachable("DeleteQueryCheckpoint should not fail",
			details.With(internal.Details{"error": err}))

		return
	}

	// 5. Verify deletion — should no longer appear in list.
	listAfter, err := client.ListQueryCheckpoints(ctx, &clusterpb.ListQueryCheckpointsRequest{})
	if err != nil {
		internal.LogCleanupError("list query checkpoints after delete", err)
		return
	}

	foundAfterDelete := false
	for _, cp := range listAfter.GetCheckpoints() {
		if cp.GetCheckpointId() == cpID {
			foundAfterDelete = true

			break
		}
	}

	assert.Sometimes(!foundAfterDelete,
		"deleted checkpoint should not appear in list", details)

	assert.Reachable("query checkpoint lifecycle completed", details)
	log.Printf("Query checkpoint lifecycle: created %d (maxSeq=%d), verified, deleted", cpID, maxSeq)
}
