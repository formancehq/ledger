package main

import (
	"context"
	"log"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	// Checkpoint mutations are audited writes: they travel as ledger.Request
	// variants through BucketService.Apply. The read RPCs stay on ClusterService.
	bucketClient := servicepb.NewBucketServiceClient(conn)
	runQueryCheckpointDriver(ctx, client, bucketClient)
}

func runQueryCheckpointDriver(ctx context.Context, client clusterpb.ClusterServiceClient, bucketClient servicepb.BucketServiceClient) {
	// 1. Create a query checkpoint.
	cpID, maxSeq, err := actions.CreateQueryCheckpoint(ctx, bucketClient)
	// Observe both outcomes so Antithesis can explore shared-pool saturation.
	capacityReached := status.Code(err) == codes.FailedPrecondition && internal.HasErrorReason(err, domain.ErrReasonCheckpointLimitReached)
	assert.Sometimes(capacityReached, "query checkpoint capacity reached", internal.Details{
		"error": err, "code": status.Code(err).String(), "reason": internal.ErrorReason(err),
	})
	if err != nil {
		// Parallel invocations share a bounded pool of retained checkpoints.
		// Capacity is a definitive business outcome, not a retryable error.
		if capacityReached {
			return
		}
		if internal.IsTransient(err) {
			log.Printf("CreateQueryCheckpoint transient: %v", err)
			return
		}

		assert.Unreachable("CreateQueryCheckpoint should not fail",
			internal.Details{"error": err})

		return
	}

	details := internal.Details{"checkpointId": cpID, "maxSequence": maxSeq}
	// Only an acknowledged ID belongs to this invocation. Read failures must
	// not leave it occupying a slot; never reclaim another driver's checkpoint.
	needsCleanup := true
	defer func() {
		if !needsCleanup {
			return
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cleanupCancel()
		if err := actions.DeleteQueryCheckpoint(cleanupCtx, bucketClient, cpID); err != nil {
			internal.LogCleanupError("delete owned query checkpoint", err)
			if !internal.IsTolerated(err) {
				assert.Unreachable("owned query checkpoint cleanup returned unexpected error",
					details.With(internal.Details{"error": err}))
			}
		}
	}()

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
		// The metadata read can land on a replica whose local view still lags
		// the acknowledged create. The deferred cleanup releases our checkpoint.
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
	// This is the invocation's deletion attempt. Do not start a second logical
	// delete from the defer if its response is ambiguous; retries belong to the
	// client/helper contract, and unexpected delete errors remain visible here.
	needsCleanup = false
	if err := actions.DeleteQueryCheckpoint(ctx, bucketClient, cpID); err != nil {
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
