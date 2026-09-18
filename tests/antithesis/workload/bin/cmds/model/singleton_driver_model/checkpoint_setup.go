package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// setupQueryCheckpoints establishes the inherited global lifecycle baseline.
// Unlike ledgers, checkpoint IDs cannot be isolated by the per-run prefix. The
// exclusive model template has no other writers and installs only non-firing
// schedules. Setup resets the schedule, frees one slot when the registry is at
// capacity, then creates a probe checkpoint. Its assigned ID gives the next-ID
// frontier in constant RPC count, even after arbitrarily long prior runs.
func setupQueryCheckpoints(ctx context.Context, node *internal.PerNodeConn, c *Checker) bool {
	bucket := node.Bucket
	ctx = metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	scheduleLog, err := applyCheckpointSetup(ctx, bucket, &servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpointSchedule{DeleteQueryCheckpointSchedule: &servicepb.DeleteQueryCheckpointScheduleRequest{}}})
	if err != nil {
		return checkpointSetupFailure(err)
	}
	if scheduleLog.GetPayload().GetDeleteQueryCheckpointSchedule() == nil {
		return checkpointSetupFailure(fmt.Errorf("checkpoint schedule reset returned the wrong log"))
	}

	listed, err := readCheckpointRegistry(ctx, node)
	if err != nil {
		return checkpointSetupFailure(err)
	}

	limit := c.modelState.QueryCheckpointLimit()
	if limit != 0 && uint64(len(listed.GetCheckpoints())) >= limit {
		victim := listed.GetCheckpoints()[0].GetCheckpointId()
		log, deleteErr := applyCheckpointSetup(ctx, bucket, &servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &servicepb.DeleteQueryCheckpointRequest{CheckpointId: victim}}})
		if deleteErr != nil {
			return checkpointSetupFailure(deleteErr)
		}
		if log.GetPayload().GetDeletedQueryCheckpoint().GetCheckpointId() != victim {
			return checkpointSetupFailure(fmt.Errorf("checkpoint baseline deletion returned the wrong log"))
		}
		listed.Checkpoints = slices.DeleteFunc(listed.GetCheckpoints(), func(cp *clusterpb.QueryCheckpointInfo) bool {
			return cp.GetCheckpointId() == victim
		})
	}

	log, err := applyCheckpointSetup(ctx, bucket, &servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}})
	if err != nil {
		return checkpointSetupFailure(err)
	}
	created := log.GetPayload().GetCreatedQueryCheckpoint()
	if created == nil || created.GetCheckpointId() == 0 || created.GetCheckpointId() == ^uint64(0) || created.GetMaxSequence() != log.GetSequence()-1 {
		return checkpointSetupFailure(fmt.Errorf("checkpoint baseline creation returned an invalid log"))
	}

	ids, nextID, err := checkpointBaseline(listed, created.GetCheckpointId())
	if err != nil {
		return checkpointSetupFailure(err)
	}
	c.modelState = c.modelState.SeedQueryCheckpoints(ids, nextID)
	c.checkpoints[created.GetCheckpointId()] = checkpointSnapshot{state: c.modelState, maxSequence: created.GetMaxSequence()}

	return true
}

func applyCheckpointSetup(ctx context.Context, bucket servicepb.BucketServiceClient, request *servicepb.Request) (*commonpb.Log, error) {
	response, err := bucket.Apply(ctx, applyRequest(oracle.Bulk{Requests: []*servicepb.Request{request}}))
	if err != nil {
		return nil, err
	}
	logs := response.GetLogs()
	if len(logs) != 1 || logs[0].GetSequence() == 0 {
		return nil, fmt.Errorf("checkpoint setup returned no committed log")
	}

	return logs[0], nil
}

func checkpointSetupFailure(err error) bool {
	if internal.IsTransient(err) || isShutdownError(err) {
		return false
	}
	assert.Unreachable("singleton_driver_model: checkpoint setup failed", internal.Details{"error": err.Error()})

	return false
}

// checkpointBaseline combines the current registry with the setup probe. The
// probe is the latest allocation, so it establishes the next ID without walking
// the global log history from the previous invocation.
func checkpointBaseline(listed *clusterpb.ListQueryCheckpointsResponse, probeID uint64) ([]uint64, uint64, error) {
	if probeID == 0 || probeID == ^uint64(0) {
		return nil, 0, fmt.Errorf("checkpoint baseline has invalid probe ID %d", probeID)
	}
	ids := make([]uint64, 0, len(listed.GetCheckpoints())+1)
	seen := map[uint64]bool{}
	for _, cp := range listed.GetCheckpoints() {
		id := cp.GetCheckpointId()
		if id == 0 || id >= probeID || seen[id] {
			return nil, 0, fmt.Errorf("invalid checkpoint baseline registry ID %d with probe ID %d", id, probeID)
		}
		seen[id] = true
		ids = append(ids, id)
	}
	ids = append(ids, probeID)
	slices.Sort(ids)

	return ids, probeID + 1, nil
}
