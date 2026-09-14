package main

import (
	"context"
	"fmt"

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
// exclusive model template has no other writers and installs only a non-firing
// schedule. Resetting that schedule before observing the registry establishes
// its startup boundary. An arbitrary active scheduler is not supported: disabling
// it cannot cancel an already proposed creation. Historical logs are trusted
// only as setup state; subsequent effects are predicted.
func setupQueryCheckpoints(ctx context.Context, bucket servicepb.BucketServiceClient, cluster clusterpb.ClusterServiceClient, c *Checker) bool {
	ctx = metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	bulk := oracle.Bulk{Requests: []*servicepb.Request{{Type: &servicepb.Request_DeleteQueryCheckpointSchedule{DeleteQueryCheckpointSchedule: &servicepb.DeleteQueryCheckpointScheduleRequest{}}}}}
	response, err := bucket.Apply(ctx, applyRequest(bulk))
	if err == nil {
		logs := response.GetLogs()
		if len(logs) != 1 || logs[0].GetSequence() == 0 || logs[0].GetPayload().GetDeleteQueryCheckpointSchedule() == nil {
			err = fmt.Errorf("schedule reset returned no committed deletion log")
		} else {
			var listed *clusterpb.ListQueryCheckpointsResponse
			listed, err = readCheckpointRegistry(ctx, bucket, cluster, c.ledgerNames[0])
			if err == nil {
				var ids []uint64
				var nextID uint64
				ids, nextID, err = checkpointBaseline(listed, logs[0].GetSequence()-1, func(sequence uint64) (*commonpb.Log, error) {
					return bucket.GetLog(ctx, &servicepb.GetLogRequest{Sequence: sequence})
				})
				if err == nil {
					c.modelState = c.modelState.SeedQueryCheckpoints(ids, nextID)
					return true
				}
			}
		}
	}
	if isShutdownError(err) {
		return false
	}
	assert.Unreachable("singleton_driver_model: checkpoint setup failed", internal.Details{"error": err.Error()})
	return false
}

// checkpointBaseline scans backwards because the largest live ID need not be
// the last allocated ID. A deleted last checkpoint still advances the counter.
func checkpointBaseline(listed *clusterpb.ListQueryCheckpointsResponse, sequence uint64, read func(uint64) (*commonpb.Log, error)) ([]uint64, uint64, error) {
	ids := make([]uint64, 0, len(listed.GetCheckpoints()))
	for _, cp := range listed.GetCheckpoints() {
		ids = append(ids, cp.GetCheckpointId())
	}
	nextID := uint64(1)
	for ; sequence > 0; sequence-- {
		entry, err := read(sequence)
		if err != nil {
			return nil, 0, fmt.Errorf("reading checkpoint baseline log %d: %w", sequence, err)
		}
		if entry == nil || entry.GetSequence() != sequence {
			return nil, 0, fmt.Errorf("checkpoint baseline log %d has wrong sequence", sequence)
		}
		if cp := entry.GetPayload().GetCreatedQueryCheckpoint(); cp != nil {
			if cp.GetCheckpointId() == 0 || cp.GetCheckpointId() == ^uint64(0) {
				return nil, 0, fmt.Errorf("checkpoint baseline has invalid ID %d", cp.GetCheckpointId())
			}
			nextID = cp.GetCheckpointId() + 1
			break
		}
	}
	seen := map[uint64]bool{}
	for _, id := range ids {
		if id == 0 || id >= nextID || seen[id] {
			return nil, 0, fmt.Errorf("invalid checkpoint baseline registry ID %d with next ID %d", id, nextID)
		}
		seen[id] = true
	}
	return ids, nextID, nil
}
