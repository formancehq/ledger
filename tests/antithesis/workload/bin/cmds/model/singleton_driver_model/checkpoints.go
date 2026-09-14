package main

import (
	"github.com/antithesishq/antithesis-sdk-go/random"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// The model template owns its cluster's checkpoint timeline. This must match
// the server's query-checkpoint-limit configuration (10 by default).
const defaultModelCheckpointLimit = 10

// A 100-year interval exercises configuration without introducing scheduler
// writes outside the model driver's observed Apply stream during a test run.
const modelCheckpointCron = "@every 876000h"

type checkpointSnapshot struct {
	state       oracle.GlobalState
	maxSequence uint64
}

// generateCheckpointBulk emits singleton bulks so a checkpoint's max_sequence
// is the predecessor of its creation log. Ordinary workers continue writing
// while the checkpoint Apply is in flight.
func generateCheckpointBulk(state oracle.GlobalState) oracle.Bulk {
	ids := state.QueryCheckpointIDs()
	var req *servicepb.Request
	switch random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5, 6, 7}) {
	case 0:
		req = &servicepb.Request{Type: &servicepb.Request_SetQueryCheckpointSchedule{SetQueryCheckpointSchedule: &servicepb.SetQueryCheckpointScheduleRequest{Cron: modelCheckpointCron}}}
	case 1:
		req = &servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpointSchedule{DeleteQueryCheckpointSchedule: &servicepb.DeleteQueryCheckpointScheduleRequest{}}}
	case 2, 3:
		if len(ids) > 0 {
			req = &servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &servicepb.DeleteQueryCheckpointRequest{CheckpointId: random.RandomChoice(ids)}}}
		}
	}
	if req == nil {
		req = &servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}}
	}
	return oracle.Bulk{Requests: []*servicepb.Request{req}}
}

// checkpointOrdersMatch checks the independent lifecycle prediction against
// the actual global log payloads. No snapshots are published on disagreement.
func checkpointOrdersMatch(bulk oracle.Bulk, orders []oracle.OrderResult, logs []*commonpb.Log) bool {
	for i, req := range bulk.Requests {
		if req.GetCreateQueryCheckpoint() == nil && req.GetDeleteQueryCheckpoint() == nil && req.GetSetQueryCheckpointSchedule() == nil && req.GetDeleteQueryCheckpointSchedule() == nil {
			continue
		}
		if i >= len(orders) || i >= len(logs) || logs[i].GetSequence() == 0 {
			return false
		}
		payload := logs[i].GetPayload()
		switch {
		case req.GetCreateQueryCheckpoint() != nil:
			cp := payload.GetCreatedQueryCheckpoint()
			if cp == nil || cp.GetCheckpointId() != orders[i].CheckpointID || cp.GetMaxSequence() != logs[i].GetSequence()-1 {
				return false
			}
		case req.GetDeleteQueryCheckpoint() != nil:
			cp := payload.GetDeletedQueryCheckpoint()
			if cp == nil || cp.GetCheckpointId() != orders[i].CheckpointID {
				return false
			}
		case req.GetSetQueryCheckpointSchedule() != nil:
			schedule := payload.GetSetQueryCheckpointSchedule()
			if schedule == nil || schedule.GetCron() != req.GetSetQueryCheckpointSchedule().GetCron() {
				return false
			}
		case req.GetDeleteQueryCheckpointSchedule() != nil:
			if payload.GetDeleteQueryCheckpointSchedule() == nil {
				return false
			}
		}
	}
	return true
}

// recordCheckpoints runs only after a validated commit advances modelState.
// Creates are singleton bulks, so the post-state has precisely the business
// contents at max_sequence. Readers hold a value copy even after deletion.
func (c *Checker) recordCheckpoints(logs []*commonpb.Log) {
	for _, log := range logs {
		payload := log.GetPayload()
		switch {
		case payload.GetCreatedQueryCheckpoint() != nil:
			cp := payload.GetCreatedQueryCheckpoint()
			c.checkpoints[cp.GetCheckpointId()] = checkpointSnapshot{state: c.modelState, maxSequence: cp.GetMaxSequence()}
			noteCheckpointCoverage(checkpointCreateCoverage)
		case payload.GetDeletedQueryCheckpoint() != nil:
			id := payload.GetDeletedQueryCheckpoint().GetCheckpointId()
			delete(c.checkpoints, id)
			c.deletedCheckpoints = append(c.deletedCheckpoints, id)
			if len(c.deletedCheckpoints) > defaultModelCheckpointLimit {
				c.deletedCheckpoints = c.deletedCheckpoints[1:]
			}
			noteCheckpointCoverage(checkpointDeleteCoverage)
		case payload.GetSetQueryCheckpointSchedule() != nil:
			noteCheckpointCoverage(checkpointScheduleSetCoverage)
		case payload.GetDeleteQueryCheckpointSchedule() != nil:
			noteCheckpointCoverage(checkpointScheduleDeleteCoverage)
		}
	}
}
