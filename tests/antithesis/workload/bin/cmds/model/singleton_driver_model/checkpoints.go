package main

import (
	"github.com/antithesishq/antithesis-sdk-go/random"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// The model template owns its cluster's checkpoint timeline. This must match
// the server's query-checkpoint-limit configuration (10 by default).
const (
	defaultModelCheckpointLimit = 10
	deletedCheckpointHistoryCap = 10
)

// A 100-year interval exercises configuration without introducing scheduler
// writes outside the model driver's observed Apply stream during a test run.
var modelCheckpointCrons = []string{"@every 876000h", "@every 900000h", "@every 950000h"}

type checkpointSnapshot struct {
	state       oracle.GlobalState
	maxSequence uint64
}

// generateCheckpointBulk emits singleton bulks so a checkpoint's max_sequence
// is the predecessor of its creation log. Ordinary workers continue writing
// while the checkpoint Apply is in flight.
func generateCheckpointBulk(state oracle.GlobalState) oracle.Bulk {
	ids := state.QueryCheckpointIDs()
	var req *ledgerpb.Request
	switch random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
	case 0:
		req = &ledgerpb.Request{Type: &ledgerpb.Request_SetQueryCheckpointSchedule{SetQueryCheckpointSchedule: &ledgerpb.SetQueryCheckpointScheduleRequest{Cron: random.RandomChoice(modelCheckpointCrons)}}}
	case 1:
		req = &ledgerpb.Request{Type: &ledgerpb.Request_DeleteQueryCheckpointSchedule{DeleteQueryCheckpointSchedule: &ledgerpb.DeleteQueryCheckpointScheduleRequest{}}}
	case 2:
		req = &ledgerpb.Request{Type: &ledgerpb.Request_SetQueryCheckpointSchedule{SetQueryCheckpointSchedule: &ledgerpb.SetQueryCheckpointScheduleRequest{Cron: "invalid checkpoint cron"}}}
	case 3:
		req = &ledgerpb.Request{Type: &ledgerpb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &ledgerpb.DeleteQueryCheckpointRequest{}}}
	case 4:
		req = &ledgerpb.Request{Type: &ledgerpb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &ledgerpb.DeleteQueryCheckpointRequest{CheckpointId: state.NextQueryCheckpointID()}}}
	case 5, 6:
		if len(ids) > 0 {
			req = &ledgerpb.Request{Type: &ledgerpb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &ledgerpb.DeleteQueryCheckpointRequest{CheckpointId: random.RandomChoice(ids)}}}
		}
	}
	if req == nil {
		req = &ledgerpb.Request{Type: &ledgerpb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{}}}
	}

	return oracle.Bulk{Requests: []*ledgerpb.Request{req}}
}

// checkpointOrdersMatch checks the independent lifecycle prediction against
// the actual global log payloads. No snapshots are published on disagreement.
func checkpointOrdersMatch(bulk oracle.Bulk, orders []oracle.OrderResult, logs []*ledgerpb.Log) bool {
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
// contents at max_sequence. Readers hold a value copy even after deletion. A
// bounded history also retains known snapshots for later reads on replicas
// that have not removed the files.
func (c *Checker) recordCheckpoints(logs []*ledgerpb.Log) {
	for _, log := range logs {
		payload := log.GetPayload()
		switch {
		case payload.GetCreatedQueryCheckpoint() != nil:
			cp := payload.GetCreatedQueryCheckpoint()
			c.checkpoints[cp.GetCheckpointId()] = checkpointSnapshot{state: c.modelState, maxSequence: cp.GetMaxSequence()}
			noteCheckpointCoverage(checkpointCreateCoverage)
		case payload.GetDeletedQueryCheckpoint() != nil:
			id := payload.GetDeletedQueryCheckpoint().GetCheckpointId()
			snapshot, known := c.checkpoints[id]
			delete(c.checkpoints, id)
			// Inherited checkpoints have no captured business state. Do not
			// invent one for validating successful reads after deletion.
			if !known {
				noteCheckpointCoverage(checkpointDeleteCoverage)

				continue
			}
			c.deletedCheckpointSnapshots[id] = snapshot
			c.deletedCheckpoints = append(c.deletedCheckpoints, id)
			if len(c.deletedCheckpoints) > deletedCheckpointHistoryCap {
				delete(c.deletedCheckpointSnapshots, c.deletedCheckpoints[0])
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
