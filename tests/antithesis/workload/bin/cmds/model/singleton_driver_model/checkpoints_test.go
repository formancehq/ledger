package main

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

func TestCheckpointCreateGateSerializesPredictedIDProbes(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	create := bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{}}})
	type dispatch struct {
		ticket    uint64
		predicted uint64
		processed chan struct{}
	}
	dispatched := make(chan dispatch)
	var workers sync.WaitGroup
	workers.Add(2)
	dispatchCreate := func() {
		defer workers.Done()
		c.checkpointCreateMu.Lock()
		defer c.checkpointCreateMu.Unlock()

		c.mu.Lock()
		d := dispatch{
			ticket:    c.registerInflight(create),
			predicted: c.modelState.NextQueryCheckpointID(),
			processed: make(chan struct{}),
		}
		c.mu.Unlock()
		dispatched <- d
		<-d.processed
	}

	go dispatchCreate()
	go dispatchCreate()

	first := <-dispatched
	require.Equal(t, uint64(1), first.predicted)
	c.handleObservation(observation{
		ticket: first.ticket, bulk: create, resp: checkpointCreateResponse(1, 1),
		observeTicket: first.ticket, processed: first.processed,
	})

	second := <-dispatched
	require.Equal(t, uint64(2), second.predicted,
		"the second probe must bind after the first create drains")
	c.handleObservation(observation{
		ticket: second.ticket, bulk: create, resp: checkpointCreateResponse(2, 2),
		observeTicket: second.ticket, processed: second.processed,
	})
	workers.Wait()
}

func TestCheckpointCapturesCommitOrderInsteadOfResponseOrder(t *testing.T) {
	t.Parallel()
	c := NewChecker([]string{"L"}, nil)
	before, beforeResponse := checkpointMetadataWrite("before", 10)
	create := bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{}}})
	after, afterResponse := checkpointMetadataWrite("after", 12)
	first := c.registerInflight(before)
	second := c.registerInflight(create)
	third := c.registerInflight(after)
	c.handleObservation(observation{ticket: third, bulk: after, resp: afterResponse, observeTicket: third})
	c.handleObservation(observation{ticket: second, bulk: create, resp: &ledgerpb.ApplyResponse{Logs: []*ledgerpb.Log{{Sequence: 11, Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_CreatedQueryCheckpoint{CreatedQueryCheckpoint: &ledgerpb.CreatedQueryCheckpointLog{CheckpointId: 1, MaxSequence: 10}}}}}}, observeTicket: third})
	require.Empty(t, c.checkpoints, "an observed but undrained creation cannot be read")
	c.handleObservation(observation{ticket: first, bulk: before, resp: beforeResponse, observeTicket: third})
	require.Len(t, c.checkpoints, 1)
	require.Equal(t, uint64(10), c.checkpoints[1].maxSequence)
	require.True(t, ledgerMetaMatches(c.checkpoints[1].state.Ledger("L"), checkpointMetadata("before")))
	require.True(t, ledgerMetaMatches(c.modelState.Ledger("L"), checkpointMetadata("after")))
	require.False(t, ledgerMetaMatches(c.checkpoints[1].state.Ledger("L"), checkpointMetadata("after")))
}

func TestCheckpointCreateDrainsAfterEarlierInflightFailure(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	failed, _ := checkpointMetadataWrite("failed", 1)
	create := bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{}}})
	failedTicket := c.registerInflight(failed)
	createTicket := c.registerInflight(create)
	processed := make(chan struct{})

	c.handleObservation(observation{
		ticket: createTicket, bulk: create, resp: checkpointCreateResponse(1, 1),
		observeTicket: createTicket, processed: processed,
	})
	require.Len(t, c.pending, 1, "the earlier in-flight request must initially gate the create")

	c.handleObservation(observation{
		ticket: failedTicket, bulk: failed,
		err: status.Error(codes.Unavailable, "request outcome unavailable"),
	})

	require.Empty(t, c.pending)
	require.Len(t, c.checkpoints, 1)
	require.Equal(t, uint64(2), c.modelState.NextQueryCheckpointID())
	select {
	case <-processed:
	default:
		t.Fatal("the create worker was not released after the failed request stopped gating its observation")
	}
}

func TestPredictedCheckpointMatchesCreationPredecessor(t *testing.T) {
	t.Parallel()
	c := NewChecker([]string{"L"}, nil)
	before, beforeResponse := checkpointMetadataWrite("before", 10)
	c.validateBulkSuccess(before, beforeResponse)
	create := bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{}}})
	createTicket := c.registerInflight(create)
	after, _ := checkpointMetadataWrite("after", 12)
	c.registerInflight(after)

	c.mu.Lock()
	require.True(t, c.checkpointCreationMatches(c.ticketSeq.Load(), 1, func(state oracle.GlobalState) bool {
		return ledgerMetaMatches(state.Ledger("L"), checkpointMetadata("before"))
	}))
	require.False(t, c.checkpointCreationMatches(c.ticketSeq.Load(), 1, func(state oracle.GlobalState) bool {
		return ledgerMetaMatches(state.Ledger("L"), checkpointMetadata("impossible"))
	}))
	c.mu.Unlock()
	require.Equal(t, uint64(1), createTicket)
}

func TestPredictedCheckpointRejectsTombstonedLedger(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	deleted := c.modelState.Apply(bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_DeleteLedger{
		DeleteLedger: &ledgerpb.DeleteLedgerRequest{Name: "L"},
	}}))
	require.True(t, deleted.OK)
	require.False(t, predictedCheckpointLedgerMatches(deleted.State, "L", &ledgerpb.LedgerInfo{}))
}

func checkpointMetadata(value string) map[string]*ledgerpb.MetadataValue {
	return map[string]*ledgerpb.MetadataValue{"phase": {Type: &ledgerpb.MetadataValue_StringValue{StringValue: value}}}
}

func checkpointMetadataWrite(value string, sequence uint64) (oracleBulk oracle.Bulk, response *ledgerpb.ApplyResponse) {
	md := checkpointMetadata(value)

	return bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_SaveLedgerMetadata{SaveLedgerMetadata: &ledgerpb.SaveLedgerMetadataRequest{Ledger: "L", Metadata: md}}}), &ledgerpb.ApplyResponse{Logs: []*ledgerpb.Log{{Sequence: sequence, Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_SavedLedgerMetadata{SavedLedgerMetadata: &ledgerpb.SavedLedgerMetadataLog{Ledger: "L", Metadata: md}}}}}}
}

func TestCheckpointDeletionRetainsFrozenSnapshotWithoutChangingReaders(t *testing.T) {
	t.Parallel()
	c := NewChecker([]string{"L"}, nil)
	write, response := checkpointMetadataWrite("frozen", 10)
	c.validateBulkSuccess(write, response)
	create := bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{}}})
	c.validateBulkSuccess(create, checkpointCreateResponse(11, 1))
	frozen := c.checkpoints[1]
	del := bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &ledgerpb.DeleteQueryCheckpointRequest{CheckpointId: 1}}})
	c.validateBulkSuccess(del, &ledgerpb.ApplyResponse{Logs: []*ledgerpb.Log{{Sequence: 12, Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_DeletedQueryCheckpoint{DeletedQueryCheckpoint: &ledgerpb.DeletedQueryCheckpointLog{CheckpointId: 1}}}}}})
	require.Empty(t, c.checkpoints)
	require.Equal(t, []uint64{1}, c.deletedCheckpoints)
	require.True(t, ledgerMetaMatches(c.deletedCheckpointSnapshots[1].state.Ledger("L"), checkpointMetadata("frozen")))
	require.True(t, ledgerMetaMatches(frozen.state.Ledger("L"), checkpointMetadata("frozen")), "a read already in flight retains its frozen value")
}

func TestCheckpointReplayMustPreserveIdentityAndFrontier(t *testing.T) {
	t.Parallel()
	c := NewChecker([]string{"L"}, nil)
	create := bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{}}})
	create.IdempotencyKey = "checkpoint"
	result := c.modelState.Apply(create)
	require.True(t, result.OK)
	response := checkpointCreateResponse(11, 1)
	require.True(t, replayOrdersMatch(create, result.Orders, response.GetLogs()))
	response.Logs[0].Payload.GetCreatedQueryCheckpoint().CheckpointId = 2
	require.False(t, replayOrdersMatch(create, result.Orders, response.GetLogs()))
	response.Logs[0].Payload.GetCreatedQueryCheckpoint().CheckpointId = 1
	response.Logs[0].Payload.GetCreatedQueryCheckpoint().MaxSequence = 11
	require.False(t, replayOrdersMatch(create, result.Orders, response.GetLogs()))
}

func checkpointCreateResponse(sequence, id uint64) *ledgerpb.ApplyResponse {
	return &ledgerpb.ApplyResponse{Logs: []*ledgerpb.Log{{Sequence: sequence, Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_CreatedQueryCheckpoint{CreatedQueryCheckpoint: &ledgerpb.CreatedQueryCheckpointLog{CheckpointId: id, MaxSequence: sequence - 1}}}}}}
}

func TestDeletedCheckpointSnapshotsHaveBoundedRetention(t *testing.T) {
	t.Parallel()
	c := NewChecker([]string{"L"}, nil)
	create := bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{}}})
	for id := uint64(1); id <= deletedCheckpointHistoryCap+1; id++ {
		c.validateBulkSuccess(create, checkpointCreateResponse(id*2, id))
		del := bulkOf(&ledgerpb.Request{Type: &ledgerpb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &ledgerpb.DeleteQueryCheckpointRequest{CheckpointId: id}}})
		c.validateBulkSuccess(del, &ledgerpb.ApplyResponse{Logs: []*ledgerpb.Log{{Sequence: id*2 + 1, Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_DeletedQueryCheckpoint{DeletedQueryCheckpoint: &ledgerpb.DeletedQueryCheckpointLog{CheckpointId: id}}}}}})
	}
	require.Len(t, c.deletedCheckpointSnapshots, deletedCheckpointHistoryCap)
	require.Len(t, c.deletedCheckpoints, deletedCheckpointHistoryCap)
	require.NotContains(t, c.deletedCheckpointSnapshots, uint64(1))
	require.NotContains(t, c.deletedCheckpoints, uint64(1))
	require.Contains(t, c.deletedCheckpointSnapshots, uint64(deletedCheckpointHistoryCap+1))
}
