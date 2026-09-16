package main

import (
	"sync"
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/stretchr/testify/require"
)

func TestCheckpointCreateGateSerializesPredictedIDProbes(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	create := bulkOf(&servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}})
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
	create := bulkOf(&servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}})
	after, afterResponse := checkpointMetadataWrite("after", 12)
	first := c.registerInflight(before)
	second := c.registerInflight(create)
	third := c.registerInflight(after)
	c.handleObservation(observation{ticket: third, bulk: after, resp: afterResponse, observeTicket: third})
	c.handleObservation(observation{ticket: second, bulk: create, resp: &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Sequence: 11, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreatedQueryCheckpoint{CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{CheckpointId: 1, MaxSequence: 10}}}}}}, observeTicket: third})
	require.Empty(t, c.checkpoints, "an observed but undrained creation cannot be read")
	c.handleObservation(observation{ticket: first, bulk: before, resp: beforeResponse, observeTicket: third})
	require.Len(t, c.checkpoints, 1)
	require.Equal(t, uint64(10), c.checkpoints[1].maxSequence)
	require.True(t, ledgerMetaMatches(c.checkpoints[1].state.Ledger("L"), checkpointMetadata("before")))
	require.True(t, ledgerMetaMatches(c.modelState.Ledger("L"), checkpointMetadata("after")))
	require.False(t, ledgerMetaMatches(c.checkpoints[1].state.Ledger("L"), checkpointMetadata("after")))
}

func TestPredictedCheckpointMatchesCreationPredecessor(t *testing.T) {
	t.Parallel()
	c := NewChecker([]string{"L"}, nil)
	before, beforeResponse := checkpointMetadataWrite("before", 10)
	c.validateBulkSuccess(before, beforeResponse)
	create := bulkOf(&servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}})
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

func checkpointMetadata(value string) map[string]*commonpb.MetadataValue {
	return map[string]*commonpb.MetadataValue{"phase": {Type: &commonpb.MetadataValue_StringValue{StringValue: value}}}
}

func checkpointMetadataWrite(value string, sequence uint64) (oracleBulk oracle.Bulk, response *servicepb.ApplyResponse) {
	md := checkpointMetadata(value)
	return bulkOf(&servicepb.Request{Type: &servicepb.Request_SaveLedgerMetadata{SaveLedgerMetadata: &servicepb.SaveLedgerMetadataRequest{Ledger: "L", Metadata: md}}}), &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Sequence: sequence, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_SavedLedgerMetadata{SavedLedgerMetadata: &commonpb.SavedLedgerMetadataLog{Ledger: "L", Metadata: md}}}}}}
}

func TestCheckpointDeletionRetainsFrozenSnapshotWithoutChangingReaders(t *testing.T) {
	t.Parallel()
	c := NewChecker([]string{"L"}, nil)
	write, response := checkpointMetadataWrite("frozen", 10)
	c.validateBulkSuccess(write, response)
	create := bulkOf(&servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}})
	c.validateBulkSuccess(create, checkpointCreateResponse(11, 1))
	frozen := c.checkpoints[1]
	del := bulkOf(&servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &servicepb.DeleteQueryCheckpointRequest{CheckpointId: 1}}})
	c.validateBulkSuccess(del, &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Sequence: 12, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeletedQueryCheckpoint{DeletedQueryCheckpoint: &commonpb.DeletedQueryCheckpointLog{CheckpointId: 1}}}}}})
	require.Empty(t, c.checkpoints)
	require.Equal(t, []uint64{1}, c.deletedCheckpoints)
	require.True(t, ledgerMetaMatches(c.deletedCheckpointSnapshots[1].state.Ledger("L"), checkpointMetadata("frozen")))
	require.True(t, ledgerMetaMatches(frozen.state.Ledger("L"), checkpointMetadata("frozen")), "a read already in flight retains its frozen value")
}

func TestCheckpointReplayMustPreserveIdentityAndFrontier(t *testing.T) {
	t.Parallel()
	c := NewChecker([]string{"L"}, nil)
	create := bulkOf(&servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}})
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

func checkpointCreateResponse(sequence, id uint64) *servicepb.ApplyResponse {
	return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Sequence: sequence, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreatedQueryCheckpoint{CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{CheckpointId: id, MaxSequence: sequence - 1}}}}}}
}

func TestDeletedCheckpointSnapshotsHaveBoundedRetention(t *testing.T) {
	t.Parallel()
	c := NewChecker([]string{"L"}, nil)
	create := bulkOf(&servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}})
	for id := uint64(1); id <= deletedCheckpointHistoryCap+1; id++ {
		c.validateBulkSuccess(create, checkpointCreateResponse(id*2, id))
		del := bulkOf(&servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &servicepb.DeleteQueryCheckpointRequest{CheckpointId: id}}})
		c.validateBulkSuccess(del, &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Sequence: id*2 + 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeletedQueryCheckpoint{DeletedQueryCheckpoint: &commonpb.DeletedQueryCheckpointLog{CheckpointId: id}}}}}})
	}
	require.Len(t, c.deletedCheckpointSnapshots, deletedCheckpointHistoryCap)
	require.Len(t, c.deletedCheckpoints, deletedCheckpointHistoryCap)
	require.NotContains(t, c.deletedCheckpointSnapshots, uint64(1))
	require.NotContains(t, c.deletedCheckpoints, uint64(1))
	require.Contains(t, c.deletedCheckpointSnapshots, uint64(deletedCheckpointHistoryCap+1))
}
