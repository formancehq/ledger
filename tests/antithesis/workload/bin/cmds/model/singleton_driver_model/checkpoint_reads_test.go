package main

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestCheckpointReadsRejectLiveMutation(t *testing.T) {
	t.Parallel()
	frozen := buildGlobal(t, oracletest.TxReqL("L", "world", "acc:1", "USD", 5))
	result := frozen.Apply(oracle.Bulk{Requests: []*servicepb.Request{oracletest.RevertReqL("L", 1, true)}})
	require.True(t, result.OK)
	account := &commonpb.Account{Address: "acc:1", Volumes: []*commonpb.AccountVolume{{Asset: "USD", Volumes: &commonpb.VolumesWithBalance{Input: "5", Output: "0", Balance: "5"}}}}
	require.True(t, checkpointAccountReadMatches(frozen, "L", "acc:1", account, true))
	account.Volumes[0].Volumes.Output = "5"
	account.Volumes[0].Volumes.Balance = "0"
	require.True(t, checkpointAccountReadMatches(result.State, "L", "acc:1", account, true))
	require.False(t, checkpointAccountReadMatches(frozen, "L", "acc:1", account, true))
	require.True(t, checkpointTransactionReadMatches(frozen, "L", 1, serverTxFromRec(frozen.Ledger("L").Txs().Get(0)), true))
	require.False(t, checkpointTransactionReadMatches(frozen, "L", 1, serverTxFromRec(result.State.Ledger("L").Txs().Get(0)), true))
	require.False(t, checkpointTransactionReadMatches(frozen, "L", 1, nil, false))
	require.True(t, checkpointTransactionReadMatches(frozen, "L", 2, nil, false))
}

func TestCheckpointMissingRequiresNotFoundStatus(t *testing.T) {
	t.Parallel()
	require.True(t, checkpointNotFound(status.Error(codes.NotFound, "query checkpoint 7 not found")))
	require.False(t, checkpointNotFound(nil))
	require.False(t, checkpointNotFound(errors.New("query checkpoint 7 not found")))
	for _, code := range []codes.Code{codes.Internal, codes.FailedPrecondition, codes.Unavailable, codes.InvalidArgument} {
		require.False(t, checkpointNotFound(status.Error(code, "query checkpoint 7 not found")))
	}
}

func TestCheckpointMetadataOnlyAccountWithLaterVolume(t *testing.T) {
	t.Parallel()
	value := &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "before"}}
	frozen := buildGlobal(t, oracletest.TxReqL("L", "world", "z", "USD", 5), oracletest.AddAccountMetaReq("a", "phase", value))
	account := &commonpb.Account{Address: "a", Metadata: map[string]*commonpb.MetadataValue{"phase": value}}
	require.True(t, checkpointAccountReadMatches(frozen, "L", "a", account, true))
	require.False(t, checkpointAccountReadMatches(frozen, "L", "a", nil, false))
}

func TestCheckpointReadAcceptsEmptyAccountWithoutInventingState(t *testing.T) {
	t.Parallel()
	frozen := oracle.NewGlobalState()
	require.True(t, checkpointAccountReadMatches(frozen, "L", "empty", &commonpb.Account{Address: "empty"}, true))
	require.False(t, checkpointAccountReadMatches(frozen, "L", "empty", &commonpb.Account{Address: "empty", Metadata: checkpointMetadata("ghost")}, true))
}

func TestDeletedCheckpointReadAcceptsOnlyFrozenSuccessOrNotFound(t *testing.T) {
	t.Parallel()
	c := NewChecker([]string{"L"}, nil)
	frozen := buildGlobal(t, oracletest.TxReqL("L", "world", "acc:1", "USD", 5))
	c.modelState = frozen
	create := bulkOf(&servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}})
	c.validateBulkSuccess(create, checkpointCreateResponse(11, 1))
	require.False(t, c.checkpointReadOutcomeMatches(1, 0, false, status.Error(codes.NotFound, "missing live checkpoint")))
	del := bulkOf(&servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &servicepb.DeleteQueryCheckpointRequest{CheckpointId: 1}}})
	c.validateBulkSuccess(del, &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Sequence: 12, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeletedQueryCheckpoint{DeletedQueryCheckpoint: &commonpb.DeletedQueryCheckpointLog{CheckpointId: 1}}}}}})
	frozen = c.deletedCheckpointSnapshots[1].state
	account := &commonpb.Account{Address: "acc:1", Volumes: []*commonpb.AccountVolume{{Asset: "USD", Volumes: &commonpb.VolumesWithBalance{Input: "5", Output: "0", Balance: "5"}}}}
	require.True(t, c.checkpointReadOutcomeMatches(1, 0, checkpointAccountReadMatches(frozen, "L", "acc:1", account, true), nil), "a replica may still serve the frozen checkpoint after deletion")
	account.Volumes[0].Volumes.Input = "9"
	account.Volumes[0].Volumes.Balance = "9"
	require.False(t, c.checkpointReadOutcomeMatches(1, 0, checkpointAccountReadMatches(frozen, "L", "acc:1", account, true), nil), "deletion must never permit newer business data")
	require.True(t, c.checkpointReadOutcomeMatches(1, 0, false, status.Error(codes.NotFound, "deleted")))
	require.False(t, c.checkpointReadOutcomeMatches(1, 0, true, status.Error(codes.Internal, "I/O failure")))
}

func TestLiveCheckpointTransactionAbsenceMatchesFrozenState(t *testing.T) {
	t.Parallel()
	c := NewChecker([]string{"L"}, nil)
	c.modelState = buildGlobal(t, oracletest.TxReqL("L", "world", "acc:1", "USD", 5))
	create := bulkOf(&servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}})
	c.validateBulkSuccess(create, checkpointCreateResponse(11, 1))
	frozen := c.checkpoints[1].state
	notFound := status.Error(codes.NotFound, "transaction not found")
	require.True(t, c.checkpointReadOutcomeMatches(1, 0, checkpointTransactionReadMatches(frozen, "L", 2, nil, false), notFound), "NotFound also describes a transaction absent from a live frozen checkpoint")
	require.False(t, c.checkpointReadOutcomeMatches(1, 0, checkpointTransactionReadMatches(frozen, "L", 1, nil, false), notFound), "a known frozen transaction cannot disappear while the checkpoint remains live")
	deleted := c.modelState.Apply(bulkOf(&servicepb.Request{Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "L"}}}))
	require.True(t, deleted.OK)
	c.modelState = deleted.State
	require.False(t, c.checkpointReadOutcomeMatches(1, 0, checkpointTransactionReadMatches(frozen, "L", 1, nil, false), notFound), "deleting the live ledger cannot make a frozen transaction disappear")
	require.False(t, c.checkpointReadOutcomeMatches(1, 0, checkpointTransactionReadMatches(frozen, "L", 2, nil, false), status.Error(codes.Internal, "I/O failure")), "absence does not excuse an unrelated error")
}
