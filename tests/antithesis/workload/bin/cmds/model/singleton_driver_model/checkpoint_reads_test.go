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
