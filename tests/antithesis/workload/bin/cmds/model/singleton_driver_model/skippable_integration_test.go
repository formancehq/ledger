package main

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	workloadinternal "github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// Exercise the same wire requests through the real admission/FSM path and the
// oracle, including the response validator that advances the driver's state.
func TestSkippableOrdersAgainstServer(t *testing.T) {
	t.Parallel()
	ctx, client := skippableTestServer(t)
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("L", nil)))
	require.NoError(t, err)
	checker := NewChecker([]string{"L"}, nil)
	commit := func(t *testing.T, reqs ...*servicepb.Request) *servicepb.ApplyResponse {
		t.Helper()
		bulk := oracle.Bulk{Requests: reqs}
		before := len(checker.modelState.Ledger("L").LogRows())
		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", reqs...))
		require.NoError(t, err)
		require.Len(t, resp.GetLogs(), len(reqs))
		checker.crossCheckCommit(bulk, resp)
		require.Len(t, checker.modelState.Ledger("L").LogRows(), before+len(reqs), "response validator must advance the model")
		return resp
	}
	rejectCorruption := func(t *testing.T, mutate func(*servicepb.ApplyResponse), reqs ...*servicepb.Request) *servicepb.ApplyResponse {
		t.Helper()
		bulk := oracle.Bulk{Requests: reqs}
		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", reqs...))
		require.NoError(t, err)
		before := checker.modelState.Fingerprint()
		corrupted := proto.Clone(resp).(*servicepb.ApplyResponse)
		mutate(corrupted)
		checker.crossCheckCommit(bulk, corrupted)
		require.Equal(t, before, checker.modelState.Fingerprint(), "a rejected response must not advance the model")
		checker.crossCheckCommit(bulk, resp)
		require.NotEqual(t, before, checker.modelState.Fingerprint(), "the matching response must advance the model")
		return resp
	}

	t.Run("commit response checks reject mutations", func(t *testing.T) {
		rejectCorruption(t, func(resp *servicepb.ApplyResponse) {
			resp.Logs[0].GetPayload().GetApply().GetLog().Id++
		}, oracletest.TxReqRefL("L", "identity-check", "world", "typed:1", "USD", 1))

		duplicate := actions.WithSkippableReasons(
			oracletest.TxReqRefL("L", "identity-check", "world", "typed:1", "USD", 1),
			commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
		)
		rejectCorruption(t, func(resp *servicepb.ApplyResponse) {
			resp.Logs[0].GetPayload().GetApply().GetLog().GetData().GetOrderSkipped().Reason = commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED
		}, duplicate, oracletest.TxReq("world", "typed:1", "USD", 1))

		mode := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
			Ledger: "L",
			Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_SetDefaultEnforcementMode{
				SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeRequest{EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT},
			}},
		}}}
		rejectCorruption(t, func(resp *servicepb.ApplyResponse) {
			resp.Logs[0].GetPayload().GetApply().GetLog().GetData().GetUpdatedDefaultEnforcementMode().EnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
		}, mode)

		rejectCorruption(t, func(resp *servicepb.ApplyResponse) {
			resp.Logs[0].GetPayload().GetApply().GetLog().GetData().GetAddedAccountType().AccountType.Name = "corrupted"
		}, actions.AddAccountTypeAction("L", "response-check", "response-check:{id}"))
	})

	first := commit(t, oracletest.TxReqRefL("L", "original", "world", "typed:1", "USD", 10))
	txID := first.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction().GetId()
	commit(t, oracletest.RevertReqL("L", txID, true))
	addType := func() *servicepb.Request {
		return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "L", Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddAccountType{AddAccountType: &servicepb.AddAccountTypeRequest{AccountType: &commonpb.AccountType{Name: "typed", Pattern: "typed:{id}"}}}}}}}
	}
	commit(t, addType())
	cases := []struct {
		name    string
		request *servicepb.Request
		reason  commonpb.ErrorReason
	}{
		{"reference conflict", oracletest.TxReqRefL("L", "original", "world", "typed:1", "USD", 2), commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
		{"already reverted", oracletest.RevertReqL("L", txID, true), commonpb.ErrorReason_ERROR_REASON_TRANSACTION_ALREADY_REVERTED},
		{"missing account metadata", actions.DeleteAccountMetadataAction("L", "typed:1", "missing"), commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND},
		{"missing transaction metadata", actions.DeleteTransactionMetadataAction("L", txID, "missing"), commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND},
		{"existing account type", addType(), commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS},
		{"missing account type", &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "L", Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RemoveAccountType{RemoveAccountType: &servicepb.RemoveAccountTypeRequest{Name: "absent"}}}}}}, commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			actions.WithSkippableReasons(tc.request, tc.reason)
			resp := commit(t, tc.request, oracletest.TxReq("world", "typed:1", "USD", 1))
			skipped := resp.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetOrderSkipped()
			require.NotNil(t, skipped)
			require.Equal(t, tc.reason, skipped.GetReason())
			require.NotNil(t, resp.GetLogs()[1].GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction())
		})
	}
	t.Run("keyed skipped batch replays original result", func(t *testing.T) {
		bulk := oracle.Bulk{IdempotencyKey: "skip-replay", Requests: []*servicepb.Request{cases[0].request, oracletest.TxReq("world", "typed:1", "USD", 4)}}
		first, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(bulk.IdempotencyKey, bulk.Requests...))
		require.NoError(t, err)
		before := len(checker.modelState.Ledger("L").LogRows())
		checker.crossCheckCommit(bulk, first)
		require.Len(t, checker.modelState.Ledger("L").LogRows(), before+2)
		fingerprint := checker.modelState.Fingerprint()
		expected := checker.modelState.Apply(bulk)
		require.True(t, expected.OK)
		require.Equal(t, fingerprint, expected.State.Fingerprint())
		replay, err := client.Apply(ctx, servicepb.UnsignedApplyRequest(bulk.IdempotencyKey, bulk.Requests...))
		require.NoError(t, err)
		require.True(t, replayOrdersMatch(bulk, expected.Orders, replay.GetLogs()))
		require.True(t, proto.Equal(first, replay), "replay must preserve original logs and sequences")
		require.Equal(t, fingerprint, checker.modelState.Fingerprint())
		corrupted := proto.Clone(replay).(*servicepb.ApplyResponse)
		corrupted.Logs[0].GetPayload().GetApply().GetLog().GetData().GetOrderSkipped().Context["reference"] = "wrong"
		require.False(t, replayOrdersMatch(bulk, expected.Orders, corrupted.GetLogs()))
	})
	t.Run("later fatal order rolls back skipped log and earlier success", func(t *testing.T) {
		reqs := []*servicepb.Request{oracletest.TxReq("world", "typed:1", "USD", 3), cases[0].request, oracletest.TxReq("typed:empty", "typed:1", "USD", 1)}
		predicted := checker.modelState.Apply(oracle.Bulk{Requests: reqs})
		require.False(t, predicted.OK)
		require.Equal(t, "INSUFFICIENT_FUNDS", predicted.Reason)
		require.Equal(t, checker.modelState.Ledger("L").LogRows(), predicted.State.Ledger("L").LogRows())
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", reqs...))
		require.Error(t, err)
		require.True(t, workloadinternal.HasErrorReason(err, "INSUFFICIENT_FUNDS"))
		// The next successful response pins IDs and volumes against the unchanged
		// model, exposing any leaked transaction, skipped log, or balance effect.
		commit(t, oracletest.TxReq("world", "typed:1", "USD", 1))
	})
	t.Run("disallowed opt in is rejected at admission", func(t *testing.T) {
		req := actions.WithSkippableReasons(actions.SaveAccountMetadataAction("L", "typed:1", map[string]string{"key": "value"}), commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND)
		require.False(t, checker.modelState.Apply(oracle.Bulk{Requests: []*servicepb.Request{req}}).OK)
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", req))
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
	t.Run("listed skipped logs preserve their correlators", func(t *testing.T) {
		readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
		logs, err := actions.ListLogsFiltered(readCtx, client, &servicepb.ListLogsRequest{Ledger: "L", Options: &commonpb.ListOptions{PageSize: 100}})
		require.NoError(t, err)
		require.Len(t, logs, len(checker.modelState.Ledger("L").LogRows()))
		require.True(t, logWindowMatches(checker.modelState.Ledger("L"), "L", nil, 0, 100, serverLogRows(logs)))
		found := false
		for _, log := range logs {
			skipped := log.GetPayload().GetApply().GetLog().GetData().GetOrderSkipped()
			if skipped == nil {
				continue
			}
			found = true
			skipped.Context["unexpected"] = "corrupted"
			require.False(t, logWindowMatches(checker.modelState.Ledger("L"), "L", nil, 0, 100, serverLogRows(logs)))
			break
		}
		require.True(t, found, "the listed page must contain a skipped log")
	})

}

func skippableTestServer(t *testing.T) (context.Context, servicepb.BucketServiceClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	lease := testserver.AllocateNodeLease()
	instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{NodeID: 1, ClusterID: "oracle-skippable", Ports: lease.Ports(), WalDir: t.TempDir(), DataDir: t.TempDir(), Output: io.Discard})
	instruments = append(instruments, testserver.WithBootstrap())
	server := lease.NewService(cmdserver.NewRunCommandWithBindings, testservice.WithInstruments(instruments...))
	require.NoError(t, server.Start(ctx))
	t.Cleanup(func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		require.NoError(t, server.Stop(stopCtx))
	})
	conn, err := grpc.NewClient(fmt.Sprintf("localhost:%d", lease.Ports().GRPC()), grpcprotocol.ClientOption(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	cluster := clusterpb.NewClusterServiceClient(conn)
	require.Eventually(t, func() bool {
		state, err := cluster.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		return err == nil && state.GetLeader() != 0
	}, 5*time.Second, 10*time.Millisecond)
	return ctx, servicepb.NewBucketServiceClient(conn)
}
