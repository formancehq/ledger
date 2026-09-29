package main

import (
	"fmt"
	"testing"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
	"github.com/stretchr/testify/require"
)

func TestIdempotencyEquivalentFormsAgainstServer(t *testing.T) {
	t.Parallel()
	ctx, client := skippableTestServer(t)
	_, err := client.Apply(ctx, commonpb.UnsignedApplyRequest("", actions.CreateLedgerAction("L", nil)))
	require.NoError(t, err)
	state := oracle.NewGlobalState()
	for _, reverse := range []bool{false, true} {
		name := fmt.Sprintf("typed%t", reverse)
		topAdd := oracletest.AddTypeReq(name)
		pairs := [][2]*commonpb.Request{
			{enforcementModeRequest("L", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, false), enforcementModeRequest("L", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, true)},
			{topAdd, {Type: &commonpb.Request_Apply{Apply: &commonpb.LedgerApplyRequest{Ledger: "L", Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_AddAccountType{AddAccountType: &commonpb.AddAccountTypeRequest{AccountType: topAdd.GetAddAccountType().GetAccountType()}}}}}}},
			{oracletest.RemoveTypeReq(name), {Type: &commonpb.Request_Apply{Apply: &commonpb.LedgerApplyRequest{Ledger: "L", Action: &commonpb.LedgerAction{Data: &commonpb.LedgerAction_RemoveAccountType{RemoveAccountType: &commonpb.RemoveAccountTypeRequest{Name: name}}}}}}},
		}
		for i, pair := range pairs {
			if reverse {
				pair[0], pair[1] = pair[1], pair[0]
			}
			key := fmt.Sprintf("forms-%t-%d", reverse, i)
			original := oracle.Bulk{IdempotencyKey: key, Requests: []*commonpb.Request{pair[0]}}
			first, err := client.Apply(ctx, applyRequest(original))
			require.NoError(t, err)
			predicted := state.Apply(original)
			require.True(t, predicted.OK, predicted.Reason)
			state = predicted.State
			replayBulk := oracle.Bulk{IdempotencyKey: key, Requests: []*commonpb.Request{pair[1]}}
			replay, err := client.Apply(ctx, applyRequest(replayBulk))
			require.NoError(t, err)
			require.True(t, first.EqualVT(replay), "equivalent wire forms replay original sequences and payload")
			expected := state.Apply(replayBulk)
			require.True(t, expected.OK, expected.Reason)
			require.Equal(t, state.Fingerprint(), expected.State.Fingerprint())
			require.True(t, replayOrdersMatch(replayBulk, expected.Orders, replay.GetLogs()))
		}
	}
}
