package main

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestIdempotencyEquivalentFormsAgainstServer(t *testing.T) {
	t.Parallel()
	ctx, client := skippableTestServer(t)
	_, err := client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", actions.CreateLedgerAction("L", nil)))
	require.NoError(t, err)
	state := oracle.NewGlobalState()
	for _, reverse := range []bool{false, true} {
		name := fmt.Sprintf("typed%t", reverse)
		topAdd := oracletest.AddTypeReq(name)
		pairs := [][2]*ledgerpb.Request{
			{enforcementModeRequest("L", ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, false), enforcementModeRequest("L", ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, true)},
			{topAdd, {Type: &ledgerpb.Request_Apply{Apply: &ledgerpb.LedgerApplyRequest{Ledger: "L", Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_AddAccountType{AddAccountType: &ledgerpb.AddAccountTypeRequest{AccountType: topAdd.GetAddAccountType().GetAccountType()}}}}}}},
			{oracletest.RemoveTypeReq(name), {Type: &ledgerpb.Request_Apply{Apply: &ledgerpb.LedgerApplyRequest{Ledger: "L", Action: &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_RemoveAccountType{RemoveAccountType: &ledgerpb.RemoveAccountTypeRequest{Name: name}}}}}}},
		}
		for i, pair := range pairs {
			if reverse {
				pair[0], pair[1] = pair[1], pair[0]
			}
			key := fmt.Sprintf("forms-%t-%d", reverse, i)
			original := oracle.Bulk{IdempotencyKey: key, Requests: []*ledgerpb.Request{pair[0]}}
			first, err := client.Apply(ctx, applyRequest(original))
			require.NoError(t, err)
			predicted := state.Apply(original)
			require.True(t, predicted.OK, predicted.Reason)
			state = predicted.State
			replayBulk := oracle.Bulk{IdempotencyKey: key, Requests: []*ledgerpb.Request{pair[1]}}
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
