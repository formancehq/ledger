package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestEnforcementModesAgainstServer(t *testing.T) {
	t.Parallel()
	ctx, client := skippableTestServer(t)
	_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("L", nil)))
	require.NoError(t, err)
	checker := NewChecker([]string{"L"}, nil)
	commit := func(reqs ...*servicepb.Request) *servicepb.ApplyResponse {
		t.Helper()
		bulk := oracle.Bulk{Requests: reqs}
		predicted := checker.modelState.Apply(bulk)
		require.True(t, predicted.OK)
		resp, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", reqs...))
		require.NoError(t, err)
		checker.crossCheckCommit(bulk, resp)
		require.Equal(t, predicted.State.Ledger("L").LogKinds(), checker.modelState.Ledger("L").LogKinds())
		return resp
	}
	reject := func(reqs ...*servicepb.Request) {
		t.Helper()
		predicted := checker.modelState.Apply(oracle.Bulk{Requests: reqs})
		require.False(t, predicted.OK)
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", reqs...))
		require.Error(t, err)
		require.Equal(t, predicted.Reason, internal.ErrorReason(err))
	}
	commit(oracletest.AddTypeReq("known"))
	tx := oracletest.TxReq("world", "unknown:1", "USD", 10)
	reject(tx)
	resp := commit(enforcementModeRequest("L", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, false), tx)
	modeLog := resp.GetLogs()[0].GetPayload().GetApply().GetLog().GetData().GetUpdatedDefaultEnforcementMode()
	require.NotNil(t, modeLog)
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, modeLog.GetEnforcementMode())
	commit(actions.SaveAccountMetadataAction("L", "unknown:1", map[string]string{"key": "value"}))
	// A strict flip preceding an unmatched posting is rolled back with it.
	reject(enforcementModeRequest("L", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, true), tx)
	info, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: "L"})
	require.NoError(t, err)
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, info.GetDefaultEnforcementMode())
	// The nested setter takes effect for both metadata and revert validation.
	commit(enforcementModeRequest("L", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, true))
	reject(actions.SaveAccountMetadataAction("L", "unknown:1", map[string]string{"key": "rejected"}))
	revert := oracletest.RevertReqL("L", 1, false)
	reject(revert)
	commit(enforcementModeRequest("L", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, true), revert)
	// Successful metadata and IDs after rejection also pin rollback behavior.
	commit(actions.SaveAccountMetadataAction("L", "unknown:1", map[string]string{"key": "after"}))
	info, err = client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: "L"})
	require.NoError(t, err)
	require.Equal(t, checker.modelState.Ledger("L").DefaultEnforcementMode(), info.GetDefaultEnforcementMode())
}

func TestMirrorBulkRejectsFirstFailingRequest(t *testing.T) {
	t.Parallel()
	create := &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{
		Name:         "L",
		Mode:         commonpb.LedgerMode_LEDGER_MODE_MIRROR,
		MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "source"},
		AccountTypes: map[string]*commonpb.AccountType{
			"known": {Name: "known", Pattern: "known:{id}"},
		},
	}}}
	tx := oracletest.TxReq("world", "known:1", "USD", 1)
	duplicate := actions.AddAccountTypeAction("L", "known", "known:{id}")
	state := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: []*servicepb.Request{create}}).State
	bulk := oracle.Bulk{Requests: []*servicepb.Request{tx, duplicate}}
	require.Equal(t, "LEDGER_IN_MIRROR_MODE", state.Apply(bulk).Reason)
}
