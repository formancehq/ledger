package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestReplayRejectsChangedApplyOutcomes(t *testing.T) {
	t.Parallel()
	first := oracle.NewGlobalState().Apply(bulkOf(oracletest.TxReqRefL("L", "ref", "world", "account", "USD", 1)))
	skipped := oracletest.TxReqRefL("L", "ref", "world", "account", "USD", 2)
	skipped.GetApply().SkippableReasons = []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT}
	bulk := bulkOf(skipped, enforcementModeRequest("L", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, true))
	result := first.State.Apply(bulk)
	require.True(t, result.OK)
	logs := []*commonpb.Log{
		replayApplyLog("L", 2, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_OrderSkipped{OrderSkipped: result.Orders[0].Skipped.CloneVT()}}),
		replayApplyLog("L", 3, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_UpdatedDefaultEnforcementMode{UpdatedDefaultEnforcementMode: &commonpb.UpdatedDefaultEnforcementModeLog{EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT}}}),
	}
	require.True(t, replayOrdersMatch(bulk, result.Orders, logs))
	for name, mutate := range map[string]func([]*commonpb.Log){
		"missing skip": func(logs []*commonpb.Log) {
			logs[0].GetPayload().GetApply().GetLog().Data = logs[1].GetPayload().GetApply().GetLog().GetData().CloneVT()
		},
		"skip reason": func(logs []*commonpb.Log) {
			logs[0].GetPayload().GetApply().GetLog().GetData().GetOrderSkipped().Reason = commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
		},
		"skip correlator": func(logs []*commonpb.Log) {
			logs[0].GetPayload().GetApply().GetLog().GetData().GetOrderSkipped().Context["reference"] = "other"
		},
		"mode": func(logs []*commonpb.Log) {
			logs[1].GetPayload().GetApply().GetLog().GetData().GetUpdatedDefaultEnforcementMode().EnforcementMode = commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
		},
		"log ID": func(logs []*commonpb.Log) { logs[0].GetPayload().GetApply().GetLog().Id = 9 },
		"ledger": func(logs []*commonpb.Log) { logs[0].GetPayload().GetApply().LedgerName = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			changed := []*commonpb.Log{logs[0].CloneVT(), logs[1].CloneVT()}
			mutate(changed)
			require.False(t, replayOrdersMatch(bulk, result.Orders, changed))
		})
	}
	require.False(t, replayOrdersMatch(bulk, result.Orders, append(logs, logs[0])))
}

func replayApplyLog(ledger string, id uint64, data *commonpb.LedgerLogPayload) *commonpb.Log {
	return &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{LedgerName: ledger, Log: &commonpb.LedgerLog{Id: id, Data: data}}}}}
}

func TestReplayRejectsChangedNestedChartOutcomes(t *testing.T) {
	t.Parallel()
	add := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "L", Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddAccountType{AddAccountType: &servicepb.AddAccountTypeRequest{AccountType: &commonpb.AccountType{Name: "known", Pattern: "known:{id}"}}}}}}}
	remove := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "L", Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RemoveAccountType{RemoveAccountType: &servicepb.RemoveAccountTypeRequest{Name: "known"}}}}}}
	bulk := bulkOf(add, remove)
	result := oracle.NewGlobalState().Apply(bulk)
	require.True(t, result.OK)
	logs := []*commonpb.Log{
		replayApplyLog("L", 1, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_AddedAccountType{AddedAccountType: &commonpb.AddedAccountTypeLog{AccountType: add.GetApply().GetAction().GetAddAccountType().GetAccountType().CloneVT()}}}),
		replayApplyLog("L", 2, &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_RemovedAccountType{RemovedAccountType: &commonpb.RemovedAccountTypeLog{Name: "known"}}}),
	}
	require.True(t, replayOrdersMatch(bulk, result.Orders, logs))
	for name, mutate := range map[string]func([]*commonpb.Log){
		"added pattern": func(logs []*commonpb.Log) {
			logs[0].GetPayload().GetApply().GetLog().GetData().GetAddedAccountType().GetAccountType().Pattern = "other:{id}"
		},
		"removed name": func(logs []*commonpb.Log) {
			logs[1].GetPayload().GetApply().GetLog().GetData().GetRemovedAccountType().Name = "other"
		},
		"missing added payload": func(logs []*commonpb.Log) { logs[0].GetPayload().GetApply().GetLog().Data = nil },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			changed := []*commonpb.Log{logs[0].CloneVT(), logs[1].CloneVT()}
			mutate(changed)
			require.False(t, replayOrdersMatch(bulk, result.Orders, changed))
		})
	}
}
