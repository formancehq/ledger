package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestReplayRejectsChangedApplyOutcomes(t *testing.T) {
	t.Parallel()
	first := oracle.NewGlobalState().Apply(bulkOf(oracletest.TxReqRefL("L", "ref", "world", "account", "USD", 1)))
	skipped := oracletest.TxReqRefL("L", "ref", "world", "account", "USD", 2)
	skipped.GetApply().SkippableReasons = []ledgerpb.ErrorReason{ledgerpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT}
	bulk := bulkOf(skipped, enforcementModeRequest("L", ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, true))
	result := first.State.Apply(bulk)
	require.True(t, result.OK)
	logs := []*ledgerpb.Log{
		replayApplyLog("L", 2, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_OrderSkipped{OrderSkipped: result.Orders[0].Skipped.CloneVT()}}),
		replayApplyLog("L", 3, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_UpdatedDefaultEnforcementMode{UpdatedDefaultEnforcementMode: &ledgerpb.UpdatedDefaultEnforcementModeLog{EnforcementMode: ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT}}}),
	}
	require.True(t, replayOrdersMatch(bulk, result.Orders, logs))
	for name, mutate := range map[string]func([]*ledgerpb.Log){
		"missing skip": func(logs []*ledgerpb.Log) {
			logs[0].GetPayload().GetApply().GetLog().Data = logs[1].GetPayload().GetApply().GetLog().GetData().CloneVT()
		},
		"skip reason": func(logs []*ledgerpb.Log) {
			logs[0].GetPayload().GetApply().GetLog().GetData().GetOrderSkipped().Details = ledgerpb.MustFailureDetails("METADATA_NOT_FOUND", map[string]string{"target": "account", "key": "k"})
		},
		"skip correlator": func(logs []*ledgerpb.Log) {
			other := "other"
			logs[0].GetPayload().GetApply().GetLog().GetData().GetOrderSkipped().GetDetails().GetTransactionReferenceConflict().Reference = &other
		},
		"mode": func(logs []*ledgerpb.Log) {
			logs[1].GetPayload().GetApply().GetLog().GetData().GetUpdatedDefaultEnforcementMode().EnforcementMode = ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT
		},
		"log ID": func(logs []*ledgerpb.Log) { logs[0].GetPayload().GetApply().GetLog().Id = 9 },
		"ledger": func(logs []*ledgerpb.Log) { logs[0].GetPayload().GetApply().LedgerName = "other" },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			changed := []*ledgerpb.Log{logs[0].CloneVT(), logs[1].CloneVT()}
			mutate(changed)
			require.False(t, replayOrdersMatch(bulk, result.Orders, changed))
		})
	}
	require.False(t, replayOrdersMatch(bulk, result.Orders, append(logs, logs[0])))
}

func replayApplyLog(ledger string, id uint64, data *ledgerpb.LedgerLogPayload) *ledgerpb.Log {
	return &ledgerpb.Log{Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_Apply{Apply: &ledgerpb.ApplyLedgerLog{LedgerName: ledger, Log: &ledgerpb.LedgerLog{Id: id, Data: data}}}}}
}

func TestReplayRejectsChangedNestedChartOutcomes(t *testing.T) {
	t.Parallel()
	add := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "L", Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddAccountType{AddAccountType: &servicepb.AddAccountTypeRequest{AccountType: &ledgerpb.AccountType{Name: "known", Pattern: "known:{id}"}}}}}}}
	remove := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "L", Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RemoveAccountType{RemoveAccountType: &servicepb.RemoveAccountTypeRequest{Name: "known"}}}}}}
	bulk := bulkOf(add, remove)
	result := oracle.NewGlobalState().Apply(bulk)
	require.True(t, result.OK)
	logs := []*ledgerpb.Log{
		replayApplyLog("L", 1, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_AddedAccountType{AddedAccountType: &ledgerpb.AddedAccountTypeLog{AccountType: add.GetApply().GetAction().GetAddAccountType().GetAccountType().CloneVT()}}}),
		replayApplyLog("L", 2, &ledgerpb.LedgerLogPayload{Payload: &ledgerpb.LedgerLogPayload_RemovedAccountType{RemovedAccountType: &ledgerpb.RemovedAccountTypeLog{Name: "known"}}}),
	}
	require.True(t, replayOrdersMatch(bulk, result.Orders, logs))
	for name, mutate := range map[string]func([]*ledgerpb.Log){
		"added pattern": func(logs []*ledgerpb.Log) {
			logs[0].GetPayload().GetApply().GetLog().GetData().GetAddedAccountType().GetAccountType().Pattern = "other:{id}"
		},
		"removed name": func(logs []*ledgerpb.Log) {
			logs[1].GetPayload().GetApply().GetLog().GetData().GetRemovedAccountType().Name = "other"
		},
		"missing added payload": func(logs []*ledgerpb.Log) { logs[0].GetPayload().GetApply().GetLog().Data = nil },
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			changed := []*ledgerpb.Log{logs[0].CloneVT(), logs[1].CloneVT()}
			mutate(changed)
			require.False(t, replayOrdersMatch(bulk, result.Orders, changed))
		})
	}
}
