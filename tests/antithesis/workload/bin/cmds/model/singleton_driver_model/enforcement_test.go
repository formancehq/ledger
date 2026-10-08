package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestGenerateEnforcementMode(t *testing.T) {
	t.Parallel()
	for range 100 {
		req := generateEnforcementMode("ledger")
		result := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: []*ledgerpb.Request{req}})
		require.True(t, result.OK)
		require.Equal(t, "ledger", oracle.LedgerOf(req))
		require.Contains(t, []ledgerpb.ChartEnforcementMode{ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT}, result.State.Ledger("ledger").DefaultEnforcementMode())
		require.Equal(t, []uint64{1}, result.State.Ledger("ledger").LogIDs())
	}
}

func TestLedgerReadRequiresModeFromSameSnapshot(t *testing.T) {
	t.Parallel()
	base := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: []*ledgerpb.Request{oracletest.AddTypeReq("known")}}).State
	audit := base.Apply(oracle.Bulk{Requests: []*ledgerpb.Request{enforcementModeRequest("L", ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, false)}}).State
	chart := map[string]*ledgerpb.AccountType{"known": {Name: "known", Pattern: "known:{id}"}}
	require.True(t, ledgerReadMatches(audit.Ledger("L"), chart, nil, ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT))
	require.False(t, ledgerReadMatches(audit.Ledger("L"), chart, nil, ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT))
	require.False(t, ledgerReadMatches(base.Ledger("L"), chart, nil, ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT))
	require.False(t, ledgerReadMatches(audit.Ledger("L"), nil, nil, ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT))
}
