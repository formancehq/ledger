package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func enforcementRequest(mode commonpb.ChartEnforcementMode, nested bool) *servicepb.Request {
	if nested {
		return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "L", Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeRequest{EnforcementMode: mode}}}}}}
	}

	return &servicepb.Request{Type: &servicepb.Request_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeLedgerRequest{Ledger: "L", EnforcementMode: mode}}}
}

func TestGlobalState_Apply_EnforcementOrdering(t *testing.T) {
	t.Parallel()
	for _, nested := range []bool{false, true} {
		t.Run(map[bool]string{false: "top-level", true: "action"}[nested], func(t *testing.T) {
			t.Parallel()
			base := NewGlobalState().Apply(bulkOf(oracletest.AddTypeReq("known")))
			require.True(t, base.OK)
			rejected := base.State.Apply(bulkOf(oracletest.TxReq("world", "unknown:1", "USD", 5)))
			require.Equal(t, domain.ErrReasonAccountNotMatchingType, rejected.Reason)
			accepted := base.State.Apply(bulkOf(enforcementRequest(commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, nested), oracletest.TxReq("world", "unknown:1", "USD", 5)))
			require.True(t, accepted.OK)
			require.Equal(t, []uint64{1, 2, 3}, accepted.State.Ledger("L").LogIDs())
			modeLog := accepted.State.Ledger("L").LogRows()[1]
			require.Equal(t, "updated_default_enforcement_mode", modeLog.Kind)
			require.Equal(t, "mode=1", modeLog.Payload)
			require.Equal(t, rejected.State.Fingerprint(), base.State.Fingerprint(), "fork must preserve strict behavior")
			rolledBack := accepted.State.Apply(bulkOf(enforcementRequest(commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, nested), oracletest.TxReq("world", "unknown:2", "USD", 5)))
			require.Equal(t, domain.ErrReasonAccountNotMatchingType, rolledBack.Reason)
			require.Equal(t, accepted.State.Fingerprint(), rolledBack.State.Fingerprint())
		})
	}
}

func TestGlobalState_Apply_EnforcementAffectedOrders(t *testing.T) {
	t.Parallel()
	base := NewGlobalState().Apply(bulkOf(oracletest.TxReq("world", "unknown:1", "USD", 5), oracletest.AddTypeReq("known")))
	require.True(t, base.OK)
	audit := base.State.Apply(bulkOf(enforcementRequest(commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, true)))
	require.True(t, audit.OK)
	for name, req := range map[string]*servicepb.Request{
		"revert":           oracletest.RevertReqL("L", 1, false),
		"account metadata": oracletest.AddAccountMetaReq("unknown:1", "key", &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "value"}}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rejected := base.State.Apply(bulkOf(req))
			require.False(t, rejected.OK)
			require.Equal(t, domain.ErrReasonAccountNotMatchingType, rejected.Reason)
			accepted := audit.State.Apply(bulkOf(req))
			require.True(t, accepted.OK)
			require.NotEqual(t, audit.State.Fingerprint(), accepted.State.Fingerprint())
		})
	}
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, base.State.Ledger("L").DefaultEnforcementMode())
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, audit.State.Ledger("L").DefaultEnforcementMode())
}

func TestGlobalState_Apply_EnforcementLedgerIsolation(t *testing.T) {
	t.Parallel()
	base := NewGlobalState()
	for _, ledger := range []string{"L", "other"} {
		req := oracletest.AddTypeReq("known")
		req.GetAddAccountType().Ledger = ledger
		applied := base.Apply(bulkOf(req))
		require.True(t, applied.OK)
		base = applied.State
	}
	changed := base.Apply(bulkOf(enforcementRequest(commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, false)))
	require.True(t, changed.OK)
	require.True(t, changed.State.Apply(bulkOf(oracletest.TxReq("world", "unknown:1", "USD", 1))).OK)
	rejected := changed.State.Apply(bulkOf(oracletest.TxReqL("other", "world", "unknown:1", "USD", 1)))
	require.Equal(t, domain.ErrReasonAccountNotMatchingType, rejected.Reason)
	require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, changed.State.Ledger("other").DefaultEnforcementMode())
}
