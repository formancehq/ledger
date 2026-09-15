package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestIdempotencyEquivalentRequestForms(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		top    *servicepb.Request
		action *servicepb.LedgerAction
	}{
		{"mode", enforcementRequest(commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, false), &servicepb.LedgerAction{Data: &servicepb.LedgerAction_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &servicepb.SetDefaultEnforcementModeRequest{EnforcementMode: commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT}}}},
		{"add type", oracletest.AddTypeReq("known"), &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddAccountType{AddAccountType: &servicepb.AddAccountTypeRequest{AccountType: oracletest.AddTypeReq("known").GetAddAccountType().GetAccountType()}}}},
		{"remove type", oracletest.RemoveTypeReq("known"), &servicepb.LedgerAction{Data: &servicepb.LedgerAction_RemoveAccountType{RemoveAccountType: &servicepb.RemoveAccountTypeRequest{Name: "known"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nested := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "L", Action: tc.action}}}
			before := tc.top.CloneVT()
			require.True(t, RequestsEqual([]*servicepb.Request{tc.top}, []*servicepb.Request{nested}))
			require.True(t, RequestsEqual([]*servicepb.Request{nested}, []*servicepb.Request{tc.top}))
			require.True(t, before.EqualVT(tc.top), "comparison must not mutate accepted intent")
			for _, reverse := range []bool{false, true} {
				first, second := tc.top, nested
				if reverse {
					first, second = second, first
				}
				base := NewGlobalState()
				if tc.name == "remove type" {
					base = base.Apply(bulkOf(oracletest.AddTypeReq("known"))).State
				}
				original := base.Apply(keyedBulk("key", first))
				require.True(t, original.OK, original.Reason)
				replay := original.State.Apply(keyedBulk("key", second))
				require.True(t, replay.OK, replay.Reason)
				require.Equal(t, original.State.Fingerprint(), replay.State.Fingerprint())
				require.Equal(t, original.Orders, replay.Orders)
			}
			changed := nested.CloneVT()
			changed.GetApply().Ledger = "other"
			require.False(t, RequestsEqual([]*servicepb.Request{tc.top}, []*servicepb.Request{changed}))
			changed = nested.CloneVT()
			changed.GetApply().SkippableReasons = []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS}
			require.False(t, RequestsEqual([]*servicepb.Request{tc.top}, []*servicepb.Request{changed}))
		})
	}
}

func TestIdempotencyChangedModeConflicts(t *testing.T) {
	t.Parallel()
	first := NewGlobalState().Apply(keyedBulk("mode", enforcementRequest(commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, false)))
	got := first.State.Apply(keyedBulk("mode", enforcementRequest(commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, true)))
	require.Equal(t, domain.ErrReasonIdempotencyKeyConflict, got.Reason)
}
