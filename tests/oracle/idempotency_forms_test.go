package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func TestIdempotencyEquivalentRequestForms(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		top    *ledgerpb.Request
		action *ledgerpb.LedgerAction
	}{
		{"mode", enforcementRequest(ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, false), &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_SetDefaultEnforcementMode{SetDefaultEnforcementMode: &ledgerpb.SetDefaultEnforcementModeRequest{EnforcementMode: ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT}}}},
		{"add type", oracletest.AddTypeReq("known"), &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_AddAccountType{AddAccountType: &ledgerpb.AddAccountTypeRequest{AccountType: oracletest.AddTypeReq("known").GetAddAccountType().GetAccountType()}}}},
		{"remove type", oracletest.RemoveTypeReq("known"), &ledgerpb.LedgerAction{Data: &ledgerpb.LedgerAction_RemoveAccountType{RemoveAccountType: &ledgerpb.RemoveAccountTypeRequest{Name: "known"}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			nested := &ledgerpb.Request{Type: &ledgerpb.Request_Apply{Apply: &ledgerpb.LedgerApplyRequest{Ledger: "L", Action: tc.action}}}
			before := tc.top.CloneVT()
			require.True(t, RequestsEqual([]*ledgerpb.Request{tc.top}, []*ledgerpb.Request{nested}))
			require.True(t, RequestsEqual([]*ledgerpb.Request{nested}, []*ledgerpb.Request{tc.top}))
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
			require.False(t, RequestsEqual([]*ledgerpb.Request{tc.top}, []*ledgerpb.Request{changed}))
			changed = nested.CloneVT()
			changed.GetApply().SkippableReasons = []ledgerpb.ErrorReason{ledgerpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_ALREADY_EXISTS}
			require.False(t, RequestsEqual([]*ledgerpb.Request{tc.top}, []*ledgerpb.Request{changed}))
		})
	}
}

func TestIdempotencyChangedModeConflicts(t *testing.T) {
	t.Parallel()
	first := NewGlobalState().Apply(keyedBulk("mode", enforcementRequest(ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, false)))
	got := first.State.Apply(keyedBulk("mode", enforcementRequest(ledgerpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT, true)))
	require.Equal(t, domain.ErrReasonIdempotencyKeyConflict, got.Reason)
}
