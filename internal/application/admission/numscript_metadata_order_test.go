package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestExecuteNumscriptOrderIsPlannedAndValidated(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)
	request := &servicepb.LedgerApplyRequest{
		Ledger: testLedgerName,
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_ExecuteNumscript{ExecuteNumscript: &servicepb.ExecuteNumscriptPayload{
			Script: &commonpb.Script{Plain: `set_account_meta(@users:alice, "tier", "gold")`},
		}}},
	}
	apply, err := admission.convertApplyRequest(t.Context(), request, newBulkOverlay())
	require.NoError(t, err)
	require.NotNil(t, apply.GetExecuteNumscript())
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger:  testLedgerName,
		Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: apply},
	}}}
	require.NoError(t, validateOrder(order))
	orders := []*raftcmdpb.Order{order}
	overlay := newBulkOverlay()
	needs, perOrder, err := admission.extractPreloadNeeds(t.Context(), orders, overlay)
	require.NoError(t, err)
	require.NoError(t, admission.resolveScriptsAndEnrichNeeds(t.Context(), orders, overlay, needs, perOrder, false))
}

func TestExecuteNumscriptReferenceIsPlanned(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)
	request := &servicepb.LedgerApplyRequest{
		Ledger: testLedgerName,
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_ExecuteNumscript{ExecuteNumscript: &servicepb.ExecuteNumscriptPayload{
			Script:    &commonpb.Script{Plain: `send [USD/2 100] (source = @world destination = @users:alice)`},
			Reference: "ref-1",
		}}},
	}
	apply, err := admission.convertApplyRequest(t.Context(), request, newBulkOverlay())
	require.NoError(t, err)
	require.Equal(t, "ref-1", apply.GetExecuteNumscript().GetReference())
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger:  testLedgerName,
		Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: apply},
	}}}
	needs, _, err := admission.extractPreloadNeeds(t.Context(), []*raftcmdpb.Order{order}, newBulkOverlay())
	require.NoError(t, err)
	require.NotNil(t, needs)
}

func TestNumscriptCompetingMetadataErrorsReachProposalPreparation(t *testing.T) {
	t.Parallel()
	store := createTestStore(t)
	admission, _ := createTestAdmission(t, store)
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: testLedgerName,
		Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{
			Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: &raftcmdpb.CreateTransactionOrder{
				Script: &commonpb.Script{Plain: `
					vars { string $poison }
					set_tx_meta("a", $poison)
					set_tx_meta("b", $poison)
					send [USD/2 100] (source = @world destination = @users:alice)
				`, Vars: map[string]string{"poison": "safe\x00poison"}},
			}},
		}},
	}}}
	orders := []*raftcmdpb.Order{order}
	overlay := newBulkOverlay()
	needs, perOrder, err := admission.extractPreloadNeeds(context.Background(), orders, overlay)
	require.NoError(t, err)
	require.NoError(t, admission.resolveScriptsAndEnrichNeeds(context.Background(), orders, overlay, needs, perOrder, true))
	require.False(t, order.GetTechnical().GetPreloadUnavailable())
}
