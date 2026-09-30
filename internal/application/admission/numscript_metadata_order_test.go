package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	commonpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

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
