package catalog

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func createLedgerProposal() *raftcmdpb.Proposal {
	return &raftcmdpb.Proposal{Payload: &raftcmdpb.Proposal_Batch{Batch: &raftcmdpb.ExecutionBatch{
		Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger: "l", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
		}}}},
	}}}
}

func TestCreateLedgerCapabilitySelectionAndFinalPolicyGate(t *testing.T) {
	t.Parallel()
	active := &commonpb.ClusterPolicy{CapabilityActivations: []*commonpb.CapabilityActivation{{Id: uint32(CreateLedgerAccountTypesV2), Active: true}}}
	for _, tc := range []struct {
		name   string
		policy *commonpb.ClusterPolicy
		marked bool
	}{
		{name: "inactive"},
		{name: "active", policy: active, marked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			proposal := createLedgerProposal()
			SelectCreateLedgerValidation(proposal, tc.policy)
			require.Equal(t, tc.marked, len(proposal.GetRequiredCapabilities()) != 0)
			declared := make([]capabilities.CapabilityID, len(proposal.GetRequiredCapabilities()))
			for i, id := range proposal.GetRequiredCapabilities() {
				declared[i] = capabilities.CapabilityID(id)
			}
			require.NoError(t, ValidateCreateLedgerDeclaration(tc.policy, true, declared))
		})
	}
	require.ErrorIs(t, ValidateCreateLedgerDeclaration(active, true, nil), capabilities.ErrMissingCapability)
	require.NoError(t, ValidateCreateLedgerDeclaration(active, false, nil))
}
