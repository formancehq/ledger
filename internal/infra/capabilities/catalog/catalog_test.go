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

func TestValidateDeclarationRejectsUnsupportedID(t *testing.T) {
	t.Parallel()
	proposal := &raftcmdpb.Proposal{RequiredCapabilities: []uint32{99}}
	_, err := ValidateDeclaration(proposal)
	require.ErrorIs(t, err, capabilities.ErrInvalidDeclaration)
}

func TestCreateLedgerCapabilitySelectionAndFinalValidation(t *testing.T) {
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
			declared, err := ValidateDeclaration(proposal)
			require.NoError(t, err)
			require.Equal(t, tc.marked, len(declared) != 0)
			data, err := proposal.MarshalVT()
			require.NoError(t, err)
			require.NoError(t, ValidateSerializedDeclaration(data, declared))
			require.NoError(t, ValidateCreateLedgerDeclaration(tc.policy, true, declared))
		})
	}
	require.ErrorIs(t, ValidateCreateLedgerDeclaration(active, true, nil), capabilities.ErrMissingCapability)
	require.NoError(t, ValidateCreateLedgerDeclaration(active, false, nil))
}

func TestCreateLedgerCapabilityRejectsUnrelatedDeclaration(t *testing.T) {
	t.Parallel()
	proposal := &raftcmdpb.Proposal{RequiredCapabilities: []uint32{uint32(CreateLedgerAccountTypesV2)}}
	_, err := ValidateDeclaration(proposal)
	require.ErrorIs(t, err, capabilities.ErrInvalidDeclaration)

	proposal = createLedgerProposal()
	proposal.RequiredCapabilities = []uint32{uint32(CreateLedgerAccountTypesV2), uint32(CreateLedgerAccountTypesV2)}
	_, err = ValidateDeclaration(proposal)
	require.ErrorIs(t, err, capabilities.ErrInvalidDeclaration)
}
