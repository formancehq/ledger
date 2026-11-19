package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities/catalog"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestAdmitCreateLedgerSelectsCommittedCapability(t *testing.T) {
	t.Parallel()
	h := newMetadataReplayHarness(t)
	request := func(name string) *servicepb.ApplyRequest {
		return servicepb.UnsignedApplyRequest("", &servicepb.Request{Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name: name, AccountTypes: map[string]*commonpb.AccountType{"users": {
					Pattern: "users:{id}", SegmentTypes: map[string]*commonpb.SegmentType{
						"unknown": {Constraint: &commonpb.SegmentType_Uuid{Uuid: &commonpb.UUIDConstraint{}}},
					},
				}},
			},
		}})
	}

	_, err := h.admission.Admit(attributedTestContext(t.Context()), request("legacy"))
	require.NoError(t, err)
	require.Len(t, h.proposals, 1)
	require.Empty(t, h.proposals[0].GetRequiredCapabilities())

	policy := h.machine.State.ClusterPolicy.CloneVT()
	policy.Revision++
	policy.CapabilityActivations = []*commonpb.CapabilityActivation{{Id: uint32(catalog.CreateLedgerAccountTypesV2), Active: true}}
	batch := h.admission.store.OpenWriteSession()
	require.NoError(t, state.SaveClusterPolicy(batch, policy))
	require.NoError(t, batch.Commit())
	h.machine.State.UpdateClusterPolicy(policy)

	_, err = h.admission.Admit(attributedTestContext(t.Context()), request("enforced"))
	var invalid *domain.ErrInvalidPattern
	require.ErrorAs(t, err, &invalid)
	require.Len(t, h.proposals, 2)
	require.Equal(t, []uint32{uint32(catalog.CreateLedgerAccountTypesV2)}, h.proposals[1].GetRequiredCapabilities())
}
