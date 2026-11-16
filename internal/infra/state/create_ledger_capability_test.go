package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestCreateLedgerCapabilitySelectsApplySemantics(t *testing.T) {
	t.Parallel()

	invalidConstraint := map[string]*commonpb.AccountType{"users": {
		Pattern: "users:{id}", SegmentTypes: map[string]*commonpb.SegmentType{
			"unknown": {Constraint: &commonpb.SegmentType_Uuid{Uuid: &commonpb.UUIDConstraint{}}},
		},
	}}
	conflicting := map[string]*commonpb.AccountType{
		"users-by-id": {Pattern: "users:{id}"}, "users-by-name": {Pattern: "users:{name}"},
	}
	for _, tc := range []struct {
		name     string
		types    map[string]*commonpb.AccountType
		marked   bool
		wantKind string
	}{
		{name: "historical invalid constraint", types: invalidConstraint},
		{name: "marked invalid constraint", types: invalidConstraint, marked: true, wantKind: "invalid"},
		{name: "historical conflict", types: conflicting},
		{name: "marked conflict", types: conflicting, marked: true, wantKind: "conflict"},
		{name: "marked valid", types: map[string]*commonpb.AccountType{"users": {Pattern: "users:{id}"}}, marked: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			machine, store, _ := newTestMachine(t)
			policy := machine.State.ClusterPolicy.CloneVT()
			policy.CapabilityActivations = []*commonpb.CapabilityActivation{{Id: uint32(capabilities.CreateLedgerAccountTypesV2), Active: true}}
			machine.State.UpdateClusterPolicy(policy)
			order := createLedgerOrderWithAccountTypes("l", tc.types)
			proposal := makeProposal(1, order)
			if tc.marked {
				proposal.RequiredCapabilities = []uint32{uint32(capabilities.CreateLedgerAccountTypesV2)}
			}
			results, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 1, proposal))
			require.NoError(t, err)
			require.Len(t, results.Results, 1)
			result := results.Results[0]
			require.True(t, result.AuditEntryWritten)
			switch tc.wantKind {
			case "invalid":
				var invalid *domain.ErrInvalidPattern
				require.ErrorAs(t, result.Error, &invalid)
				require.EqualValues(t, 1, machine.State.NextLedgerID)
			case "conflict":
				var conflict *domain.ErrAccountTypeConflict
				require.ErrorAs(t, result.Error, &conflict)
				require.EqualValues(t, 1, machine.State.NextLedgerID)
			default:
				require.NoError(t, result.Error)
				require.Len(t, result.Logs, 1)
				require.EqualValues(t, 2, machine.State.NextLedgerID)
			}
		})
	}
}
