package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/capabilities"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestCreateLedgerRejectsInvalidInitialAccountTypesAtApply(t *testing.T) {
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
		wantKind string
	}{
		{name: "invalid constraint", types: invalidConstraint, wantKind: "invalid"},
		{name: "conflict", types: conflicting, wantKind: "conflict"},
		{name: "valid", types: map[string]*commonpb.AccountType{"users": {Pattern: "users:{id}"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			machine, store, _ := newTestMachine(t)
			order := createLedgerOrderWithAccountTypes("l", tc.types)
			proposal := makeProposal(1, order)
			prepared, err := capabilities.PrepareProposal(proposal)
			require.NoError(t, err)
			require.Empty(t, prepared.RequiredCapabilities(), "RC.5 CreateLedger validation predates capability 1")
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
