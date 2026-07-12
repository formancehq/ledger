package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestProcessAddAccountType_ConflictSelectionDeterministic pins EN-1521: when a
// new account type conflicts with more than one existing type, the selected
// conflict (surfaced in the chain-bound ErrAccountTypeConflict → AuditFailure)
// must be the same on every replica. The processor iterates the existing-types
// map in sorted key order, so the lexicographically-first conflicting name is
// always chosen — never whichever the Go map range happened to visit first.
//
// Go randomizes map iteration order per range, so running the apply repeatedly
// would surface both orderings before the fix; asserting a constant result
// across many runs is the determinism proof.
func TestProcessAddAccountType_ConflictSelectionDeterministic(t *testing.T) {
	t.Parallel()

	// "aaa" and "zzz" have identical two-segment structure (fixed + variable),
	// so both conflict with the new "users:{z}" pattern. "aaa" sorts first.
	existing := &commonpb.LedgerInfo{
		Name: "l",
		AccountTypes: map[string]*commonpb.AccountType{
			"zzz": {Name: "zzz", Pattern: "users:{y}"},
			"aaa": {Name: "aaa", Pattern: "users:{x}"},
		},
	}

	const runs = 64
	for range runs {
		ctrl := gomock.NewController(t)
		mockStore := NewMockScope(ctrl)
		expectGetLedger(mockStore, domain.LedgerKey{Name: "l"}, existing.AsReader(), nil)

		order := &raftcmdpb.AddAccountTypeOrder{
			AccountType: &commonpb.AccountType{Name: "new-type", Pattern: "users:{z}"},
		}

		_, derr := processAddAccountType("l", order, &Context{Scope: mockStore})
		require.NotNil(t, derr)

		var conflict *domain.ErrAccountTypeConflict
		require.ErrorAs(t, derr, &conflict,
			"a multi-conflict add must surface ErrAccountTypeConflict")
		require.Equal(t, "aaa", conflict.ExistingName,
			"the selected conflict must be the lexicographically-first name, deterministically")
		require.Equal(t, "users:{x}", conflict.ExistingPattern)

		ctrl.Finish()
	}
}
