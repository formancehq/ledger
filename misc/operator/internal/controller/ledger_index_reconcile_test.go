package controller

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestReconcileIndexes_CreateConflictDoesNotAdopt(t *testing.T) {
	t.Parallel()

	for _, firstCreated := range []bool{false, true} {
		name := "first create conflicts"
		if firstCreated {
			name = "preserves earlier successful create"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			ledger := &ledgerv1alpha1.Ledger{}
			ledger.Spec.Name = "L"
			ledger.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{Transaction: []string{"timestamp"}}
			if firstCreated {
				ledger.Spec.Indexes.Transaction = []string{"reference", "timestamp"}
			}

			conflict := errors.New("ledgerctl indexes: index already exists: tx_builtin:TX_BUILTIN_INDEX_TIMESTAMP")
			var commands [][]string
			synced, err := reconcileIndexesWithExec(t.Context(), ledger, func(args ...string) (string, error) {
				commands = append(commands, args)
				if args[1] == "list" {
					return "[]", nil
				}
				if args[len(args)-1] == "reference" {
					return "", nil
				}

				return "", conflict
			})
			require.ErrorIs(t, err, conflict)
			require.False(t, synced)
			if firstCreated {
				require.Equal(t, []string{"reference"}, ledger.Status.AppliedIndexes)
				require.Len(t, commands, 3)
			} else {
				require.Empty(t, ledger.Status.AppliedIndexes)
				require.Len(t, commands, 2)
			}

			// The next reconciliation observes the externally created index and
			// converges without retrying creation or claiming it as owned.
			commands = nil
			synced, err = reconcileIndexesWithExec(t.Context(), ledger, func(args ...string) (string, error) {
				commands = append(commands, args)
				require.Equal(t, []string{"indexes", "list", "--ledger", "L", "--json"}, args)

				return `[{"id":{"txBuiltin":"TX_BUILTIN_INDEX_TIMESTAMP"}}, {"id":{"txBuiltin":"TX_BUILTIN_INDEX_REFERENCE"}}]`, nil
			})
			require.NoError(t, err)
			require.True(t, synced)
			require.Len(t, commands, 1)
			require.NotContains(t, ledger.Status.AppliedIndexes, "timestamp")
		})
	}
}
