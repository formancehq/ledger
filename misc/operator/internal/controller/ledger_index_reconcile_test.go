package controller

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
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
			ledger.UID = "resource-uid"
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
				if slices.Contains(args, "reference") {
					return "", nil
				}

				return "", conflict
			})
			require.ErrorIs(t, err, conflict)
			require.False(t, synced)
			if firstCreated {
				require.Equal(t, []string{"reference"}, ledger.Status.AppliedIndexes)
				require.Len(t, commands, 4)
			} else {
				require.Empty(t, ledger.Status.AppliedIndexes)
				require.Len(t, commands, 3)
			}

			// The next reconciliation observes the externally created index and
			// converges without retrying creation or claiming it as owned.
			commands = nil
			synced, err = reconcileIndexesWithExec(t.Context(), ledger, func(args ...string) (string, error) {
				commands = append(commands, args)
				require.Equal(t, []string{"indexes", "list", "--ledger", "L", "--json"}, args[:5])
				if slices.Contains(args, "--creation-key-prefix") {
					require.Equal(t, "ledger-operator/index/resource-uid/", args[6])
					if firstCreated {
						return `[{"id":{"txBuiltin":"TX_BUILTIN_INDEX_REFERENCE"}}]`, nil
					}

					return "[]", nil
				}

				return `[{"id":{"txBuiltin":"TX_BUILTIN_INDEX_TIMESTAMP"}}, {"id":{"txBuiltin":"TX_BUILTIN_INDEX_REFERENCE"}}]`, nil
			})
			require.NoError(t, err)
			require.True(t, synced)
			require.Len(t, commands, 2)
			require.NotContains(t, ledger.Status.AppliedIndexes, "timestamp")
		})
	}
}

func TestReconcileIndexesRequiresUID(t *testing.T) {
	t.Parallel()
	ledger := &ledgerv1alpha1.Ledger{}
	ledger.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{}
	_, err := reconcileIndexesWithExec(t.Context(), ledger, func(...string) (string, error) {
		t.Fatal("must not execute a command without a resource UID")

		return "", nil
	})
	require.EqualError(t, err, "index reconciliation requires a persisted Ledger UID")
}

func TestReconcileIndexesAuditFailureDoesNotUseStaleStatus(t *testing.T) {
	t.Parallel()
	for _, malformed := range []bool{false, true} {
		t.Run(map[bool]string{false: "read error", true: "malformed response"}[malformed], func(t *testing.T) {
			t.Parallel()
			ledger := &ledgerv1alpha1.Ledger{}
			ledger.UID = "resource-uid"
			ledger.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{}
			ledger.Status.AppliedIndexes = []string{"reference"}
			calls := 0
			_, err := reconcileIndexesWithExec(t.Context(), ledger, func(args ...string) (string, error) {
				calls++
				require.Equal(t, "list", args[1], "audit failure must never permit a drop")
				if slices.Contains(args, "--creation-key-prefix") {
					if malformed {
						return "broken-json", nil
					}

					return "", errors.New("audit unavailable")
				}

				return `[{"id":{"txBuiltin":"TX_BUILTIN_INDEX_REFERENCE"}}]`, nil
			})
			require.Error(t, err)
			require.Equal(t, 2, calls)
			require.Empty(t, ledger.Status.AppliedIndexes)
		})
	}
}

func TestReconcileIndexesCreationAttemptsHaveDistinctKeys(t *testing.T) {
	t.Parallel()
	ledger := &ledgerv1alpha1.Ledger{}
	ledger.UID = "resource-uid"
	ledger.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{Transaction: []string{"reference", "timestamp"}}
	var keys []string
	for range 2 {
		_, err := reconcileIndexesWithExec(t.Context(), ledger, func(args ...string) (string, error) {
			if args[1] == "list" {
				return "[]", nil
			}
			require.Equal(t, "create", args[1])
			require.Equal(t, "--idempotency-key", args[len(args)-2])
			key := args[len(args)-1]
			require.True(t, strings.HasPrefix(key, "ledger-operator/index/resource-uid/"))
			_, err := uuid.Parse(strings.TrimPrefix(key, "ledger-operator/index/resource-uid/"))
			require.NoError(t, err)
			require.NotContains(t, keys, key)
			keys = append(keys, key)

			return "", nil
		})
		require.NoError(t, err)
	}
	require.Len(t, keys, 4)
}
