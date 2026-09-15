package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestBuildCreateArgsIncludesInitialIndexes(t *testing.T) {
	t.Parallel()
	ledger := newLedger("test", "default", "cluster", "L")
	ledger.UID = types.UID("creation-uid")
	ledger.Generation = 3
	ledger.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{
		Transaction: []string{"reference", "insertedAt"},
		Account:     []string{"asset"},
		Metadata:    []ledgerv1alpha1.MetadataIndexSpec{{Target: "transaction", Key: "external:id", Type: "string"}},
	}
	r := newTestLedgerReconciler()
	args, err := r.buildCreateArgs(t.Context(), ledger)
	require.NoError(t, err)
	require.Equal(t, []string{
		"ledgers", "create", "--name", "L", "--index", "reference", "--index", "inserted-at", "--index", "account-asset",
		"--schema", "transaction:external:id:string", "--index", "metadata:transaction:external:id",
		"--idempotency-key", "operator-ledger-create:creation-uid:3",
	}, args)
	replay, err := r.buildCreateArgs(t.Context(), ledger)
	require.NoError(t, err)
	require.Equal(t, args, replay)
	ledger.Generation++
	next, err := r.buildCreateArgs(t.Context(), ledger)
	require.NoError(t, err)
	require.NotEqual(t, args[len(args)-1], next[len(next)-1])
}

func TestCreateLedgerOwnershipAfterLostResponse(t *testing.T) {
	t.Parallel()
	ledger := newLedger("test", "default", "cluster", "L")
	ledger.UID = types.UID("creation-uid")
	ledger.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{Transaction: []string{"reference", "timestamp"}}
	args, err := newTestLedgerReconciler().buildCreateArgs(t.Context(), ledger)
	require.NoError(t, err)
	attempts := 0
	exec := func(got ...string) error {
		attempts++
		require.Equal(t, args, got)
		if attempts == 1 {
			return context.DeadlineExceeded
		}

		return nil // The server replays the successful atomic creation for its key.
	}
	require.ErrorIs(t, createLedgerWithExec(ledger, args, exec), context.DeadlineExceeded)
	require.Empty(t, ledger.Status.AppliedIndexes)
	require.NoError(t, createLedgerWithExec(ledger, args, exec))
	require.Equal(t, 2, attempts)
	require.Equal(t, []string{"reference", "timestamp"}, ledger.Status.AppliedIndexes)

	// A later managed-empty spec must still remove those owned indexes.
	ledger.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{}
	var dropped []string
	_, err = reconcileIndexesWithExec(t.Context(), ledger, func(args ...string) (string, error) {
		if args[1] == "list" {
			return `[{"id":{"txBuiltin":"TX_BUILTIN_INDEX_REFERENCE"}},{"id":{"txBuiltin":"TX_BUILTIN_INDEX_TIMESTAMP"}}]`, nil
		}
		dropped = append(dropped, args[len(args)-1])

		return "", nil
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"reference", "timestamp"}, dropped)
	require.Empty(t, ledger.Status.AppliedIndexes)
}

func TestCreateLedgerDoesNotClaimExternalIndexes(t *testing.T) {
	t.Parallel()
	ledger := newLedger("test", "default", "cluster", "L")
	ledger.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{Transaction: []string{"reference"}}
	err := createLedgerWithExec(ledger, nil, func(...string) error { return errors.New("ledger already exists") })
	require.Error(t, err)
	require.Empty(t, ledger.Status.AppliedIndexes)
}
