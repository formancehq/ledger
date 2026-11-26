package controller

import (
	"context"
	"errors"
	"slices"
	"strings"
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
		"ledgers", "create", "--index", "reference", "--index", "inserted-at", "--index", "account-asset",
		"--schema", "transaction:external:id:string", "--index", "metadata:transaction:external:id",
		"--idempotency-key", "operator-ledger-create:creation-uid:3",
		"--", "L",
	}, args)
	replay, err := r.buildCreateArgs(t.Context(), ledger)
	require.NoError(t, err)
	require.Equal(t, args, replay)
	ledger.Generation++
	next, err := r.buildCreateArgs(t.Context(), ledger)
	require.NoError(t, err)
	idempotencyKey := func(args []string) string {
		i := slices.Index(args, "--idempotency-key")
		require.GreaterOrEqual(t, i, 0)

		return args[i+1]
	}
	require.NotEqual(t, idempotencyKey(args), idempotencyKey(next))
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
	ledger.Status.AppliedIndexes = nil // Simulate restart or a failed status write.
	var dropped []string
	_, err = reconcileIndexesWithExec(t.Context(), ledger, func(args ...string) (string, error) {
		if args[0] == "audit" {
			return initialAuditFixture, nil
		}
		if args[1] == "list" {
			if slices.Contains(args, "--creation-key-prefix") {
				return "[]", nil // The initial multi-order batch is not a singleton create.
			}

			return `[{"id":{"txBuiltin":"TX_BUILTIN_INDEX_REFERENCE"},"ledger":"L","createdAt":"1970-01-01T00:00:00.001234Z"},{"id":{"txBuiltin":"TX_BUILTIN_INDEX_TIMESTAMP"},"ledger":"L","createdAt":"1970-01-01T00:00:00.001234Z"}]`, nil
		}
		dropped = append(dropped, args[len(args)-1])

		return "", nil
	})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"reference", "timestamp"}, dropped)
	require.Empty(t, ledger.Status.AppliedIndexes)
}

const initialAuditFixture = `[{"entry":{"timestamp":{"data":"1234"},"idempotency":{"key":"operator-ledger-create:creation-uid:1"},"success":{"minLogSequence":"8","maxLogSequence":"10"},"orderCount":3,"items":[{"logSequence":"8"},{"orderIndex":1,"logSequence":"9"},{"orderIndex":2,"logSequence":"10"}]},"orders":[{"ledgerScoped":{"ledger":"L","createLedger":{}}},{"ledgerScoped":{"ledger":"L","apply":{"createIndex":{"id":{"txBuiltin":"TX_BUILTIN_INDEX_REFERENCE"}}}}},{"ledgerScoped":{"ledger":"L","apply":{"createIndex":{"id":{"txBuiltin":"TX_BUILTIN_INDEX_TIMESTAMP"}}}}}]}]`

func TestInitialIndexesFromAuditRejectsUnownedBatches(t *testing.T) {
	t.Parallel()
	current := map[string]string{"reference": "1234", "timestamp": "1234"}
	prefix := "operator-ledger-create:creation-uid:"
	owned, err := initialIndexesFromAudit(initialAuditFixture, "L", prefix, current)
	require.NoError(t, err)
	require.Equal(t, map[string]bool{"reference": true, "timestamp": true}, owned)
	for name, changed := range map[string]string{
		"wrong uid":      strings.Replace(initialAuditFixture, "creation-uid:1", "other-uid:1", 1),
		"failed":         strings.Replace(initialAuditFixture, `"success":{"minLogSequence":"8","maxLogSequence":"10"}`, `"failure":{}`, 1),
		"replay":         strings.Replace(initialAuditFixture, `"logSequence":"9"`, `"logSequence":"0"`, 1),
		"wrong ledger":   strings.Replace(initialAuditFixture, `"ledger":"L"`, `"ledger":"other"`, 1),
		"missing create": strings.Replace(initialAuditFixture, `"createLedger":{}`, `"deleteLedger":{}`, 1),
		"other order":    strings.Replace(initialAuditFixture, `"createIndex":{"id":{"txBuiltin":"TX_BUILTIN_INDEX_REFERENCE"}}`, `"deleteIndex":{}`, 1),
		"duplicate":      strings.Replace(initialAuditFixture, `"TX_BUILTIN_INDEX_TIMESTAMP"`, `"TX_BUILTIN_INDEX_REFERENCE"`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got, err := initialIndexesFromAudit(changed, "L", prefix, current)
			require.NoError(t, err)
			require.Empty(t, got)
		})
	}
	owned, err = initialIndexesFromAudit(initialAuditFixture, "L", prefix, map[string]string{"reference": "1235", "timestamp": "1235"})
	require.NoError(t, err)
	require.Empty(t, owned)
}

func TestCreateLedgerDoesNotClaimExternalIndexes(t *testing.T) {
	t.Parallel()
	ledger := newLedger("test", "default", "cluster", "L")
	ledger.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{Transaction: []string{"reference"}}
	err := createLedgerWithExec(ledger, nil, func(...string) error { return errors.New("ledger already exists") })
	require.Error(t, err)
	require.Empty(t, ledger.Status.AppliedIndexes)
}
