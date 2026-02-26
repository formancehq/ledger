//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	policyv1 "k8s.io/api/policy/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func TestReconcile_PDBMinAvailable(t *testing.T) {
	ns := createTestNamespace(t)
	minAvail := int32(2)
	ls := newLedgerService("pdb-min", ns)
	ls.Spec.PodDisruptionBudget = &ledgerv1alpha1.PodDisruptionBudgetSpec{
		Enabled:      true,
		MinAvailable: &minAvail,
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	pdb := &policyv1.PodDisruptionBudget{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "pdb-min", Namespace: ns}, pdb) == nil
	}, "PDB should be created")

	require.NotNil(t, pdb.Spec.MinAvailable)
	assert.Equal(t, int32(2), pdb.Spec.MinAvailable.IntVal)

	// Selector should match the LedgerService selector labels
	require.NotNil(t, pdb.Spec.Selector)
	assert.Equal(t, "ledger", pdb.Spec.Selector.MatchLabels[labelName])
	assert.Equal(t, "pdb-min", pdb.Spec.Selector.MatchLabels[labelInstance])

	requireOwnerRef(t, pdb.OwnerReferences, "pdb-min")
}

func TestReconcile_PDBDisabledCleansUp(t *testing.T) {
	ns := createTestNamespace(t)
	minAvail := int32(2)
	ls := newLedgerService("pdb-cleanup", ns)
	ls.Spec.PodDisruptionBudget = &ledgerv1alpha1.PodDisruptionBudgetSpec{
		Enabled:      true,
		MinAvailable: &minAvail,
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Wait for PDB creation
	pdb := &policyv1.PodDisruptionBudget{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "pdb-cleanup", Namespace: ns}, pdb) == nil
	}, "PDB should be created")

	// Disable PDB
	updated := &ledgerv1alpha1.LedgerService{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "pdb-cleanup", Namespace: ns}, updated))
	updated.Spec.PodDisruptionBudget.Enabled = false
	require.NoError(t, k8sClient.Update(ctx, updated))

	// Wait for PDB deletion
	requireEventually(t, func() bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: "pdb-cleanup", Namespace: ns}, pdb)
		return apierrors.IsNotFound(err)
	}, "PDB should be deleted after disabling")
}
