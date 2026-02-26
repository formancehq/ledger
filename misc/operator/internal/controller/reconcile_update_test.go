//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func TestReconcile_SpecHashChanges(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newLedgerService("hash-change", ns)
	require.NoError(t, k8sClient.Create(ctx, ls))

	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "hash-change", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")

	initialHash := sts.Spec.Template.Annotations[annotationSpecHash]
	require.NotEmpty(t, initialHash)

	// Update spec to trigger hash change
	updated := &ledgerv1alpha1.LedgerService{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "hash-change", Namespace: ns}, updated))
	updated.Spec.Config.Debug = true
	require.NoError(t, k8sClient.Update(ctx, updated))

	// Wait for spec hash to change
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "hash-change", Namespace: ns}, sts); err != nil {
			return false
		}
		return sts.Spec.Template.Annotations[annotationSpecHash] != initialHash
	}, "spec hash should change after spec update")

	assert.NotEqual(t, initialHash, sts.Spec.Template.Annotations[annotationSpecHash])
}
