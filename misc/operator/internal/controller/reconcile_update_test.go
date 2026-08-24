//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestReconcile_SpecHashChanges(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("hash-change", ns)
	require.NoError(t, k8sClient.Create(ctx, ls))

	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-hash-change", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")

	initialHash := sts.Spec.Template.Annotations[annotationSpecHash]
	require.NotEmpty(t, initialHash)

	// Update spec to trigger hash change
	updated := &ledgerv1alpha1.Cluster{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "hash-change", Namespace: ns}, updated))
	updated.Spec.Debug = true
	require.NoError(t, k8sClient.Update(ctx, updated))

	// Wait for spec hash to change
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-hash-change", Namespace: ns}, sts); err != nil {
			return false
		}
		return sts.Spec.Template.Annotations[annotationSpecHash] != initialHash
	}, "spec hash should change after spec update")

	assert.NotEqual(t, initialHash, sts.Spec.Template.Annotations[annotationSpecHash])
}

// TestReconcile_PreservesKubectlRestartedAt covers EN-1850: the reconciler
// rebuilds the pod template from the Cluster CR, and must carry over the
// annotation `kubectl rollout restart` stamps on the StatefulSet, otherwise
// the reconcile reverts kubectl's patch and cancels the rolling restart after
// the first (highest-ordinal) pod.
func TestReconcile_PreservesKubectlRestartedAt(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("kubectl-restart", ns)
	require.NoError(t, k8sClient.Create(ctx, ls))

	stsName := types.NamespacedName{Name: "ledger-kubectl-restart", Namespace: ns}
	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, stsName, sts) == nil
	}, "StatefulSet should be created")

	// Simulate `kubectl rollout restart statefulset`: stamp restartedAt on the
	// pod template. This patch itself triggers a reconcile of the owned
	// StatefulSet.
	const restartedAt = "2026-08-24T10:00:00Z"
	require.NoError(t, k8sClient.Get(ctx, stsName, sts))
	if sts.Spec.Template.Annotations == nil {
		sts.Spec.Template.Annotations = map[string]string{}
	}
	sts.Spec.Template.Annotations[annotationKubectlRestartedAt] = restartedAt
	require.NoError(t, k8sClient.Update(ctx, sts))

	// Force a reconcile that rewrites the template from the CR and wait for it
	// to land (new spec hash). The kubectl stamp must survive that rewrite.
	initialHash := sts.Spec.Template.Annotations[annotationSpecHash]
	updated := &ledgerv1alpha1.Cluster{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "kubectl-restart", Namespace: ns}, updated))
	updated.Spec.Debug = true
	require.NoError(t, k8sClient.Update(ctx, updated))

	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, stsName, sts); err != nil {
			return false
		}
		return sts.Spec.Template.Annotations[annotationSpecHash] != initialHash
	}, "spec hash should change after spec update")

	assert.Equal(t, restartedAt, sts.Spec.Template.Annotations[annotationKubectlRestartedAt],
		"kubectl rollout restart stamp must survive template rewrites (EN-1850)")
}
