//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func TestReconcile_DefaultsMerge(t *testing.T) {
	ns := createTestNamespace(t)

	// Create cluster-scoped LedgerDefaults
	defaults := newLedgerDefaults("defaults-merge")
	defaults.Spec.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("100m"),
			corev1.ResourceMemory: resource.MustParse("256Mi"),
		},
	}
	require.NoError(t, k8sClient.Create(ctx, defaults))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, defaults) //nolint:errcheck
	})

	ls := newLedgerService("merge", ns)
	ls.Spec.DefaultsRef = "defaults-merge"
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Wait for StatefulSet
	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "merge", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")

	// Verify image from defaults was applied
	container := sts.Spec.Template.Spec.Containers[0]
	assert.Equal(t, "ghcr.io/formancehq/ledger-v3-poc:v1.0.0", container.Image)

	// Verify resources from defaults
	assert.Equal(t, resource.MustParse("100m"), container.Resources.Requests[corev1.ResourceCPU])
	assert.Equal(t, resource.MustParse("256Mi"), container.Resources.Requests[corev1.ResourceMemory])

	// Verify DefaultsResolved condition
	updated := &ledgerv1alpha1.LedgerService{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "merge", Namespace: ns}, updated); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(updated.Status.Conditions, "DefaultsResolved")
		return cond != nil && cond.Status == metav1.ConditionTrue
	}, "DefaultsResolved should be True")
}

func TestReconcile_DefaultsOverride(t *testing.T) {
	ns := createTestNamespace(t)

	defaults := newLedgerDefaults("defaults-override")
	defaults.Spec.Image.Tag = "v1.0.0"
	defaults.Spec.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("100m"),
		},
	}
	require.NoError(t, k8sClient.Create(ctx, defaults))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, defaults) //nolint:errcheck
	})

	ls := newLedgerService("override", ns)
	ls.Spec.DefaultsRef = "defaults-override"
	ls.Spec.Image.Tag = "v2.0.0" // Override the defaults tag
	ls.Spec.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("500m"),
		},
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "override", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")

	// LedgerService values take precedence
	container := sts.Spec.Template.Spec.Containers[0]
	assert.Contains(t, container.Image, ":v2.0.0")
	assert.Equal(t, resource.MustParse("500m"), container.Resources.Requests[corev1.ResourceCPU])
}

func TestReconcile_DefaultsNotFound(t *testing.T) {
	ns := createTestNamespace(t)

	ls := newLedgerService("no-defaults", ns)
	ls.Spec.DefaultsRef = "nonexistent-defaults"
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Wait for DefaultsResolved=False
	updated := &ledgerv1alpha1.LedgerService{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "no-defaults", Namespace: ns}, updated); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(updated.Status.Conditions, "DefaultsResolved")
		return cond != nil && cond.Status == metav1.ConditionFalse
	}, "DefaultsResolved should be False")

	assert.Equal(t, "Degraded", updated.Status.Phase)

	// StatefulSet should NOT be created
	sts := &appsv1.StatefulSet{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: "no-defaults", Namespace: ns}, sts)
	assert.Error(t, err, "StatefulSet should not be created when defaults are missing")
}

func TestReconcile_DefaultsUpdate(t *testing.T) {
	ns := createTestNamespace(t)

	defaults := newLedgerDefaults("defaults-update")
	defaults.Spec.Image.Tag = "v1.0.0"
	require.NoError(t, k8sClient.Create(ctx, defaults))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, defaults) //nolint:errcheck
	})

	ls := newLedgerService("upd", ns)
	ls.Spec.DefaultsRef = "defaults-update"
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Wait for initial StatefulSet
	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "upd", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")

	initialHash := sts.Spec.Template.Annotations[annotationSpecHash]
	require.NotEmpty(t, initialHash)

	// Update defaults
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "defaults-update"}, defaults))
	defaults.Spec.Image.Tag = "v2.0.0"
	require.NoError(t, k8sClient.Update(ctx, defaults))

	// Wait for spec hash to change
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "upd", Namespace: ns}, sts); err != nil {
			return false
		}
		return sts.Spec.Template.Annotations[annotationSpecHash] != initialHash
	}, "spec hash should change after defaults update")

	assert.Contains(t, sts.Spec.Template.Spec.Containers[0].Image, ":v2.0.0")
}
