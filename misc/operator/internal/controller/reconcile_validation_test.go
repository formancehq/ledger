//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestReconcile_EvenReplicas(t *testing.T) {
	ns := createTestNamespace(t)
	replicas := int32(4)
	ls := newLedgerService("even", ns)
	ls.Spec.Replicas = &replicas
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Wait for ConfigValid=False
	updated := &ledgerv1alpha1.LedgerService{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "even", Namespace: ns}, updated); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(updated.Status.Conditions, "ConfigValid")
		return cond != nil && cond.Status == metav1.ConditionFalse
	}, "ConfigValid should be False for even replicas")

	assert.Equal(t, "Degraded", updated.Status.Phase)

	// StatefulSet should NOT be created
	sts := &appsv1.StatefulSet{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: "even", Namespace: ns}, sts)
	assert.Error(t, err, "StatefulSet should not be created with even replicas")
}

func TestReconcile_IngressEnabledNoHosts(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newLedgerService("ing-nohosts", ns)
	ls.Spec.Ingress = &ledgerv1alpha1.IngressSpec{
		Enabled: true,
		// No hosts → validation error
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	updated := &ledgerv1alpha1.LedgerService{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "ing-nohosts", Namespace: ns}, updated); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(updated.Status.Conditions, "ConfigValid")
		return cond != nil && cond.Status == metav1.ConditionFalse
	}, "ConfigValid should be False when ingress is enabled with no hosts")

	cond := meta.FindStatusCondition(updated.Status.Conditions, "ConfigValid")
	assert.Contains(t, cond.Message, "ingress")
}

func TestReconcile_IngressGrpcEnabledNoHosts(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newLedgerService("grpc-nohosts", ns)
	ls.Spec.IngressGrpc = &ledgerv1alpha1.IngressGrpcSpec{
		Enabled: true,
		// No hosts → validation error
	}
	require.NoError(t, k8sClient.Create(ctx, ls))

	updated := &ledgerv1alpha1.LedgerService{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "grpc-nohosts", Namespace: ns}, updated); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(updated.Status.Conditions, "ConfigValid")
		return cond != nil && cond.Status == metav1.ConditionFalse
	}, "ConfigValid should be False when ingressGrpc is enabled with no hosts")

	cond := meta.FindStatusCondition(updated.Status.Conditions, "ConfigValid")
	assert.Contains(t, cond.Message, "ingressGrpc")
}

func TestReconcile_ValidationFixed(t *testing.T) {
	ns := createTestNamespace(t)
	replicas := int32(4)
	ls := newLedgerService("fix-val", ns)
	ls.Spec.Replicas = &replicas
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Wait for ConfigValid=False
	updated := &ledgerv1alpha1.LedgerService{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "fix-val", Namespace: ns}, updated); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(updated.Status.Conditions, "ConfigValid")
		return cond != nil && cond.Status == metav1.ConditionFalse
	}, "ConfigValid should be False initially")

	// Fix the spec
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "fix-val", Namespace: ns}, updated))
	fixed := int32(3)
	updated.Spec.Replicas = &fixed
	require.NoError(t, k8sClient.Update(ctx, updated))

	// Wait for ConfigValid=True and StatefulSet
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "fix-val", Namespace: ns}, updated); err != nil {
			return false
		}
		cond := meta.FindStatusCondition(updated.Status.Conditions, "ConfigValid")
		return cond != nil && cond.Status == metav1.ConditionTrue
	}, "ConfigValid should become True after fix")

	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "fix-val", Namespace: ns}, sts) == nil
	}, "StatefulSet should appear after validation fix")
}
