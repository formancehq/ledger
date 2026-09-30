//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
)

func TestNextControllerWithPebbleAndRocksDBGroupsInstalled(t *testing.T) {
	ns := createTestNamespace(t)
	legacy := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "ledger.formance.com/v1alpha1", "kind": "Cluster",
		"metadata": map[string]any{"name": "shared", "namespace": ns},
		"spec":     map[string]any{"replicas": int64(3), "pebble": map[string]any{"cacheSize": "1Gi"}},
	}}
	require.NoError(t, k8sClient.Create(ctx, legacy))
	legacyVersion := legacy.GetResourceVersion()
	legacyService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "ledger-shared", Namespace: ns},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app.kubernetes.io/name": "ledger", "app.kubernetes.io/instance": "shared"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 9000}},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, legacyService))
	legacyServiceVersion := legacyService.ResourceVersion
	next := newCluster("shared", ns)
	require.NoError(t, k8sClient.Create(ctx, next))
	var nextStatefulSet appsv1.StatefulSet
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-next-shared", Namespace: ns}, &nextStatefulSet) == nil
	}, "next controller must reconcile its own same-named Cluster")
	require.Equal(t, "ledger-next.formance.com/v1alpha1", nextStatefulSet.OwnerReferences[0].APIVersion)
	require.Equal(t, "ledger-next", nextStatefulSet.Spec.Template.Labels["app.kubernetes.io/name"])
	var nextService corev1.Service
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-next-shared", Namespace: ns}, &nextService))
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "shared", Namespace: ns}, legacy))
	require.Equal(t, legacyVersion, legacy.GetResourceVersion(), "next reconciliation must not update the Pebble CR")
	cacheSize, found, err := unstructured.NestedString(legacy.Object, "spec", "pebble", "cacheSize")
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "1Gi", cacheSize)
	var storedLegacyService corev1.Service
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: legacyService.Name, Namespace: ns}, &storedLegacyService))
	require.Equal(t, legacyServiceVersion, storedLegacyService.ResourceVersion)
	require.Equal(t, legacyService.Spec, storedLegacyService.Spec)
}
