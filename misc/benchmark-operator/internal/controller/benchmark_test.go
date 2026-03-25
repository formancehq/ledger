//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	benchmarkv1alpha1 "github.com/formancehq/ledger-v3-poc/misc/benchmark-operator/api/v1alpha1"
)

func TestBenchmark_Lifecycle(t *testing.T) {
	ns := createTestNamespace(t)
	bm := newBenchmark("lifecycle", ns)
	require.NoError(t, k8sClient.Create(ctx, bm))

	// Resource should be created and phase should progress past Pending.
	resName := resourceName("lifecycle", 0)
	requireEventually(t, func() bool {
		_, err := dynamicClient.Resource(testServiceGVR).Namespace(ns).Get(ctx, resName, metav1.GetOptions{})
		return err == nil
	}, "Resource should be created")

	// Verify resource has owner reference.
	res, err := dynamicClient.Resource(testServiceGVR).Namespace(ns).Get(ctx, resName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, res.GetOwnerReferences(), 1)
	assert.Equal(t, "Benchmark", res.GetOwnerReferences()[0].Kind)
	assert.Equal(t, "lifecycle", res.GetOwnerReferences()[0].Name)

	// Phase should reach at least CreatingResources.
	requireEventually(t, func() bool {
		var updated benchmarkv1alpha1.Benchmark
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lifecycle", Namespace: ns}, &updated); err != nil {
			return false
		}
		return updated.Status.Phase == benchmarkv1alpha1.BenchmarkPhaseCreatingResources ||
			updated.Status.Phase == benchmarkv1alpha1.BenchmarkPhaseWaitingForResources
	}, "Benchmark should be in CreatingResources or WaitingForResources phase")

	// Simulate resource becoming ready.
	setServicePhase(t, ns, resName, "Running")

	// Phase should move to Running and TestRun should be created.
	requireEventually(t, func() bool {
		var updated benchmarkv1alpha1.Benchmark
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lifecycle", Namespace: ns}, &updated); err != nil {
			return false
		}
		return updated.Status.Phase == benchmarkv1alpha1.BenchmarkPhaseRunning
	}, "Benchmark should be in Running phase")

	// Verify TestRun was created.
	trName := testRunName("lifecycle")
	requireEventually(t, func() bool {
		_, err := dynamicClient.Resource(testRunGVR).Namespace(ns).Get(ctx, trName, metav1.GetOptions{})
		return err == nil
	}, "TestRun should be created")

	// Verify TestRun has owner reference.
	tr, err := dynamicClient.Resource(testRunGVR).Namespace(ns).Get(ctx, trName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, tr.GetOwnerReferences(), 1)
	assert.Equal(t, "Benchmark", tr.GetOwnerReferences()[0].Kind)

	// Verify StartTime was set.
	var updated benchmarkv1alpha1.Benchmark
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "lifecycle", Namespace: ns}, &updated))
	assert.NotNil(t, updated.Status.StartTime)
	assert.Equal(t, trName, updated.Status.TestRunName)
}

func TestBenchmark_FinalizerAndDeletion(t *testing.T) {
	ns := createTestNamespace(t)
	bm := newBenchmark("deletion", ns)
	require.NoError(t, k8sClient.Create(ctx, bm))

	// Wait for finalizer to be added.
	requireEventually(t, func() bool {
		var updated benchmarkv1alpha1.Benchmark
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "deletion", Namespace: ns}, &updated); err != nil {
			return false
		}
		for _, f := range updated.Finalizers {
			if f == benchmarkFinalizer {
				return true
			}
		}
		return false
	}, "Benchmark should have finalizer")

	// Delete the Benchmark.
	require.NoError(t, k8sClient.Delete(ctx, bm))

	// Verify the Benchmark is eventually deleted (finalizer removed).
	requireEventually(t, func() bool {
		var updated benchmarkv1alpha1.Benchmark
		err := k8sClient.Get(ctx, types.NamespacedName{Name: "deletion", Namespace: ns}, &updated)
		return client.IgnoreNotFound(err) == nil && err != nil
	}, "Benchmark should be deleted")
}

func TestBenchmark_StatusFields(t *testing.T) {
	ns := createTestNamespace(t)
	bm := newBenchmark("status", ns)
	require.NoError(t, k8sClient.Create(ctx, bm))

	// Wait for ResourceNames to be set in status.
	requireEventually(t, func() bool {
		var updated benchmarkv1alpha1.Benchmark
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "status", Namespace: ns}, &updated); err != nil {
			return false
		}
		return len(updated.Status.ResourceNames) > 0 && updated.Status.ResourceNames[0] == resourceName("status", 0)
	}, "ResourceNames should be set")
}

// setServicePhase patches the test Service status to simulate readiness.
func setServicePhase(t *testing.T, namespace, name, phase string) {
	t.Helper()

	patch := []byte(`{"status":{"phase":"` + phase + `"}}`)
	_, err := dynamicClient.Resource(testServiceGVR).Namespace(namespace).Patch(
		ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}, "status",
	)
	require.NoError(t, err)
}
