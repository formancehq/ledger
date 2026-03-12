//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	benchmarkv1alpha1 "github.com/formancehq/ledger-v3-poc/misc/benchmark-operator/api/v1alpha1"
)

func TestBenchmark_Lifecycle(t *testing.T) {
	ns := createTestNamespace(t)
	bm := newBenchmark("lifecycle", ns)
	require.NoError(t, k8sClient.Create(ctx, bm))

	// LedgerService should be created and phase should progress past Pending.
	lsName := ledgerServiceName("lifecycle")
	requireEventually(t, func() bool {
		_, err := dynamicClient.Resource(ledgerServiceGVR).Namespace(ns).Get(ctx, lsName, metav1.GetOptions{})
		return err == nil
	}, "LedgerService should be created")

	// Verify LedgerService has owner reference.
	ls, err := dynamicClient.Resource(ledgerServiceGVR).Namespace(ns).Get(ctx, lsName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, ls.GetOwnerReferences(), 1)
	assert.Equal(t, "Benchmark", ls.GetOwnerReferences()[0].Kind)
	assert.Equal(t, "lifecycle", ls.GetOwnerReferences()[0].Name)

	// Phase should reach at least WaitingForCluster (may skip CreatingCluster quickly).
	requireEventually(t, func() bool {
		var updated benchmarkv1alpha1.Benchmark
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "lifecycle", Namespace: ns}, &updated); err != nil {
			return false
		}
		return updated.Status.Phase == benchmarkv1alpha1.BenchmarkPhaseWaitingCluster ||
			updated.Status.Phase == benchmarkv1alpha1.BenchmarkPhaseCreatingCluster
	}, "Benchmark should be in CreatingCluster or WaitingForCluster phase")

	// Simulate LedgerService becoming ready.
	setLedgerServicePhase(t, ns, lsName, "Running")

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

	// Verify TestRun has GRPC_ADDR injected and owner reference.
	tr, err := dynamicClient.Resource(testRunGVR).Namespace(ns).Get(ctx, trName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Len(t, tr.GetOwnerReferences(), 1)
	assert.Equal(t, "Benchmark", tr.GetOwnerReferences()[0].Kind)

	runner, _, _ := unstructured.NestedMap(tr.Object, "spec", "runner")
	if runner != nil {
		envList, _, _ := unstructured.NestedSlice(tr.Object, "spec", "runner", "env")
		foundEndpoint := false
		for _, item := range envList {
			if m, ok := item.(map[string]any); ok {
				if m["name"] == "GRPC_ADDR" {
					foundEndpoint = true
					expected := ledgerServiceGRPCEndpoint(lsName, ns)
					assert.Equal(t, expected, m["value"])
				}
			}
		}
		assert.True(t, foundEndpoint, "GRPC_ADDR should be injected")
	}

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

	// Wait for LedgerServiceName to be set in status.
	requireEventually(t, func() bool {
		var updated benchmarkv1alpha1.Benchmark
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "status", Namespace: ns}, &updated); err != nil {
			return false
		}
		return updated.Status.LedgerServiceName == ledgerServiceName("status")
	}, "LedgerServiceName should be set")
}

// setLedgerServicePhase patches the LedgerService status to simulate readiness.
func setLedgerServicePhase(t *testing.T, namespace, name, phase string) {
	t.Helper()

	patch := []byte(`{"status":{"phase":"` + phase + `"}}`)
	_, err := dynamicClient.Resource(ledgerServiceGVR).Namespace(namespace).Patch(
		ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}, "status",
	)
	require.NoError(t, err)
}
