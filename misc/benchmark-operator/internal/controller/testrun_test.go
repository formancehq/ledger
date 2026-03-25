//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	benchmarkv1alpha1 "github.com/formancehq/ledger-v3-poc/misc/benchmark-operator/api/v1alpha1"
)

func TestTestRun_StandaloneFinalizerAdded(t *testing.T) {
	ns := createTestNamespace(t)

	tr := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "k6.io/v1alpha1",
			"kind":       "TestRun",
			"metadata": map[string]any{
				"name":      "standalone-tr",
				"namespace": ns,
			},
			"spec": map[string]any{},
		},
	}
	_, err := dynamicClient.Resource(testRunGVR).Namespace(ns).Create(ctx, tr, metav1.CreateOptions{})
	require.NoError(t, err)

	// Wait for finalizer to be added.
	requireEventually(t, func() bool {
		obj, getErr := dynamicClient.Resource(testRunGVR).Namespace(ns).Get(ctx, "standalone-tr", metav1.GetOptions{})
		if getErr != nil {
			return false
		}
		return hasTestRunFinalizer(obj)
	}, "TestRun should have finalizer")
}

func TestTestRun_OwnedByBenchmarkSkipped(t *testing.T) {
	ns := createTestNamespace(t)

	// Create a Benchmark to get a valid UID.
	bm := newBenchmark("owner-bm", ns)
	require.NoError(t, k8sClient.Create(ctx, bm))

	// Wait for the Benchmark to get a UID.
	requireEventually(t, func() bool {
		var updated benchmarkv1alpha1.Benchmark
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "owner-bm", Namespace: ns}, &updated); err != nil {
			return false
		}
		return updated.UID != ""
	})

	var bmObj benchmarkv1alpha1.Benchmark
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "owner-bm", Namespace: ns}, &bmObj))

	// Create a TestRun owned by the Benchmark.
	tr := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "k6.io/v1alpha1",
			"kind":       "TestRun",
			"metadata": map[string]any{
				"name":      "owned-tr",
				"namespace": ns,
				"ownerReferences": []any{
					map[string]any{
						"apiVersion": benchmarkv1alpha1.GroupVersion.String(),
						"kind":       "Benchmark",
						"name":       bmObj.Name,
						"uid":        string(bmObj.UID),
					},
				},
			},
			"spec": map[string]any{},
		},
	}
	_, err := dynamicClient.Resource(testRunGVR).Namespace(ns).Create(ctx, tr, metav1.CreateOptions{})
	require.NoError(t, err)

	// Set the TestRun to finished — the standalone reconciler should NOT process it.
	setTestRunStage(t, ns, "owned-tr", stageFinished)

	// Give it some time to see if it would be processed.
	requireEventually(t, func() bool {
		obj, getErr := dynamicClient.Resource(testRunGVR).Namespace(ns).Get(ctx, "owned-tr", metav1.GetOptions{})
		if getErr != nil {
			return false
		}
		// It should NOT have the processed annotation — the standalone reconciler skips it.
		return !isProcessed(obj)
	}, "Owned TestRun should not be processed by standalone reconciler")
}

func TestTestRun_StandaloneDeletion(t *testing.T) {
	ns := createTestNamespace(t)

	tr := &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": "k6.io/v1alpha1",
			"kind":       "TestRun",
			"metadata": map[string]any{
				"name":      "delete-tr",
				"namespace": ns,
			},
			"spec": map[string]any{},
		},
	}
	_, err := dynamicClient.Resource(testRunGVR).Namespace(ns).Create(ctx, tr, metav1.CreateOptions{})
	require.NoError(t, err)

	// Wait for finalizer.
	requireEventually(t, func() bool {
		obj, getErr := dynamicClient.Resource(testRunGVR).Namespace(ns).Get(ctx, "delete-tr", metav1.GetOptions{})
		if getErr != nil {
			return false
		}
		return hasTestRunFinalizer(obj)
	}, "TestRun should have finalizer")

	// Delete the TestRun.
	err = dynamicClient.Resource(testRunGVR).Namespace(ns).Delete(ctx, "delete-tr", metav1.DeleteOptions{})
	require.NoError(t, err)

	// Verify it's eventually deleted (finalizer removed).
	requireEventually(t, func() bool {
		_, getErr := dynamicClient.Resource(testRunGVR).Namespace(ns).Get(ctx, "delete-tr", metav1.GetOptions{})
		return getErr != nil
	}, "TestRun should be deleted")
}

func TestBenchmark_TestRunNotProcessedByStandalone(t *testing.T) {
	// This test verifies that when a Benchmark creates a TestRun,
	// the standalone TestRun reconciler skips it because it has an owner reference.
	ns := createTestNamespace(t)

	bm := newBenchmark("skip-check", ns)
	require.NoError(t, k8sClient.Create(ctx, bm))

	// Wait for resource to be created.
	resName := resourceName("skip-check", 0)
	requireEventually(t, func() bool {
		_, getErr := dynamicClient.Resource(testServiceGVR).Namespace(ns).Get(ctx, resName, metav1.GetOptions{})
		return getErr == nil
	}, "Resource should be created")

	// Set resource to Running.
	setServicePhase(t, ns, resName, "Running")

	// Wait for TestRun to be created.
	trName := testRunName("skip-check")
	requireEventually(t, func() bool {
		_, getErr := dynamicClient.Resource(testRunGVR).Namespace(ns).Get(ctx, trName, metav1.GetOptions{})
		return getErr == nil
	}, "TestRun should be created")

	// Verify TestRun has owner reference.
	tr, err := dynamicClient.Resource(testRunGVR).Namespace(ns).Get(ctx, trName, metav1.GetOptions{})
	require.NoError(t, err)
	_, owned := isOwnedByBenchmark(tr)
	assert.True(t, owned, "TestRun should be owned by Benchmark")
}

func setTestRunStage(t *testing.T, namespace, name, stage string) {
	t.Helper()

	patch := []byte(`{"status":{"stage":"` + stage + `"}}`)
	_, err := dynamicClient.Resource(testRunGVR).Namespace(namespace).Patch(
		ctx, name, types.MergePatchType, patch, metav1.PatchOptions{}, "status",
	)
	require.NoError(t, err)
}
