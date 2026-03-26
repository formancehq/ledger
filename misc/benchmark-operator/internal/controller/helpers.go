package controller

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	benchmarkv1alpha1 "github.com/formancehq/ledger-v3-poc/misc/benchmark-operator/api/v1alpha1"
)

const (
	processedAnnotation = "benchmark.formance.com/processed"
	benchmarkFinalizer  = "benchmark.formance.com/finalizer"
	stageFinished       = "finished"
	stageError          = "error"
)

var testRunGVR = schema.GroupVersionResource{
	Group:    "k6.io",
	Version:  "v1alpha1",
	Resource: "testruns",
}

func resourceName(benchmarkName string, index int) string {
	return fmt.Sprintf("%s-res-%d", benchmarkName, index)
}

func testRunName(benchmarkName string) string {
	return benchmarkName + "-run"
}

func reportConfigMapName(testRunName string) string {
	return "k6-report-" + testRunName
}

// parseGVR parses an apiVersion (e.g. "ledger.formance.com/v1alpha1") and a plural resource name
// into a GroupVersionResource.
func parseGVR(apiVersion, resource string) schema.GroupVersionResource {
	parts := strings.SplitN(apiVersion, "/", 2)
	if len(parts) == 1 {
		return schema.GroupVersionResource{Group: "", Version: parts[0], Resource: resource}
	}

	return schema.GroupVersionResource{Group: parts[0], Version: parts[1], Resource: resource}
}

// kindToResource converts a Kind (e.g. "LedgerService") to a plural resource name (e.g. "ledgerservices").
func kindToResource(kind string) string {
	return strings.ToLower(kind) + "s"
}

// getString navigates a nested map by keys and returns the string value at the final key.
func getString(obj map[string]any, path ...string) string {
	value := obj
	for i, key := range path {
		if i == len(path)-1 {
			if raw, ok := value[key]; ok {
				if str, ok := raw.(string); ok {
					return str
				}
			}

			return ""
		}

		next, ok := value[key].(map[string]any)
		if !ok {
			return ""
		}
		value = next
	}

	return ""
}

// getNestedString navigates a dot-separated field path (e.g. "status.phase")
// and returns the string value.
func getNestedString(obj map[string]any, fieldPath string) string {
	return getString(obj, strings.Split(fieldPath, ".")...)
}

func isProcessed(obj *unstructured.Unstructured) bool {
	annotations := obj.GetAnnotations()
	if annotations == nil {
		return false
	}

	return strings.TrimSpace(annotations[processedAnnotation]) != ""
}

func ownerReferenceForBenchmark(bm *benchmarkv1alpha1.Benchmark) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: benchmarkv1alpha1.GroupVersion.String(),
		Kind:       "Benchmark",
		Name:       bm.Name,
		UID:        bm.UID,
	}
}

func ownerRefToMap(ref metav1.OwnerReference) map[string]any {
	return map[string]any{
		"apiVersion": ref.APIVersion,
		"kind":       ref.Kind,
		"name":       ref.Name,
		"uid":        string(ref.UID),
	}
}

// buildResource constructs an unstructured resource from a ResourceEntry manifest,
// overriding the name and namespace and setting the owner reference.
func buildResource(bm *benchmarkv1alpha1.Benchmark, entry benchmarkv1alpha1.ResourceEntry, name string) *unstructured.Unstructured {
	var manifest map[string]any
	if len(entry.Manifest.Raw) > 0 {
		_ = json.Unmarshal(entry.Manifest.Raw, &manifest)
	}
	if manifest == nil {
		manifest = map[string]any{}
	}

	// Ensure metadata exists.
	metadata, ok := manifest["metadata"].(map[string]any)
	if !ok {
		metadata = map[string]any{}
	}
	metadata["name"] = name
	metadata["namespace"] = bm.Namespace
	metadata["ownerReferences"] = []any{
		ownerRefToMap(ownerReferenceForBenchmark(bm)),
	}
	manifest["metadata"] = metadata

	return &unstructured.Unstructured{Object: manifest}
}

// gvrForManifest extracts the GVR from a raw manifest's apiVersion and kind.
func gvrForManifest(raw []byte) (schema.GroupVersionResource, error) {
	var partial struct {
		APIVersion string `json:"apiVersion"`
		Kind       string `json:"kind"`
	}
	if err := json.Unmarshal(raw, &partial); err != nil {
		return schema.GroupVersionResource{}, fmt.Errorf("unmarshal manifest: %w", err)
	}
	if partial.APIVersion == "" || partial.Kind == "" {
		return schema.GroupVersionResource{}, errors.New("manifest missing apiVersion or kind")
	}

	return parseGVR(partial.APIVersion, kindToResource(partial.Kind)), nil
}

// checkReadyCondition checks if the resource's field at fieldPath matches the expected value.
func checkReadyCondition(obj *unstructured.Unstructured, cond benchmarkv1alpha1.ReadyCondition) bool {
	return getNestedString(obj.Object, cond.FieldPath) == cond.Value
}

func buildTestRun(bm *benchmarkv1alpha1.Benchmark) *unstructured.Unstructured {
	name := testRunName(bm.Name)

	var spec map[string]any
	if len(bm.Spec.TestRun.Raw) > 0 {
		_ = json.Unmarshal(bm.Spec.TestRun.Raw, &spec)
	}
	if spec == nil {
		spec = map[string]any{}
	}

	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": testRunGVR.Group + "/" + testRunGVR.Version,
			"kind":       "TestRun",
			"metadata": map[string]any{
				"name":      name,
				"namespace": bm.Namespace,
				"ownerReferences": []any{
					ownerRefToMap(ownerReferenceForBenchmark(bm)),
				},
			},
			"spec": spec,
		},
	}
}

func isOwnedByBenchmark(obj *unstructured.Unstructured) (types.NamespacedName, bool) {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Kind == "Benchmark" && strings.HasPrefix(ref.APIVersion, "benchmark.formance.com/") {
			return types.NamespacedName{
				Namespace: obj.GetNamespace(),
				Name:      ref.Name,
			}, true
		}
	}

	return types.NamespacedName{}, false
}
