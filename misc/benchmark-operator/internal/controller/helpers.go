package controller

import (
	"encoding/json"
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

var (
	testRunGVR = schema.GroupVersionResource{
		Group:    "k6.io",
		Version:  "v1alpha1",
		Resource: "testruns",
	}

	ledgerServiceGVR = schema.GroupVersionResource{
		Group:    "ledger.formance.com",
		Version:  "v1alpha1",
		Resource: "ledgerservices",
	}
)

func ledgerServiceName(benchmarkName string) string {
	return benchmarkName + "-ledger"
}

func testRunName(benchmarkName string) string {
	return benchmarkName + "-run"
}

func ledgerServiceGRPCEndpoint(name, namespace string) string {
	return fmt.Sprintf("%s.%s.svc.cluster.local:8888", name, namespace)
}

func reportConfigMapName(testRunName string) string {
	return "k6-report-" + testRunName
}

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

func buildLedgerService(bm *benchmarkv1alpha1.Benchmark) *unstructured.Unstructured {
	name := ledgerServiceName(bm.Name)

	var spec map[string]any
	if len(bm.Spec.LedgerService.Raw) > 0 {
		_ = json.Unmarshal(bm.Spec.LedgerService.Raw, &spec)
	}
	if spec == nil {
		spec = map[string]any{}
	}

	return &unstructured.Unstructured{
		Object: map[string]any{
			"apiVersion": ledgerServiceGVR.Group + "/" + ledgerServiceGVR.Version,
			"kind":       "LedgerService",
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

func buildTestRun(bm *benchmarkv1alpha1.Benchmark, grpcEndpoint string) *unstructured.Unstructured {
	name := testRunName(bm.Name)

	var spec map[string]any
	if len(bm.Spec.TestRun.Raw) > 0 {
		_ = json.Unmarshal(bm.Spec.TestRun.Raw, &spec)
	}
	if spec == nil {
		spec = map[string]any{}
	}

	if grpcEndpoint != "" {
		injectRunnerEnv(spec, "GRPC_ADDR", grpcEndpoint)
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

func injectRunnerEnv(spec map[string]any, name, value string) {
	runner, ok := spec["runner"].(map[string]any)
	if !ok {
		runner = map[string]any{}
		spec["runner"] = runner
	}

	envList, _ := runner["env"].([]any)

	for _, item := range envList {
		if m, ok := item.(map[string]any); ok {
			if m["name"] == name {
				m["value"] = value

				return
			}
		}
	}

	envList = append(envList, map[string]any{
		"name":  name,
		"value": value,
	})
	runner["env"] = envList
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
