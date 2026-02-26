package controller

import (
	"context"
	"encoding/json"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func (r *LedgerServiceReconciler) reconcileServiceMonitor(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) error {
	name := ledger.Name
	gvk := schema.GroupVersionKind{
		Group:   "monitoring.coreos.com",
		Version: "v1",
		Kind:    "ServiceMonitor",
	}

	if ledger.Spec.ServiceMonitor == nil || !ledger.Spec.ServiceMonitor.Enabled {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)
		obj.SetName(name)
		obj.SetNamespace(ledger.Namespace)
		return r.deleteUnstructuredIfExists(ctx, obj)
	}

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(name)
	obj.SetNamespace(ledger.Namespace)

	// Fetch existing to merge
	_ = r.Get(ctx, types.NamespacedName{Name: name, Namespace: ledger.Namespace}, obj) //nolint:errcheck // ignore not-found

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		labels := commonLabels(ledger)
		for k, v := range ledger.Spec.ServiceMonitor.Labels {
			labels[k] = v
		}
		obj.SetLabels(labels)

		endpoint := map[string]interface{}{
			"port": "http",
			"path": "/metrics",
		}
		if ledger.Spec.ServiceMonitor.Interval != "" {
			endpoint["interval"] = ledger.Spec.ServiceMonitor.Interval
		}
		if ledger.Spec.ServiceMonitor.ScrapeTimeout != "" {
			endpoint["scrapeTimeout"] = ledger.Spec.ServiceMonitor.ScrapeTimeout
		}
		if len(ledger.Spec.ServiceMonitor.Relabelings) > 0 {
			endpoint["relabelings"] = rawExtensionsToUnstructured(ledger.Spec.ServiceMonitor.Relabelings)
		}
		if len(ledger.Spec.ServiceMonitor.MetricRelabelings) > 0 {
			endpoint["metricRelabelings"] = rawExtensionsToUnstructured(ledger.Spec.ServiceMonitor.MetricRelabelings)
		}

		spec := map[string]interface{}{
			"selector": map[string]interface{}{
				"matchLabels": toStringInterfaceMap(selectorLabels(ledger)),
			},
			"endpoints": []interface{}{endpoint},
		}

		if err := unstructured.SetNestedField(obj.Object, spec, "spec"); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(ledger, obj, r.Scheme)
	})
	return err
}

func toStringInterfaceMap(m map[string]string) map[string]interface{} {
	result := make(map[string]interface{}, len(m))
	for k, v := range m {
		result[k] = v
	}
	return result
}

// rawExtensionsToUnstructured converts a slice of RawExtension to []interface{} for unstructured usage.
func rawExtensionsToUnstructured(items []runtime.RawExtension) []interface{} {
	result := make([]interface{}, 0, len(items))
	for _, item := range items {
		if item.Raw != nil {
			var parsed interface{}
			if err := json.Unmarshal(item.Raw, &parsed); err == nil {
				result = append(result, parsed)
			}
		}
	}
	return result
}
