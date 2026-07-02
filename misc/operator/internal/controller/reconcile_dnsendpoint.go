package controller

import (
	"context"
	"maps"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func (r *ClusterReconciler) reconcileDNSEndpoint(ctx context.Context, ledger *ledgerv1alpha1.Cluster) error {
	gvk := schema.GroupVersionKind{
		Group:   "externaldns.k8s.io",
		Version: "v1alpha1",
		Kind:    "DNSEndpoint",
	}

	if ledger.Spec.DNSEndpoint == nil || !ledger.Spec.DNSEndpoint.Enabled {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)
		obj.SetName(resourceName(ledger.Name))
		obj.SetNamespace(ledger.Namespace)

		return r.deleteUnstructuredIfExists(ctx, obj)
	}

	var endpoints []any
	annotations := make(map[string]string)
	maps.Copy(annotations, ledger.Spec.DNSEndpoint.Annotations)

	for _, ep := range ledger.Spec.DNSEndpoint.Endpoints {
		endpoints = append(endpoints, buildEndpointEntry(ep))
	}

	if len(endpoints) == 0 {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)
		obj.SetName(resourceName(ledger.Name))
		obj.SetNamespace(ledger.Namespace)

		return r.deleteUnstructuredIfExists(ctx, obj)
	}

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(resourceName(ledger.Name))
	obj.SetNamespace(ledger.Namespace)

	// Fetch existing to merge.
	_ = r.Get(ctx, types.NamespacedName{Name: resourceName(ledger.Name), Namespace: ledger.Namespace}, obj)

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		obj.SetLabels(commonLabels(ledger))
		obj.SetAnnotations(annotations)

		if err := unstructured.SetNestedSlice(obj.Object, endpoints, "spec", "endpoints"); err != nil {
			return err
		}

		return controllerutil.SetControllerReference(ledger, obj, r.Scheme)
	})

	return err
}

func buildEndpointEntry(ep ledgerv1alpha1.DNSEndpointEntry) map[string]any {
	entry := map[string]any{
		"dnsName": ep.DNSName,
		"targets": toInterfaceSlice(ep.Targets),
	}

	recordType := ep.RecordType
	if recordType == "" {
		recordType = "CNAME"
	}
	entry["recordType"] = recordType

	if ep.RecordTTL != nil {
		entry["recordTTL"] = *ep.RecordTTL
	}

	if len(ep.ProviderSpecific) > 0 {
		ps := make([]any, 0, len(ep.ProviderSpecific))
		for _, p := range ep.ProviderSpecific {
			ps = append(ps, map[string]any{
				"name":  p.Name,
				"value": p.Value,
			})
		}
		entry["providerSpecific"] = ps
	}

	return entry
}

func toInterfaceSlice(ss []string) []any {
	result := make([]any, len(ss))
	for i, s := range ss {
		result[i] = s
	}

	return result
}
