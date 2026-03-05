package controller

import (
	"context"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func (r *LedgerServiceReconciler) reconcileDNSEndpoint(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) error {
	gvk := schema.GroupVersionKind{
		Group:   "externaldns.k8s.io",
		Version: "v1alpha1",
		Kind:    "DNSEndpoint",
	}

	enabled := ledger.Spec.DNSEndpoint != nil && ledger.Spec.DNSEndpoint.Enabled

	if !enabled {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(gvk)
		obj.SetName(ledger.Name)
		obj.SetNamespace(ledger.Namespace)
		return r.deleteUnstructuredIfExists(ctx, obj)
	}

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetName(ledger.Name)
	obj.SetNamespace(ledger.Namespace)

	// Fetch existing to merge
	_ = r.Get(ctx, types.NamespacedName{Name: ledger.Name, Namespace: ledger.Namespace}, obj) //nolint:errcheck // ignore not-found

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		obj.SetLabels(commonLabels(ledger))
		obj.SetAnnotations(ledger.Spec.DNSEndpoint.Annotations)

		endpoints := make([]interface{}, 0, len(ledger.Spec.DNSEndpoint.Endpoints))
		for _, ep := range ledger.Spec.DNSEndpoint.Endpoints {
			entry := map[string]interface{}{
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
				ps := make([]interface{}, 0, len(ep.ProviderSpecific))
				for _, p := range ep.ProviderSpecific {
					ps = append(ps, map[string]interface{}{
						"name":  p.Name,
						"value": p.Value,
					})
				}
				entry["providerSpecific"] = ps
			}

			endpoints = append(endpoints, entry)
		}

		if err := unstructured.SetNestedSlice(obj.Object, endpoints, "spec", "endpoints"); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(ledger, obj, r.Scheme)
	})
	return err
}

func toInterfaceSlice(ss []string) []interface{} {
	result := make([]interface{}, len(ss))
	for i, s := range ss {
		result[i] = s
	}
	return result
}
