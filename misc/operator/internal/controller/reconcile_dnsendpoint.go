package controller

import (
	"context"
	"maps"

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

	auto := ledger.Spec.AutoNetworking
	autoEnabled := auto != nil && auto.DNSEndpoint != nil && auto.DNSEndpoint.Enabled
	manualEnabled := ledger.Spec.DNSEndpoint != nil && ledger.Spec.DNSEndpoint.Enabled

	// Collect all DNS endpoints first, then decide whether to create or delete.
	var endpoints []any
	annotations := make(map[string]string)

	if autoEnabled {
		cfg := auto.DNSEndpoint
		maps.Copy(annotations, cfg.Annotations)

		// HTTP ingress DNS entry — only when the HTTP ingress is also enabled.
		if auto.Ingress != nil && auto.Ingress.Enabled {
			endpoints = append(endpoints, buildEndpointEntry(ledgerv1alpha1.DNSEndpointEntry{
				DNSName:          autoHost(ledger.Name, auto, cfg.Suffix),
				RecordType:       cfg.RecordType,
				Targets:          cfg.Targets,
				RecordTTL:        cfg.RecordTTL,
				ProviderSpecific: cfg.ProviderSpecific,
			}))
		}

		// gRPC ingress DNS entry — only when the gRPC ingress is also enabled.
		if auto.IngressGrpc != nil && auto.IngressGrpc.Enabled {
			endpoints = append(endpoints, buildEndpointEntry(ledgerv1alpha1.DNSEndpointEntry{
				DNSName:          autoHost(ledger.Name, auto, auto.IngressGrpc.Suffix),
				RecordType:       cfg.RecordType,
				Targets:          cfg.Targets,
				RecordTTL:        cfg.RecordTTL,
				ProviderSpecific: cfg.ProviderSpecific,
			}))
		}
	}

	if manualEnabled {
		maps.Copy(annotations, ledger.Spec.DNSEndpoint.Annotations)
		for _, ep := range ledger.Spec.DNSEndpoint.Endpoints {
			endpoints = append(endpoints, buildEndpointEntry(ep))
		}
	}

	// No endpoints to publish → delete the DNSEndpoint resource.
	if len(endpoints) == 0 {
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
	_ = r.Get(ctx, types.NamespacedName{Name: ledger.Name, Namespace: ledger.Namespace}, obj)

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
