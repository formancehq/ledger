package controller

import (
	"context"
	"maps"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

var dnsEndpointGVK = schema.GroupVersionKind{
	Group:   "externaldns.k8s.io",
	Version: "v1alpha1",
	Kind:    "DNSEndpoint",
}

// reconcileDNSEndpoint reconciles the set of ExternalDNS DNSEndpoint objects for
// a Cluster. Each spec.dnsEndpoints entry becomes its own DNSEndpoint object so
// that, for example, a public and a private endpoint can carry different
// annotations. Entries that are disabled or carry no endpoints are not created,
// and any previously-created DNSEndpoint object no longer desired is deleted.
func (r *ClusterReconciler) reconcileDNSEndpoint(ctx context.Context, ledger *ledgerv1alpha1.Cluster) error {
	desired := make(map[string]struct{}, len(ledger.Spec.DNSEndpoints))

	for i := range ledger.Spec.DNSEndpoints {
		spec := &ledger.Spec.DNSEndpoints[i]
		if !spec.Enabled {
			continue
		}

		endpoints := make([]any, 0, len(spec.Endpoints))
		for _, ep := range spec.Endpoints {
			endpoints = append(endpoints, buildEndpointEntry(ep))
		}
		if len(endpoints) == 0 {
			continue
		}

		name := dnsEndpointName(ledger.Name, spec.Name)
		desired[name] = struct{}{}

		if err := r.applyDNSEndpoint(ctx, ledger, name, spec.Annotations, endpoints); err != nil {
			return err
		}
	}

	return r.pruneDNSEndpoints(ctx, ledger, desired)
}

// applyDNSEndpoint creates or updates a single DNSEndpoint object.
func (r *ClusterReconciler) applyDNSEndpoint(
	ctx context.Context,
	ledger *ledgerv1alpha1.Cluster,
	name string,
	specAnnotations map[string]string,
	endpoints []any,
) error {
	annotations := make(map[string]string)
	maps.Copy(annotations, specAnnotations)

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(dnsEndpointGVK)
	obj.SetName(name)
	obj.SetNamespace(ledger.Namespace)

	// CreateOrUpdate fetches the current object internally before invoking the
	// mutate function, so no explicit pre-Get is needed. Labels and annotations
	// are authoritatively (re)set: the operator owns the full metadata surface
	// of the DNSEndpoint objects it creates.
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

// pruneDNSEndpoints deletes DNSEndpoint objects owned by the Cluster that are no
// longer in the desired set (an entry was removed, renamed, disabled, or left
// with no endpoints).
//
// Discovery lists by the non-overridable managed-by label only: the instance
// label is user-overridable via spec.additionalLabels (see selectorLabels), so
// it cannot be trusted to find every object this reconciler created. Ownership
// is then established authoritatively via the controller owner reference stamped
// by SetControllerReference — a label match alone is never sufficient to delete,
// so a manually-labeled DNSEndpoint the operator does not control is left alone.
func (r *ClusterReconciler) pruneDNSEndpoints(ctx context.Context, ledger *ledgerv1alpha1.Cluster, desired map[string]struct{}) error {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(dnsEndpointGVK)

	if err := r.List(ctx, list,
		client.InNamespace(ledger.Namespace),
		client.MatchingLabels{labelManagedBy: "ledger-operator"},
	); err != nil {
		// The DNSEndpoint CRD may not be installed (NoKindMatch); nothing to prune.
		return ignoreNotFound(err)
	}

	for i := range list.Items {
		obj := &list.Items[i]
		if _, ok := desired[obj.GetName()]; ok {
			continue
		}
		if !metav1.IsControlledBy(obj, ledger) {
			continue
		}
		if err := r.deleteUnstructuredIfExists(ctx, obj); err != nil {
			return err
		}
	}

	return nil
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
