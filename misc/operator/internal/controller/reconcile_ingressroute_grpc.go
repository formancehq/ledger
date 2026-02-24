package controller

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

// reconcileIngressRouteGrpc manages the Traefik IngressRoute for gRPC.
func (r *LedgerReconciler) reconcileIngressRouteGrpc(ctx context.Context, ledger *ledgerv1alpha1.Ledger) error {
	name := ledger.Name + "-grpc"

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "traefik.io",
		Version: "v1alpha1",
		Kind:    "IngressRoute",
	})
	obj.SetName(name)
	obj.SetNamespace(ledger.Namespace)

	// Only create for Traefik
	if ledger.Spec.IngressGrpc == nil || !ledger.Spec.IngressGrpc.Enabled || ledger.Spec.IngressGrpc.ClassName != "traefik" {
		return r.deleteUnstructuredIfExists(ctx, obj)
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, obj, func() error {
		obj.SetLabels(commonLabels(ledger))

		grpcPort := serviceGrpcPort(ledger)
		grpcSvcName := ledger.Name + "-grpc"

		// Build routes
		routes := make([]interface{}, 0, len(ledger.Spec.IngressGrpc.Hosts))
		for _, h := range ledger.Spec.IngressGrpc.Hosts {
			routes = append(routes, map[string]interface{}{
				"match": fmt.Sprintf("Host(`%s`)", h.Host),
				"kind":  "Rule",
				"services": []interface{}{
					map[string]interface{}{
						"name":   grpcSvcName,
						"port":   int64(grpcPort),
						"scheme": "h2c",
					},
				},
			})
		}

		spec := map[string]interface{}{
			"entryPoints": []interface{}{"websecure"},
			"routes":      routes,
		}

		// TLS
		if len(ledger.Spec.IngressGrpc.TLS) > 0 {
			spec["tls"] = map[string]interface{}{
				"secretName": ledger.Spec.IngressGrpc.TLS[0].SecretName,
			}
		}

		if err := unstructured.SetNestedField(obj.Object, spec, "spec"); err != nil {
			return err
		}
		return controllerutil.SetControllerReference(ledger, obj, r.Scheme)
	})
	return err
}

func (r *LedgerReconciler) deleteUnstructuredIfExists(ctx context.Context, obj *unstructured.Unstructured) error {
	err := r.Client.Get(ctx, types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}, obj)
	if err != nil {
		return ignoreNotFound(err)
	}
	return r.Client.Delete(ctx, obj)
}
