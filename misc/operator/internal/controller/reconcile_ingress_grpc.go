package controller

import (
	"context"
	"maps"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

// reconcileIngressGrpc manages the gRPC Ingress resource.
func (r *LedgerServiceReconciler) reconcileIngressGrpc(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) error {
	name := ledger.Name + "-grpc"

	// Delete the Ingress if gRPC ingress is disabled or has no hosts
	// (e.g. only a TargetGroupBinding is configured).
	if ledger.Spec.IngressGrpc == nil || !ledger.Spec.IngressGrpc.Enabled || len(ledger.Spec.IngressGrpc.Hosts) == 0 {
		return r.deleteIfExists(ctx, &networkingv1.Ingress{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ledger.Namespace,
			},
		})
	}

	ing := &networkingv1.Ingress{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ledger.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, ing, func() error {
		ing.Labels = commonLabels(ledger)

		annotations := make(map[string]string)
		switch ledger.Spec.IngressGrpc.ClassName {
		case "nginx":
			annotations["nginx.ingress.kubernetes.io/backend-protocol"] = "GRPC"
		case "traefik":
			annotations["traefik.ingress.kubernetes.io/service.serversscheme"] = "h2c"
		}
		maps.Copy(annotations, ledger.Spec.IngressGrpc.Annotations)
		ing.Annotations = annotations

		spec := networkingv1.IngressSpec{}
		if ledger.Spec.IngressGrpc.ClassName != "" {
			spec.IngressClassName = &ledger.Spec.IngressGrpc.ClassName
		}

		spec.TLS = buildIngressTLS(ledger.Spec.IngressGrpc.TLS)
		spec.Rules = buildGrpcIngressRules(ledger, ledger.Spec.IngressGrpc.Hosts)

		ing.Spec = spec

		return controllerutil.SetControllerReference(ledger, ing, r.Scheme)
	})

	return err
}

func buildGrpcIngressRules(ledger *ledgerv1alpha1.LedgerService, hosts []ledgerv1alpha1.IngressHost) []networkingv1.IngressRule {
	rules := make([]networkingv1.IngressRule, 0, len(hosts))
	grpcPort := serviceGrpcPort(ledger)
	grpcSvcName := ledger.Name + "-grpc"

	for _, h := range hosts {
		paths := make([]networkingv1.HTTPIngressPath, 0, len(h.Paths))
		for _, p := range h.Paths {
			pathType := networkingv1.PathTypePrefix
			switch p.PathType {
			case "Exact":
				pathType = networkingv1.PathTypeExact
			case "ImplementationSpecific":
				pathType = networkingv1.PathTypeImplementationSpecific
			}
			paths = append(paths, networkingv1.HTTPIngressPath{
				Path:     p.Path,
				PathType: &pathType,
				Backend: networkingv1.IngressBackend{
					Service: &networkingv1.IngressServiceBackend{
						Name: grpcSvcName,
						Port: networkingv1.ServiceBackendPort{Number: grpcPort},
					},
				},
			})
		}
		rules = append(rules, networkingv1.IngressRule{
			Host: h.Host,
			IngressRuleValue: networkingv1.IngressRuleValue{
				HTTP: &networkingv1.HTTPIngressRuleValue{Paths: paths},
			},
		})
	}

	return rules
}
