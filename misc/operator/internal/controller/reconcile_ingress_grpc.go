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

	auto := ledger.Spec.AutoNetworking
	autoEnabled := auto != nil && auto.IngressGrpc != nil && auto.IngressGrpc.Enabled
	manualEnabled := ledger.Spec.IngressGrpc != nil && ledger.Spec.IngressGrpc.Enabled

	// For manual mode, also require hosts (e.g. only a TargetGroupBinding may be configured).
	if manualEnabled && len(ledger.Spec.IngressGrpc.Hosts) == 0 {
		manualEnabled = false
	}

	if !autoEnabled && !manualEnabled {
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
		labels := commonLabels(ledger)
		annotations := make(map[string]string)
		var className string
		var hosts []ledgerv1alpha1.IngressHost
		var tlsSpecs []ledgerv1alpha1.IngressTLS

		if autoEnabled {
			cfg := auto.IngressGrpc
			host := autoHost(ledger.Name, auto)

			paths := cfg.Paths
			if len(paths) == 0 {
				paths = []ledgerv1alpha1.IngressPath{{Path: "/", PathType: "Prefix"}}
			}

			hosts = append(hosts, ledgerv1alpha1.IngressHost{
				Host:  host,
				Paths: paths,
			})
			className = cfg.ClassName
			tlsSpecs = append(tlsSpecs, cfg.TLS...)
			maps.Copy(labels, cfg.Labels)
			maps.Copy(annotations, cfg.Annotations)
		}

		if manualEnabled {
			hosts = append(hosts, ledger.Spec.IngressGrpc.Hosts...)
			if ledger.Spec.IngressGrpc.ClassName != "" {
				className = ledger.Spec.IngressGrpc.ClassName
			}
			tlsSpecs = append(tlsSpecs, ledger.Spec.IngressGrpc.TLS...)
			maps.Copy(annotations, ledger.Spec.IngressGrpc.Annotations)
		}

		ing.Labels = labels
		ing.Annotations = annotations

		spec := networkingv1.IngressSpec{}
		if className != "" {
			spec.IngressClassName = &className
		}

		spec.TLS = buildIngressTLS(tlsSpecs)
		spec.Rules = buildGrpcIngressRules(ledger, hosts)

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
