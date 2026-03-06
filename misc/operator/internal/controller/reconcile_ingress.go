package controller

import (
	"context"
	"maps"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func (r *LedgerServiceReconciler) reconcileIngress(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) error {
	name := ledger.Name

	autoEnabled := ledger.Spec.AutoIngress != nil && ledger.Spec.AutoIngress.Enabled
	manualEnabled := ledger.Spec.Ingress != nil && ledger.Spec.Ingress.Enabled

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
		ing.Labels = commonLabels(ledger)

		annotations := make(map[string]string)
		var className string
		var hosts []ledgerv1alpha1.IngressHost
		var tlsSpecs []ledgerv1alpha1.IngressTLS

		if autoEnabled {
			auto := ledger.Spec.AutoIngress
			host := ledger.Name + auto.Suffix + "." + auto.TLD

			paths := auto.Paths
			if len(paths) == 0 {
				paths = []ledgerv1alpha1.IngressPath{{Path: "/", PathType: "Prefix"}}
			}

			hosts = append(hosts, ledgerv1alpha1.IngressHost{
				Host:  host,
				Paths: paths,
			})
			className = auto.ClassName
			tlsSpecs = append(tlsSpecs, auto.TLS...)
			maps.Copy(annotations, auto.Annotations)
		}

		if manualEnabled {
			hosts = append(hosts, ledger.Spec.Ingress.Hosts...)
			if ledger.Spec.Ingress.ClassName != "" {
				className = ledger.Spec.Ingress.ClassName
			}
			tlsSpecs = append(tlsSpecs, ledger.Spec.Ingress.TLS...)
			maps.Copy(annotations, ledger.Spec.Ingress.Annotations)
		}

		ing.Annotations = annotations

		spec := networkingv1.IngressSpec{}
		if className != "" {
			spec.IngressClassName = &className
		}

		spec.TLS = buildIngressTLS(tlsSpecs)
		spec.Rules = buildHTTPIngressRules(ledger, hosts)

		ing.Spec = spec

		return controllerutil.SetControllerReference(ledger, ing, r.Scheme)
	})

	return err
}

func buildHTTPIngressRules(ledger *ledgerv1alpha1.LedgerService, hosts []ledgerv1alpha1.IngressHost) []networkingv1.IngressRule {
	rules := make([]networkingv1.IngressRule, 0, len(hosts))
	httpPort := serviceHttpPort(ledger)

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
						Name: ledger.Name,
						Port: networkingv1.ServiceBackendPort{Number: httpPort},
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

func buildIngressTLS(tlsSpecs []ledgerv1alpha1.IngressTLS) []networkingv1.IngressTLS {
	if len(tlsSpecs) == 0 {
		return nil
	}
	result := make([]networkingv1.IngressTLS, 0, len(tlsSpecs))
	for _, t := range tlsSpecs {
		result = append(result, networkingv1.IngressTLS{
			Hosts:      t.Hosts,
			SecretName: t.SecretName,
		})
	}

	return result
}
