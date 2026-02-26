package controller

import (
	"context"

	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func (r *LedgerServiceReconciler) reconcileIngress(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) error {
	name := ledger.Name

	if ledger.Spec.Ingress == nil || !ledger.Spec.Ingress.Enabled {
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
		ing.Annotations = ledger.Spec.Ingress.Annotations

		spec := networkingv1.IngressSpec{}
		if ledger.Spec.Ingress.ClassName != "" {
			spec.IngressClassName = &ledger.Spec.Ingress.ClassName
		}

		spec.TLS = buildIngressTLS(ledger.Spec.Ingress.TLS)
		spec.Rules = buildHTTPIngressRules(ledger, ledger.Spec.Ingress.Hosts)

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
