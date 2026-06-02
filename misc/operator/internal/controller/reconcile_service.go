package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func (r *LedgerServiceReconciler) reconcileService(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) error {
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ledger.Name,
			Namespace: ledger.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = commonLabels(ledger)
		svc.Annotations = ledger.Spec.Service.Annotations
		svc.Spec = corev1.ServiceSpec{
			Type: ledger.Spec.Service.Type,
			Ports: []corev1.ServicePort{
				{
					Name:       "http",
					Port:       serviceHttpPort(ledger),
					TargetPort: intstr.FromString("http"),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "grpc",
					Port:       serviceGrpcPort(ledger),
					TargetPort: intstr.FromString("grpc"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			Selector: selectorLabels(ledger),
		}

		return controllerutil.SetControllerReference(ledger, svc, r.Scheme)
	})

	return err
}
