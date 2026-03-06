package controller

import (
	"context"
	"maps"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func (r *LedgerServiceReconciler) reconcileGrpcService(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) error {
	name := ledger.Name + "-grpc"

	if ledger.Spec.IngressGrpc == nil || !ledger.Spec.IngressGrpc.Enabled {
		return r.deleteIfExists(ctx, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ledger.Namespace,
			},
		})
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ledger.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = commonLabels(ledger)

		annotations := make(map[string]string)
		maps.Copy(annotations, ledger.Spec.Service.Annotations)
		svc.Annotations = annotations

		svc.Spec = corev1.ServiceSpec{
			Type: ledger.Spec.Service.Type,
			Ports: []corev1.ServicePort{
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
