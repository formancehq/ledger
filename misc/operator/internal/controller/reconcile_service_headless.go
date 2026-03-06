package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func (r *LedgerServiceReconciler) reconcileHeadlessService(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) error {
	enabled := ledger.Spec.HeadlessService.Enabled
	if enabled != nil && !*enabled {
		return r.deleteIfExists(ctx, &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      headlessServiceName(ledger),
				Namespace: ledger.Namespace,
			},
		})
	}

	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      headlessServiceName(ledger),
			Namespace: ledger.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, svc, func() error {
		svc.Labels = commonLabels(ledger)
		svc.Annotations = ledger.Spec.HeadlessService.Annotations
		svc.Spec = corev1.ServiceSpec{
			ClusterIP: corev1.ClusterIPNone,
			Ports: []corev1.ServicePort{
				{
					Name:       "raft",
					Port:       serviceRaftPort(ledger),
					TargetPort: intstr.FromString("raft"),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "grpc",
					Port:       serviceGrpcPort(ledger),
					TargetPort: intstr.FromString("grpc"),
					Protocol:   corev1.ProtocolTCP,
				},
				{
					Name:       "http",
					Port:       serviceHttpPort(ledger),
					TargetPort: intstr.FromString("http"),
					Protocol:   corev1.ProtocolTCP,
				},
			},
			PublishNotReadyAddresses: true,
			Selector:                 selectorLabels(ledger),
		}

		return controllerutil.SetControllerReference(ledger, svc, r.Scheme)
	})

	return err
}

func headlessServiceName(ledger *ledgerv1alpha1.LedgerService) string {
	return ledger.Name + "-headless"
}

func serviceRaftPort(ledger *ledgerv1alpha1.LedgerService) int32 {
	if ledger.Spec.Service.RaftPort != 0 {
		return ledger.Spec.Service.RaftPort
	}

	return 7777
}

func serviceGrpcPort(ledger *ledgerv1alpha1.LedgerService) int32 {
	if ledger.Spec.Service.GrpcPort != 0 {
		return ledger.Spec.Service.GrpcPort
	}

	return 8888
}

func serviceHttpPort(ledger *ledgerv1alpha1.LedgerService) int32 {
	if ledger.Spec.Service.HttpPort != 0 {
		return ledger.Spec.Service.HttpPort
	}

	return 9000
}
