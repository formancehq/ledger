package controller

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func (r *LedgerServiceReconciler) reconcileServiceAccount(ctx context.Context, ledger *ledgerv1alpha1.LedgerService) error {
	create := ledger.Spec.ServiceAccount.Create
	if create != nil && !*create {
		// SA is managed externally — don't touch it.
		return nil
	}

	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceAccountName(ledger),
			Namespace: ledger.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, sa, func() error {
		sa.Labels = commonLabels(ledger)
		sa.Annotations = ledger.Spec.ServiceAccount.Annotations
		return controllerutil.SetControllerReference(ledger, sa, r.Scheme)
	})
	return err
}

func serviceAccountName(ledger *ledgerv1alpha1.LedgerService) string {
	if ledger.Spec.ServiceAccount.Name != "" {
		return ledger.Spec.ServiceAccount.Name
	}
	return ledger.Name
}
