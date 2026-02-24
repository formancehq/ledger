package controller

import (
	"context"

	policyv1 "k8s.io/api/policy/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func (r *LedgerReconciler) reconcilePDB(ctx context.Context, ledger *ledgerv1alpha1.Ledger) error {
	name := ledger.Name

	if ledger.Spec.PodDisruptionBudget == nil || !ledger.Spec.PodDisruptionBudget.Enabled {
		return r.deleteIfExists(ctx, &policyv1.PodDisruptionBudget{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: ledger.Namespace,
			},
		})
	}

	pdb := &policyv1.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ledger.Namespace,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, pdb, func() error {
		pdb.Labels = commonLabels(ledger)
		pdb.Spec = policyv1.PodDisruptionBudgetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: selectorLabels(ledger),
			},
		}

		if ledger.Spec.PodDisruptionBudget.MinAvailable != nil {
			val := intstr.FromInt32(*ledger.Spec.PodDisruptionBudget.MinAvailable)
			pdb.Spec.MinAvailable = &val
		}
		if ledger.Spec.PodDisruptionBudget.MaxUnavailable != nil {
			val := intstr.FromInt32(*ledger.Spec.PodDisruptionBudget.MaxUnavailable)
			pdb.Spec.MaxUnavailable = &val
		}

		return controllerutil.SetControllerReference(ledger, pdb, r.Scheme)
	})
	return err
}
