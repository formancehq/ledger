package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// A stopped upgrade forces Retain and detaches the recorded claims, so garbage
// collection cannot apply the requested whenDeleted policy to them. This
// finalizer holds Cluster deletion until the operator has applied it.
const upgradeClaimsFinalizer = "ledger.formance.com/upgrade-claims"

func (r *ClusterReconciler) ensureUpgradeClaimsFinalizer(ctx context.Context, ledger *ledgerv1alpha1.Cluster) error {
	if controllerutil.ContainsFinalizer(ledger, upgradeClaimsFinalizer) {
		return nil
	}

	return r.patchFinalizers(ctx, ledger, func(obj *ledgerv1alpha1.Cluster) {
		controllerutil.AddFinalizer(obj, upgradeClaimsFinalizer)
	})
}

func (r *ClusterReconciler) releaseUpgradeClaimsFinalizer(ctx context.Context, ledger *ledgerv1alpha1.Cluster) error {
	if !controllerutil.ContainsFinalizer(ledger, upgradeClaimsFinalizer) {
		return nil
	}

	return r.patchFinalizers(ctx, ledger, func(obj *ledgerv1alpha1.Cluster) {
		controllerutil.RemoveFinalizer(obj, upgradeClaimsFinalizer)
	})
}

func (r *ClusterReconciler) patchFinalizers(ctx context.Context, ledger *ledgerv1alpha1.Cluster, mutate func(*ledgerv1alpha1.Cluster)) error {
	before := ledger.DeepCopy()
	updated := ledger.DeepCopy()
	mutate(updated)
	if err := r.Patch(ctx, updated, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("patching Cluster finalizers: %w", err)
	}
	ledger.ResourceVersion = updated.ResourceVersion
	ledger.Finalizers = updated.Finalizers

	return nil
}

// finalizeUpgradeClaims deletes the recorded claims that no owner would reap
// when the Cluster requested whenDeleted=Delete. A claim still owned by a
// StatefulSet is left to garbage collection, and a claim recreated under a
// recorded name is not the upgrade's claim.
func (r *ClusterReconciler) finalizeUpgradeClaims(ctx context.Context, ledger *ledgerv1alpha1.Cluster) error {
	if !controllerutil.ContainsFinalizer(ledger, upgradeClaimsFinalizer) {
		return nil
	}
	policy := ledger.Spec.Persistence.RetentionPolicy
	if ledger.Status.Upgrade != nil && policy != nil && policy.WhenDeleted == "Delete" {
		for name, uid := range ledger.Status.Upgrade.ClaimUIDs {
			pvc := &corev1.PersistentVolumeClaim{}
			err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ledger.Namespace}, pvc)
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return fmt.Errorf("reading upgrade PVC %s: %w", name, err)
			}
			if string(pvc.UID) != uid || !pvc.DeletionTimestamp.IsZero() || len(pvc.OwnerReferences) != 0 {
				continue
			}
			if err := deleteInspected(ctx, r.Client, pvc); err != nil {
				return fmt.Errorf("deleting upgrade PVC %s: %w", name, err)
			}
		}
	}

	return r.releaseUpgradeClaimsFinalizer(ctx, ledger)
}
