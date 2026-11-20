package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
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
// when the Cluster requested whenDeleted=Delete. Claims are read from the API
// server: a cached owner reference would release the finalizer on a claim that
// garbage collection no longer reaps. A claim still owned by a StatefulSet is
// left to garbage collection, and a claim recreated under a recorded name is
// not the upgrade's claim.
func (r *ClusterReconciler) finalizeUpgradeClaims(ctx context.Context, ledger *ledgerv1alpha1.Cluster) error {
	if !controllerutil.ContainsFinalizer(ledger, upgradeClaimsFinalizer) {
		return nil
	}
	policy := ledger.Spec.Persistence.RetentionPolicy
	if ledger.Status.Upgrade != nil && policy != nil && policy.WhenDeleted == "Delete" {
		for name, uid := range ledger.Status.Upgrade.ClaimUIDs {
			pvc := &corev1.PersistentVolumeClaim{}
			err := r.storageAPIReader().Get(ctx, types.NamespacedName{Name: name, Namespace: ledger.Namespace}, pvc)
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return fmt.Errorf("reading upgrade PVC %s: %w", name, err)
			}
			if string(pvc.UID) != uid || !pvc.DeletionTimestamp.IsZero() || len(pvc.OwnerReferences) != 0 {
				continue
			}
			// The resourceVersion fences the owner check against a concurrent adoption.
			observedUID, observedVersion := pvc.UID, pvc.ResourceVersion
			err = r.Delete(ctx, pvc, client.Preconditions{UID: &observedUID, ResourceVersion: &observedVersion})
			if err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("deleting upgrade PVC %s: %w", name, err)
			}
		}
	}

	return r.releaseUpgradeClaimsFinalizer(ctx, ledger)
}

// reconcileUpgradeClaimAdoption restores the requested retention policy on the
// replacement StatefulSet and keeps the claim inventory and finalizer until
// garbage collection can reap every recorded claim that whenDeleted=Delete
// covers. Every recorded claim and volume must keep its identity until then:
// a lost claim would be recreated empty under its name. Without a StatefulSet,
// ordinary reconciliation recreates it with the requested policy.
func (r *ClusterReconciler) reconcileUpgradeClaimAdoption(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet) (bool, ctrl.Result, error) {
	if err := r.verifyUpgradeClaims(ctx, ledger, ledger.Status.Upgrade); err != nil {
		return false, ctrl.Result{}, err
	}
	if sts == nil {
		return true, ctrl.Result{}, nil
	}
	if !metav1.IsControlledBy(sts, ledger) {
		return false, ctrl.Result{}, fmt.Errorf("StatefulSet %s is not controlled by Cluster %s", sts.Name, ledger.Name)
	}
	changed, err := r.reconcileStatefulSetRetentionPolicy(ctx, ledger, sts)
	if err != nil {
		return false, ctrl.Result{}, fmt.Errorf("restoring replacement StatefulSet claim retention: %w", err)
	}
	if changed {
		return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
	}
	if buildRetentionPolicy(ledger).WhenDeleted == appsv1.DeletePersistentVolumeClaimRetentionPolicyType {
		for name, uid := range ledger.Status.Upgrade.ClaimUIDs {
			pvc := &corev1.PersistentVolumeClaim{}
			if err := r.storageAPIReader().Get(ctx, types.NamespacedName{Name: name, Namespace: ledger.Namespace}, pvc); err != nil {
				return false, ctrl.Result{}, fmt.Errorf("reading upgrade PVC %s: %w", name, err)
			}
			if string(pvc.UID) != uid || !pvc.DeletionTimestamp.IsZero() {
				return false, ctrl.Result{}, fmt.Errorf("upgrade PVC %s changed identity or is deleting", name)
			}
			if ownedBy(pvc, sts.UID) {
				continue
			}

			return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
		}
	}
	if err := r.persistUpgrade(ctx, ledger, nil); err != nil {
		return false, ctrl.Result{}, err
	}
	if err := r.releaseUpgradeClaimsFinalizer(ctx, ledger); err != nil {
		return false, ctrl.Result{}, err
	}

	return true, ctrl.Result{}, nil
}

func ownedBy(obj metav1.Object, owner types.UID) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID == owner {
			return true
		}
	}

	return false
}
