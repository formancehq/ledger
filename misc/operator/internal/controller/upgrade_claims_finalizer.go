package controller

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// A stopped upgrade forces Retain, detaches the recorded claims and later hands
// them back to the replacement StatefulSet, so garbage collection cannot be
// relied on to apply the requested whenDeleted policy to them. This finalizer
// holds Cluster deletion until the operator has applied it.
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

// finalizeUpgradeClaims applies the requested whenDeleted policy to every
// recorded claim itself. A stopped upgrade changes which claims the StatefulSet
// owns, so an owner reference read here does not tell what garbage collection
// will do once the Cluster is gone. Under Delete each recorded claim is
// deleted; under Retain the workload's owner references are removed once no
// StatefulSet can add them back. Claims are read from the API server. A claim
// recreated under a recorded name is not the upgrade's claim, and a claim
// already deleting cannot be retained.
func (r *ClusterReconciler) finalizeUpgradeClaims(ctx context.Context, ledger *ledgerv1alpha1.Cluster) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(ledger, upgradeClaimsFinalizer) {
		return ctrl.Result{}, nil
	}
	if state := ledger.Status.Upgrade; state != nil {
		sts := &appsv1.StatefulSet{}
		err := r.storageAPIReader().Get(ctx, types.NamespacedName{Name: resourceName(ledger.Name), Namespace: ledger.Namespace}, sts)
		if apierrors.IsNotFound(err) {
			sts = nil
		} else if err != nil {
			return ctrl.Result{}, fmt.Errorf("reading StatefulSet: %w", err)
		}
		policy := ledger.Spec.Persistence.RetentionPolicy
		deleteClaims := policy != nil && policy.WhenDeleted == "Delete"
		if !deleteClaims {
			settled, err := r.statefulSetRetainsClaims(ctx, ledger, sts)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !settled {
				return ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
			}
		}
		for name, uid := range state.ClaimUIDs {
			pvc := &corev1.PersistentVolumeClaim{}
			err := r.storageAPIReader().Get(ctx, types.NamespacedName{Name: name, Namespace: ledger.Namespace}, pvc)
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return ctrl.Result{}, fmt.Errorf("reading upgrade PVC %s: %w", name, err)
			}
			if string(pvc.UID) != uid || !pvc.DeletionTimestamp.IsZero() {
				continue
			}
			otherOwners, err := r.ownersOutsideWorkload(ctx, ledger, sts, pvc)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !deleteClaims {
				if len(otherOwners) == len(pvc.OwnerReferences) {
					continue
				}
				detached := pvc.DeepCopy()
				detached.OwnerReferences = otherOwners
				if err := r.Patch(ctx, detached, client.MergeFromWithOptions(pvc, client.MergeFromWithOptimisticLock{})); err != nil {
					return ctrl.Result{}, fmt.Errorf("detaching owner references from PVC %s: %w", name, err)
				}

				continue
			}
			// Another owner keeps the claim alive under garbage collection too.
			if len(otherOwners) != 0 {
				continue
			}
			observedUID, observedVersion := pvc.UID, pvc.ResourceVersion
			err = r.Delete(ctx, pvc, client.Preconditions{UID: &observedUID, ResourceVersion: &observedVersion})
			if err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, fmt.Errorf("deleting upgrade PVC %s: %w", name, err)
			}
		}
	}

	return ctrl.Result{}, r.releaseUpgradeClaimsFinalizer(ctx, ledger)
}

// statefulSetRetainsClaims reports whether the Cluster's StatefulSet can no
// longer add its owner reference to a claim: it is gone or deleting, or its
// controller has observed the Retain policy applied here.
func (r *ClusterReconciler) statefulSetRetainsClaims(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet) (bool, error) {
	if sts == nil || !metav1.IsControlledBy(sts, ledger) || !sts.DeletionTimestamp.IsZero() {
		return true, nil
	}
	changed, err := r.reconcileStatefulSetRetentionPolicy(ctx, ledger, sts)
	if err != nil {
		return false, fmt.Errorf("retaining StatefulSet claims: %w", err)
	}

	return !changed && sts.Status.ObservedGeneration >= sts.Generation, nil
}

// ownersOutsideWorkload returns the claim's owner references that garbage
// collection does not remove with the Cluster. A reference named after the
// Cluster's StatefulSet or one of its Pods counts as the workload's only by
// UID: the object it designates no longer exists, or exists and belongs to the
// Cluster. A live object of the same name that is not the Cluster's is another
// owner.
func (r *ClusterReconciler) ownersOutsideWorkload(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, pvc *corev1.PersistentVolumeClaim) ([]metav1.OwnerReference, error) {
	stsName := resourceName(ledger.Name)
	workloadUIDs := map[types.UID]bool{types.UID(ledger.Status.Upgrade.StatefulSetUID): true}
	if sts != nil && metav1.IsControlledBy(sts, ledger) {
		workloadUIDs[sts.UID] = true
	}
	var others []metav1.OwnerReference
	for _, owner := range pvc.OwnerReferences {
		group, err := schema.ParseGroupVersion(owner.APIVersion)
		if err != nil {
			others = append(others, owner)

			continue
		}
		switch {
		case group.Group == appsv1.GroupName && owner.Kind == "StatefulSet" && owner.Name == stsName:
			if sts == nil || sts.UID != owner.UID || metav1.IsControlledBy(sts, ledger) {
				continue
			}
		case group.Group == corev1.GroupName && owner.Kind == "Pod" && hasStatefulSetOrdinal(owner.Name, stsName):
			pod := &corev1.Pod{}
			err := r.storageAPIReader().Get(ctx, types.NamespacedName{Name: owner.Name, Namespace: ledger.Namespace}, pod)
			if err != nil && !apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("reading Pod %s: %w", owner.Name, err)
			}
			if err != nil || pod.UID != owner.UID {
				continue
			}
			if controller := metav1.GetControllerOf(pod); controller != nil && controller.Kind == "StatefulSet" && controller.Name == stsName && workloadUIDs[controller.UID] {
				continue
			}
		}
		others = append(others, owner)
	}

	return others, nil
}

// reconcileUpgradeClaimAdoption restores the requested retention policy on the
// replacement StatefulSet and keeps the claim inventory and finalizer until its
// controller has observed that policy and the claims' ownership matches it:
// under whenDeleted=Delete, garbage collection must reach every recorded claim
// through the replacement; under Retain, the replacement must own none. A
// StatefulSet controller that has not observed the policy can still change
// ownership after it was read. Every recorded claim and volume must keep its identity until then:
// a lost claim would be recreated empty under its name. Without a StatefulSet,
// ordinary reconciliation recreates it with the requested policy.
func (r *ClusterReconciler) reconcileUpgradeClaimAdoption(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet) (bool, ctrl.Result, error) {
	upgrade := stoppedUpgradeReconciler{client: r.Client}
	if err := upgrade.verifyUpgradeClaims(ctx, ledger, ledger.Status.Upgrade); err != nil {
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
	if changed || sts.Status.ObservedGeneration < sts.Generation {
		return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
	}
	deleteClaims := buildRetentionPolicy(ledger).WhenDeleted == appsv1.DeletePersistentVolumeClaimRetentionPolicyType
	for name, uid := range ledger.Status.Upgrade.ClaimUIDs {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.storageAPIReader().Get(ctx, types.NamespacedName{Name: name, Namespace: ledger.Namespace}, pvc); err != nil {
			return false, ctrl.Result{}, fmt.Errorf("reading upgrade PVC %s: %w", name, err)
		}
		if string(pvc.UID) != uid || !pvc.DeletionTimestamp.IsZero() {
			return false, ctrl.Result{}, fmt.Errorf("upgrade PVC %s changed identity or is deleting", name)
		}
		if ownedBy(pvc, sts.UID) != deleteClaims {
			return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
		}
	}
	if err := upgrade.persistUpgrade(ctx, ledger, nil); err != nil {
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
