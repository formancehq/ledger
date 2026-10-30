package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/google/uuid"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// Explicit-target prepared-query orders cannot be applied identically by rc.2
// voters. Stop every rc.2 node before starting rc.3; RC adjacency is still
// checked by validateRCUpgrade.
var mandatoryStopVersions = []string{"v3.0.0-rc.3"}

const requiredUpgradePoll = 5 * time.Second

func requiresStoppedUpgrade(source, target string, boundaries []string) bool {
	if source == target || !strings.HasPrefix(source, "v") || !strings.HasPrefix(target, "v") {
		return false
	}
	from, err := semver.StrictNewVersion(strings.TrimPrefix(source, "v"))
	if err != nil {
		return false
	}
	to, err := semver.StrictNewVersion(strings.TrimPrefix(target, "v"))
	if err != nil || !to.GreaterThan(from) {
		return false
	}
	for _, boundary := range boundaries {
		version, err := semver.StrictNewVersion(strings.TrimPrefix(boundary, "v"))
		if err != nil {
			panic("invalid mandatory stop version: " + boundary)
		}
		if from.LessThan(version) && !to.LessThan(version) {
			return true
		}
	}

	return false
}

// A moving or non-canonical target cannot establish whether a mandatory
// boundary will be crossed, so an older published source must stay pinned.
func requiresPinnedUpgradeTarget(source, target string, boundaries []string) bool {
	if source == target || !strings.HasPrefix(source, "v") {
		return false
	}
	from, err := semver.StrictNewVersion(strings.TrimPrefix(source, "v"))
	if err != nil || "v"+from.String() != source {
		return false
	}
	if after, ok := strings.CutPrefix(target, "v"); ok {
		to, err := semver.StrictNewVersion(after)
		if err == nil && "v"+to.String() == target {
			return false
		}
	}
	for _, boundary := range boundaries {
		version, err := semver.StrictNewVersion(strings.TrimPrefix(boundary, "v"))
		if err != nil {
			panic("invalid mandatory stop version: " + boundary)
		}
		if from.LessThan(version) {
			return true
		}
	}

	return false
}

// reconcileRequiredUpgrade returns proceed=true only when ordinary StatefulSet
// reconciliation may safely run. A persisted intent also owns the missing-STS
// case, so the generic create path cannot start a new pod before the old pods
// disappear. Each transition is idempotent across operator restarts.
func (r *ClusterReconciler) reconcileRequiredUpgrade(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet) (proceed bool, result ctrl.Result, err error) {
	return r.reconcileRequiredUpgradeWithExec(ctx, ledger, sts, podExecWithTimeout)
}

func (r *ClusterReconciler) reconcileRequiredUpgradeWithExec(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, exec ledgerctlExec) (proceed bool, result ctrl.Result, err error) {
	state := ledger.Status.Upgrade
	if state == nil && sts == nil {
		return true, ctrl.Result{}, nil
	}
	if state == nil {
		source, err := ledgerImageTag(sts)
		if err != nil {
			return true, ctrl.Result{}, nil
		}
		boundaries := mandatoryStopVersions
		if r.stopVersions != nil {
			boundaries = r.stopVersions
		}
		if requiresPinnedUpgradeTarget(source, ledger.Spec.Image.Tag, boundaries) {
			return false, ctrl.Result{}, fmt.Errorf("cannot verify mandatory stopped upgrade from %s to image tag %q; pin a canonical version tag", source, ledger.Spec.Image.Tag)
		}
		if !requiresStoppedUpgrade(source, ledger.Spec.Image.Tag, boundaries) {
			return true, ctrl.Result{}, nil
		}
		if _, requested := ledger.Annotations[ClusterDrainAnnotation]; requested || ledger.Status.Drain != nil {
			return false, ctrl.Result{}, errors.New("finish the annotated drain before starting a stopped upgrade")
		}
		if ledger.Spec.HeadlessService.Enabled != nil && !*ledger.Spec.HeadlessService.Enabled {
			return false, ctrl.Result{}, errors.New("stopped upgrade requires spec.headlessService.enabled to remain true")
		}
		if !metav1.IsControlledBy(sts, ledger) {
			return false, ctrl.Result{}, fmt.Errorf("StatefulSet %s is not controlled by Cluster %s", sts.Name, ledger.Name)
		}
		if sts.UID == "" {
			return false, ctrl.Result{}, errors.New("source StatefulSet UID is unknown")
		}
		if !ledger.Spec.Persistence.WAL.IsPVC() || !ledger.Spec.Persistence.Data.IsPVC() {
			return false, ctrl.Result{}, errors.New("stopped upgrade requires PVC-backed WAL and data volumes")
		}
		claims, err := r.upgradeClaims(ctx, ledger, sts)
		if err != nil {
			return false, ctrl.Result{}, err
		}
		volumes, err := r.upgradeVolumes(ctx, ledger, claims)
		if err != nil {
			return false, ctrl.Result{}, err
		}
		if sts.Spec.Replicas == nil {
			return false, ctrl.Result{}, errors.New("source StatefulSet replica count is unknown")
		}
		if ledger.Spec.Replicas == nil || *ledger.Spec.Replicas != *sts.Spec.Replicas {
			return false, ctrl.Result{}, errors.New("finish replica changes before starting a stopped upgrade")
		}
		state = &ledgerv1alpha1.ClusterUpgradeStatus{Source: source, Target: ledger.Spec.Image.Tag, StatefulSetUID: string(sts.UID), TargetSpecHash: computeSpecHash(&ledger.Spec), ExpectedReplicas: *sts.Spec.Replicas, ClaimUIDs: claims, VolumeUIDs: volumes, DrainEpoch: uuid.NewString(), Phase: "Preflighting"}
		if err := r.persistUpgrade(ctx, ledger, state); err != nil {
			return false, ctrl.Result{}, err
		}
	}
	if ledger.Spec.Image.Tag != state.Target {
		return false, ctrl.Result{}, fmt.Errorf("upgrade from %s to %s is in progress; restore spec.image.tag to %s before changing target", state.Source, state.Target, state.Target)
	}
	if computeSpecHash(&ledger.Spec) != state.TargetSpecHash || ledger.Spec.Replicas == nil || *ledger.Spec.Replicas != state.ExpectedReplicas {
		return false, ctrl.Result{}, errors.New("cluster workload spec changed during stopped upgrade; restore the original target spec before retrying")
	}
	if sts != nil && !metav1.IsControlledBy(sts, ledger) {
		return false, ctrl.Result{}, fmt.Errorf("StatefulSet %s is not controlled by Cluster %s", sts.Name, ledger.Name)
	}
	if sts != nil && string(sts.UID) != state.StatefulSetUID && state.Phase != "Recreating" && state.Phase != "Verifying" && state.Phase != "Unblocking" {
		return false, ctrl.Result{}, fmt.Errorf("StatefulSet identity changed during stopped upgrade: expected %s, got %s", state.StatefulSetUID, sts.UID)
	}
	if sts != nil && string(sts.UID) == state.StatefulSetUID && (sts.Spec.Replicas == nil || *sts.Spec.Replicas != state.ExpectedReplicas) {
		return false, ctrl.Result{}, fmt.Errorf("source StatefulSet replica count differs from the pinned upgrade count %d; restore the expected count before retrying", state.ExpectedReplicas)
	}
	if err := r.verifyUpgradeClaims(ctx, ledger, state); err != nil {
		return false, ctrl.Result{}, err
	}
	if sts != nil && string(sts.UID) != state.StatefulSetUID && (state.Phase == "Recreating" || state.Phase == "Verifying" || state.Phase == "Unblocking") {
		changed, err := r.reconcileStatefulSetRetentionPolicy(ctx, ledger, sts)
		if err != nil {
			return false, ctrl.Result{}, fmt.Errorf("retaining replacement StatefulSet claims: %w", err)
		}
		if changed {
			return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
		}
	}
	switch state.Phase {
	case "Preflighting":
		if sts == nil {
			return false, ctrl.Result{}, errors.New("source StatefulSet disappeared before drain preflight")
		}
		live, err := drainClusterFromWorkload(ledger, sts)
		if err != nil {
			return false, ctrl.Result{}, err
		}
		if err := preflightDrain(ctx, r.upgradeDrainCluster(live, sts, exec)); err != nil {
			return false, ctrl.Result{}, fmt.Errorf("preflighting stopped-upgrade drain: %w", err)
		}
		if err := r.advanceUpgrade(ctx, ledger, "Draining"); err != nil {
			return false, ctrl.Result{}, err
		}

		return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
	case "Draining":
		if sts == nil {
			return false, ctrl.Result{}, errors.New("source StatefulSet disappeared before drain barrier")
		}
		if state.DrainEpoch == "" {
			return false, ctrl.Result{}, errors.New("stopped upgrade has no drain epoch")
		}
		live, err := drainClusterFromWorkload(ledger, sts)
		if err != nil {
			return false, ctrl.Result{}, err
		}
		ready, err := ensureDrain(ctx, r.upgradeDrainCluster(live, sts, exec), state.DrainEpoch)
		if err != nil {
			return false, ctrl.Result{}, err
		}
		if !ready {
			return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
		}
		if err := r.advanceUpgrade(ctx, ledger, "Retaining"); err != nil {
			return false, ctrl.Result{}, err
		}

		return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
	case "Retaining":
		if sts == nil {
			return false, ctrl.Result{}, errors.New("source StatefulSet disappeared before retention was established")
		}
		changed, err := r.reconcileStatefulSetRetentionPolicy(ctx, ledger, sts)
		if err != nil {
			return false, ctrl.Result{}, fmt.Errorf("retaining StatefulSet claims: %w", err)
		}
		if changed {
			return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
		}
		if sts.Status.ObservedGeneration < sts.Generation {
			return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
		}
		if err := r.detachUpgradeClaimOwners(ctx, ledger, state); err != nil {
			return false, ctrl.Result{}, err
		}
		if err := r.advanceUpgrade(ctx, ledger, "Deleting"); err != nil {
			return false, ctrl.Result{}, err
		}

		return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
	case "Deleting":
		if sts != nil {
			if err := r.verifyDetachedUpgradeClaims(ctx, ledger, state); err != nil {
				return false, ctrl.Result{}, err
			}
			if sts.DeletionTimestamp.IsZero() {
				live, err := drainClusterFromWorkload(ledger, sts)
				if err != nil {
					return false, ctrl.Result{}, err
				}
				ready, err := ensureDrain(ctx, r.upgradeDrainCluster(live, sts, exec), state.DrainEpoch)
				if err != nil {
					return false, ctrl.Result{}, fmt.Errorf("rechecking drain barrier before delete: %w", err)
				}
				if !ready {
					return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
				}
				if err := deleteInspected(ctx, r.Client, sts, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
					return false, ctrl.Result{}, fmt.Errorf("deleting source StatefulSet: %w", err)
				}
			}

			return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
		}
		pods := &corev1.PodList{}
		if err := r.List(ctx, pods, client.InNamespace(ledger.Namespace)); err != nil {
			return false, ctrl.Result{}, fmt.Errorf("listing old pods: %w", err)
		}
		for _, pod := range pods.Items {
			if hasStatefulSetOrdinal(pod.Name, resourceName(ledger.Name)) {
				return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
			}
		}
		if err := r.advanceUpgrade(ctx, ledger, "Recreating"); err != nil {
			return false, ctrl.Result{}, err
		}

		return true, ctrl.Result{}, nil
	case "Recreating":
		if sts == nil {
			gone, err := r.upgradeOrdinalPodsGone(ctx, ledger)
			if err != nil {
				return false, ctrl.Result{}, err
			}
			if !gone {
				return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
			}

			return true, ctrl.Result{}, nil
		}
		if err := r.advanceUpgrade(ctx, ledger, "Verifying"); err != nil {
			return false, ctrl.Result{}, err
		}

		return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
	case "Verifying":
		if sts == nil {
			return r.resumeMissingUpgradeReplacement(ctx, ledger)
		}
		if string(sts.UID) == state.StatefulSetUID {
			return false, ctrl.Result{}, errors.New("source StatefulSet still exists during target verification")
		}
		tag, err := ledgerImageTag(sts)
		if err != nil || tag != state.Target {
			return false, ctrl.Result{}, fmt.Errorf("replacement StatefulSet has unexpected image: %w (%s)", err, tag)
		}
		if err := verifyReplacementReplicaCount(sts, state); err != nil {
			return false, ctrl.Result{}, err
		}
		if err := r.restoreUpgradeProgress(ctx, ledger, state); err != nil {
			return false, ctrl.Result{}, err
		}
		if !replacementConverged(sts, state) {
			return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
		}
		if err := r.advanceUpgrade(ctx, ledger, "Unblocking"); err != nil {
			return false, ctrl.Result{}, err
		}

		return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
	case "Unblocking":
		if sts == nil {
			return r.resumeMissingUpgradeReplacement(ctx, ledger)
		}
		tag, err := ledgerImageTag(sts)
		if err != nil {
			return false, ctrl.Result{}, fmt.Errorf("reading replacement StatefulSet image before drain release: %w", err)
		}
		if tag != state.Target || string(sts.UID) == state.StatefulSetUID {
			return false, ctrl.Result{}, fmt.Errorf("replacement StatefulSet identity changed before drain release: expected target %s with a new UID, got tag %s and UID %s", state.Target, tag, sts.UID)
		}
		if err := verifyReplacementReplicaCount(sts, state); err != nil {
			return false, ctrl.Result{}, err
		}
		if err := r.restoreUpgradeProgress(ctx, ledger, state); err != nil {
			return false, ctrl.Result{}, err
		}
		if !replacementConverged(sts, state) {
			return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
		}
		if state.DrainEpoch == "" {
			return false, ctrl.Result{}, errors.New("stopped upgrade has no drain epoch")
		}
		live, err := drainClusterFromWorkload(ledger, sts)
		if err != nil {
			return false, ctrl.Result{}, err
		}
		ready, err := ensureDrainReleased(ctx, r.upgradeDrainCluster(live, sts, exec), state.DrainEpoch)
		if err != nil {
			return false, ctrl.Result{}, fmt.Errorf("releasing stopped-upgrade drain: %w", err)
		}
		if !ready {
			return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
		}
		if err := r.persistUpgrade(ctx, ledger, nil); err != nil {
			return false, ctrl.Result{}, err
		}

		return true, ctrl.Result{}, nil
	default:
		return false, ctrl.Result{}, fmt.Errorf("unknown stopped upgrade phase %q", state.Phase)
	}
}

func replacementConverged(sts *appsv1.StatefulSet, state *ledgerv1alpha1.ClusterUpgradeStatus) bool {
	return sts != nil && sts.Spec.Replicas != nil && *sts.Spec.Replicas == state.ExpectedReplicas && rolloutConverged(sts)
}

func verifyReplacementReplicaCount(sts *appsv1.StatefulSet, state *ledgerv1alpha1.ClusterUpgradeStatus) error {
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas != state.ExpectedReplicas {
		return fmt.Errorf("replacement StatefulSet replica count differs from the pinned upgrade count %d; restore the expected count before retrying", state.ExpectedReplicas)
	}

	return nil
}

func (r *ClusterReconciler) restoreUpgradeProgress(ctx context.Context, ledger *ledgerv1alpha1.Cluster, state *ledgerv1alpha1.ClusterUpgradeStatus) error {
	condition := meta.FindStatusCondition(ledger.Status.Conditions, "Upgrade")
	if condition == nil || condition.Status != metav1.ConditionFalse {
		return nil
	}

	return r.persistUpgrade(ctx, ledger, state)
}

func (r *ClusterReconciler) resumeMissingUpgradeReplacement(ctx context.Context, ledger *ledgerv1alpha1.Cluster) (bool, ctrl.Result, error) {
	gone, err := r.upgradeOrdinalPodsGone(ctx, ledger)
	if err != nil {
		return false, ctrl.Result{}, err
	}
	if !gone {
		return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
	}
	if err := r.advanceUpgrade(ctx, ledger, "Recreating"); err != nil {
		return false, ctrl.Result{}, err
	}

	return true, ctrl.Result{}, nil
}

func (r *ClusterReconciler) upgradeOrdinalPodsGone(ctx context.Context, ledger *ledgerv1alpha1.Cluster) (bool, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(ledger.Namespace)); err != nil {
		return false, fmt.Errorf("listing Pods during stopped upgrade: %w", err)
	}
	for _, pod := range pods.Items {
		if hasStatefulSetOrdinal(pod.Name, resourceName(ledger.Name)) {
			return false, nil
		}
	}

	return true, nil
}

func (r *ClusterReconciler) persistUpgrade(ctx context.Context, ledger *ledgerv1alpha1.Cluster, state *ledgerv1alpha1.ClusterUpgradeStatus) error {
	before := ledger.DeepCopy()
	updated := ledger.DeepCopy()
	updated.Status.Upgrade = state
	if state == nil {
		meta.RemoveStatusCondition(&updated.Status.Conditions, "Upgrade")
	} else {
		meta.SetStatusCondition(&updated.Status.Conditions, metav1.Condition{Type: "Upgrade", Status: metav1.ConditionTrue, Reason: state.Phase, Message: fmt.Sprintf("stopped upgrade %s to %s: %s", state.Source, state.Target, state.Phase), ObservedGeneration: ledger.Generation})
	}
	if err := r.Status().Patch(ctx, updated, client.MergeFromWithOptions(before, client.MergeFromWithOptimisticLock{})); err != nil {
		return fmt.Errorf("persisting stopped upgrade state: %w", err)
	}
	ledger.ResourceVersion = updated.ResourceVersion
	ledger.Status.Upgrade = state
	ledger.Status.Conditions = updated.Status.Conditions

	return nil
}

func (r *ClusterReconciler) advanceUpgrade(ctx context.Context, ledger *ledgerv1alpha1.Cluster, phase string) error {
	next := ledger.Status.Upgrade.DeepCopy()
	next.Phase = phase

	return r.persistUpgrade(ctx, ledger, next)
}

func (r *ClusterReconciler) upgradeDrainCluster(live *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, exec ledgerctlExec) *ledgerctlDrainCluster {
	return &ledgerctlDrainCluster{
		cluster: live, tlsMode: currentTLSModeFromStatefulSet(sts),
		config: r.Config, clientset: r.Clientset, exec: ownedPodExec(sts, exec),
	}
}

func (r *ClusterReconciler) upgradeClaims(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet) (map[string]string, error) {
	if len(sts.Spec.VolumeClaimTemplates) != 2 {
		return nil, errors.New("stopped upgrade requires existing WAL and data PVC templates")
	}
	seen := make(map[string]bool)
	for _, template := range sts.Spec.VolumeClaimTemplates {
		seen[template.Name] = true
	}
	if !seen["wal"] || !seen["data"] {
		return nil, errors.New("stopped upgrade requires existing WAL and data PVC templates")
	}
	claims := make(map[string]string)
	replicas := int32(1)
	if sts.Spec.Replicas != nil {
		replicas = *sts.Spec.Replicas
	}
	for _, template := range sts.Spec.VolumeClaimTemplates {
		for ordinal := range replicas {
			name := fmt.Sprintf("%s-%s-%d", template.Name, sts.Name, ordinal)
			pvc := &corev1.PersistentVolumeClaim{}
			if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ledger.Namespace}, pvc); err != nil {
				return nil, fmt.Errorf("required upgrade PVC %s is unavailable: %w", name, err)
			}
			if !pvc.DeletionTimestamp.IsZero() || pvc.Spec.VolumeName == "" || pvc.Status.Phase != corev1.ClaimBound {
				return nil, fmt.Errorf("required upgrade PVC %s is deleting or unbound", name)
			}
			if pvc.UID == "" {
				return nil, fmt.Errorf("required upgrade PVC %s has no UID", name)
			}
			claims[name] = string(pvc.UID)
		}
	}

	return claims, nil
}

func (r *ClusterReconciler) verifyUpgradeClaims(ctx context.Context, ledger *ledgerv1alpha1.Cluster, state *ledgerv1alpha1.ClusterUpgradeStatus) error {
	observedVolumes := make(map[string]string)
	for name, uid := range state.ClaimUIDs {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ledger.Namespace}, pvc); err != nil {
			return fmt.Errorf("upgrade PVC %s missing: %w", name, err)
		}
		if string(pvc.UID) != uid || !pvc.DeletionTimestamp.IsZero() || pvc.Spec.VolumeName == "" || pvc.Status.Phase != corev1.ClaimBound {
			return fmt.Errorf("upgrade PVC %s changed identity, is deleting, or is unbound", name)
		}
		observedVolumes[pvc.Spec.VolumeName] = name
	}
	for name, uid := range state.VolumeUIDs {
		claimName, ok := observedVolumes[name]
		if !ok {
			return fmt.Errorf("upgrade PV %s is no longer referenced by an original claim", name)
		}
		pv := &corev1.PersistentVolume{}
		if err := r.Get(ctx, types.NamespacedName{Name: name}, pv); err != nil {
			return fmt.Errorf("upgrade PV %s missing: %w", name, err)
		}
		if string(pv.UID) != uid || !pv.DeletionTimestamp.IsZero() || pv.Status.Phase != corev1.VolumeBound || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Name != claimName || pv.Spec.ClaimRef.Namespace != ledger.Namespace || string(pv.Spec.ClaimRef.UID) != state.ClaimUIDs[claimName] {
			return fmt.Errorf("upgrade PV %s changed identity, is deleting, or is no longer bound to PVC %s", name, claimName)
		}
	}

	return nil
}

func (r *ClusterReconciler) upgradeVolumes(ctx context.Context, ledger *ledgerv1alpha1.Cluster, claims map[string]string) (map[string]string, error) {
	volumes := make(map[string]string)
	for name, uid := range claims {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ledger.Namespace}, pvc); err != nil {
			return nil, err
		}
		pv := &corev1.PersistentVolume{}
		if err := r.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err != nil {
			return nil, fmt.Errorf("required upgrade PV %s is unavailable: %w", pvc.Spec.VolumeName, err)
		}
		if !pv.DeletionTimestamp.IsZero() || pv.Status.Phase != corev1.VolumeBound || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.Name != name || pv.Spec.ClaimRef.Namespace != ledger.Namespace || string(pv.Spec.ClaimRef.UID) != uid {
			return nil, fmt.Errorf("required upgrade PV %s is deleting or bound to a different claim", pv.Name)
		}
		if pv.UID == "" {
			return nil, fmt.Errorf("required upgrade PV %s has no UID", pv.Name)
		}
		volumes[pv.Name] = string(pv.UID)
	}

	return volumes, nil
}

func (r *ClusterReconciler) detachUpgradeClaimOwners(ctx context.Context, ledger *ledgerv1alpha1.Cluster, state *ledgerv1alpha1.ClusterUpgradeStatus) error {
	for name := range state.ClaimUIDs {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ledger.Namespace}, pvc); err != nil {
			return err
		}
		if len(pvc.OwnerReferences) == 0 {
			continue
		}
		for _, owner := range pvc.OwnerReferences {
			if owner.Kind == "StatefulSet" && string(owner.UID) == state.StatefulSetUID {
				continue
			}
			if owner.Kind == "Pod" && hasStatefulSetOrdinal(owner.Name, resourceName(ledger.Name)) {
				continue
			}

			return fmt.Errorf("upgrade PVC %s has unexpected owner %s/%s", name, owner.Kind, owner.Name)
		}
		detachedPVC := pvc.DeepCopy()
		detachedPVC.OwnerReferences = nil
		if err := r.Patch(ctx, detachedPVC, client.MergeFromWithOptions(pvc, client.MergeFromWithOptimisticLock{})); err != nil {
			return fmt.Errorf("detaching owner references from PVC %s: %w", name, err)
		}
	}

	return r.verifyDetachedUpgradeClaims(ctx, ledger, state)
}

func (r *ClusterReconciler) verifyDetachedUpgradeClaims(ctx context.Context, ledger *ledgerv1alpha1.Cluster, state *ledgerv1alpha1.ClusterUpgradeStatus) error {
	if err := r.verifyUpgradeClaims(ctx, ledger, state); err != nil {
		return err
	}
	for name := range state.ClaimUIDs {
		pvc := &corev1.PersistentVolumeClaim{}
		if err := r.Get(ctx, types.NamespacedName{Name: name, Namespace: ledger.Namespace}, pvc); err != nil {
			return err
		}
		if len(pvc.OwnerReferences) != 0 {
			return fmt.Errorf("upgrade PVC %s still has owner references", name)
		}
	}

	return nil
}
