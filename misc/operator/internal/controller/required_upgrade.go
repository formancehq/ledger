package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Masterminds/semver/v3"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// A release listed here cannot coexist with its predecessor. Entries denote
// the first target version requiring a complete stop, including allowed jumps
// that cross the boundary. RC adjacency is still checked by validateRCUpgrade.
var mandatoryStopVersions = []string{"v3.0.0-rc.2"}

const requiredUpgradePoll = 5 * time.Second

func requiresStoppedUpgrade(source, target string) bool {
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
	for _, boundary := range mandatoryStopVersions {
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
func requiresPinnedUpgradeTarget(source, target string) bool {
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
	for _, boundary := range mandatoryStopVersions {
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
		if requiresPinnedUpgradeTarget(source, ledger.Spec.Image.Tag) {
			return false, ctrl.Result{}, fmt.Errorf("cannot verify mandatory stopped upgrade from %s to image tag %q; pin a canonical version tag", source, ledger.Spec.Image.Tag)
		}
		if !requiresStoppedUpgrade(source, ledger.Spec.Image.Tag) {
			return true, ctrl.Result{}, nil
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
		state = &ledgerv1alpha1.ClusterUpgradeStatus{Source: source, Target: ledger.Spec.Image.Tag, StatefulSetUID: string(sts.UID), TargetSpecHash: computeSpecHash(&ledger.Spec), ExpectedReplicas: *sts.Spec.Replicas, ClaimUIDs: claims, VolumeUIDs: volumes, Phase: "Maintenance"}
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
	if err := r.verifyUpgradeClaims(ctx, ledger, state); err != nil {
		return false, ctrl.Result{}, err
	}
	switch state.Phase {
	case "Maintenance":
		if sts == nil {
			return false, ctrl.Result{}, errors.New("source StatefulSet disappeared before maintenance barrier")
		}
		if err := r.execUpgradeCommand(ctx, ledger, sts, exec, "cluster", "maintenance", "enable"); err != nil {
			return false, ctrl.Result{}, fmt.Errorf("enabling maintenance: %w", err)
		}
		if err := r.advanceUpgrade(ctx, ledger, "Barrier"); err != nil {
			return false, ctrl.Result{}, err
		}

		return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
	case "Barrier":
		if sts == nil {
			return false, ctrl.Result{}, errors.New("source StatefulSet disappeared before maintenance barrier")
		}
		if state.BarrierIndex == 0 {
			barrier, err := r.readUpgradeBarrier(ctx, ledger, sts, exec)
			if err != nil {
				return false, ctrl.Result{}, err
			}
			updated := state.DeepCopy()
			updated.BarrierIndex = barrier
			if err := r.persistUpgrade(ctx, ledger, updated); err != nil {
				return false, ctrl.Result{}, err
			}
			state = updated
		}
		ready, err := r.upgradeVotersPersisted(ctx, ledger, sts, exec, state.BarrierIndex)
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
		policy := sts.Spec.PersistentVolumeClaimRetentionPolicy
		if policy == nil || policy.WhenDeleted != appsv1.RetainPersistentVolumeClaimRetentionPolicyType || policy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
			retainedSTS := sts.DeepCopy()
			retainedSTS.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType, WhenScaled: appsv1.RetainPersistentVolumeClaimRetentionPolicyType}
			if err := r.Update(ctx, retainedSTS); err != nil {
				return false, ctrl.Result{}, fmt.Errorf("retaining StatefulSet claims: %w", err)
			}

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
				ready, err := r.upgradeVotersPersisted(ctx, ledger, sts, exec, state.BarrierIndex)
				if err != nil {
					return false, ctrl.Result{}, fmt.Errorf("rechecking maintenance barrier before delete: %w", err)
				}
				if !ready {
					return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
				}
				background := metav1.DeletePropagationBackground
				if err := r.Delete(ctx, sts, &client.DeleteOptions{PropagationPolicy: &background}); err != nil && !apierrors.IsNotFound(err) {
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
		if err := verifyReplacementReplicaCount(sts, state); err != nil {
			return false, ctrl.Result{}, err
		}
		if !replacementConverged(sts, state) {
			return false, ctrl.Result{RequeueAfter: requiredUpgradePoll}, nil
		}
		tag, err := ledgerImageTag(sts)
		if err != nil || tag != state.Target || string(sts.UID) == state.StatefulSetUID {
			return false, ctrl.Result{}, fmt.Errorf("replacement StatefulSet changed before maintenance release: %w (%s)", err, tag)
		}
		if err := r.execUpgradeCommand(ctx, ledger, sts, exec, "cluster", "maintenance", "disable"); err != nil {
			return false, ctrl.Result{}, fmt.Errorf("disabling maintenance: %w", err)
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

func (r *ClusterReconciler) resumeMissingUpgradeReplacement(ctx context.Context, ledger *ledgerv1alpha1.Cluster) (bool, ctrl.Result, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(ledger.Namespace)); err != nil {
		return false, ctrl.Result{}, fmt.Errorf("listing Pods after replacement StatefulSet disappeared: %w", err)
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

type stoppedUpgradeClusterStatus struct {
	State           string `json:"state"`
	MaintenanceMode bool   `json:"maintenanceMode"`
	SyncProgress    *struct {
		Status string `json:"status"`
	} `json:"syncProgress"`
	RaftStatus struct {
		Commit             uint64 `json:"commit,string"`
		LastPersistedIndex uint64 `json:"lastPersistedIndex,string"`
	} `json:"raftStatus"`
}

func (r *ClusterReconciler) execUpgradeCommand(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, exec ledgerctlExec, args ...string) error {
	_, err := r.execUpgrade(ctx, ledger, sts, exec, 0, args...)

	return err
}

func (r *ClusterReconciler) execUpgrade(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, exec ledgerctlExec, nodeID int32, args ...string) (*execResult, error) {
	if r.Config == nil || r.Clientset == nil {
		return nil, errors.New("kubernetes exec client is unavailable for stopped upgrade")
	}
	grpcPort := int32(0)
	for _, container := range sts.Spec.Template.Spec.Containers {
		if container.Name != "ledger" {
			continue
		}
		for _, port := range container.Ports {
			if port.Name == "grpc" {
				grpcPort = port.ContainerPort

				break
			}
		}
	}
	if grpcPort <= 0 {
		return nil, errors.New("live StatefulSet has no ledger gRPC port")
	}
	serverAddr := podSelfServerAddr(headlessServiceName(ledger.Name), grpcPort)
	result, err := exec(ctx, r.Config, r.Clientset, ledger.Namespace, podName(ledger.Name, 0), "ledger", ledgerctlCommand(serverAddr, currentTLSModeFromStatefulSet(sts), args...))
	if err != nil {
		return nil, err
	}
	if result == nil {
		return nil, fmt.Errorf("ledgerctl returned no result for node %d", nodeID)
	}

	return result, nil
}

func (r *ClusterReconciler) readUpgradeStatus(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, exec ledgerctlExec, nodeID int32) (stoppedUpgradeClusterStatus, error) {
	args := []string{"cluster", "status"}
	if nodeID != 0 {
		args = append(args, "--node-id", strconv.Itoa(int(nodeID)))
	}
	args = append(args, "--json")
	result, err := r.execUpgrade(ctx, ledger, sts, exec, nodeID, args...)
	if err != nil {
		return stoppedUpgradeClusterStatus{}, fmt.Errorf("reading upgrade status for node %d: %w", nodeID, err)
	}
	var status stoppedUpgradeClusterStatus
	if err := json.Unmarshal([]byte(result.Stdout), &status); err != nil {
		return status, fmt.Errorf("decoding upgrade status for node %d: %w", nodeID, err)
	}

	return status, nil
}

func (r *ClusterReconciler) readUpgradeBarrier(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, exec ledgerctlExec) (uint64, error) {
	status, err := r.readUpgradeStatus(ctx, ledger, sts, exec, 0)
	if err != nil {
		return 0, err
	}
	if !strings.EqualFold(status.State, "leader") || !status.MaintenanceMode || status.RaftStatus.Commit == 0 {
		return 0, fmt.Errorf("maintenance barrier is not visible on the leader (state %q, maintenance %t, commit %d)", status.State, status.MaintenanceMode, status.RaftStatus.Commit)
	}

	return status.RaftStatus.Commit, nil
}

func (r *ClusterReconciler) upgradeVotersPersisted(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, exec ledgerctlExec, barrier uint64) (bool, error) {
	if sts.Spec.Replicas == nil {
		return false, errors.New("source StatefulSet replica count is unknown")
	}
	for ordinal := range *sts.Spec.Replicas {
		status, err := r.readUpgradeStatus(ctx, ledger, sts, exec, ordinal+1)
		if err != nil {
			return false, err
		}
		if !status.MaintenanceMode || status.RaftStatus.LastPersistedIndex < barrier || (status.SyncProgress != nil && !strings.EqualFold(status.SyncProgress.Status, "normal")) {
			return false, nil
		}
	}

	return true, nil
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
