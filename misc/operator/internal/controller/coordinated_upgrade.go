package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	upgradePhaseMaintenance = "Maintenance"
	upgradePhaseBarrier     = "Barrier"
	upgradePhaseStopping    = "Stopping"
	upgradePhaseStarting    = "Starting"
	upgradePhaseComplete    = "Complete"
	upgradePhaseFailed      = "Failed"
)

// reconcileCoordinatedUpgrade owns the explicit, resumable stop-the-world
// protocol. It returns handled=true while the operation is active so the
// normal StatefulSet reconciler cannot publish a rolling template update.
func (r *ClusterReconciler) reconcileCoordinatedUpgrade(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, exec ledgerctlExec) (handled bool, result ctrl.Result, err error) {
	if ledger.Spec.Upgrade == nil || ledger.Spec.Upgrade.Strategy != "Coordinated" || r.Config == nil || r.Clientset == nil || sts == nil || sts.Spec.Replicas == nil {
		return false, ctrl.Result{}, nil
	}
	target := statefulSetImage(ledger)
	current := ""
	if len(sts.Spec.Template.Spec.Containers) > 0 {
		current = sts.Spec.Template.Spec.Containers[0].Image
	}
	status := ledger.Status.Upgrade
	if status == nil && current == target {
		return false, ctrl.Result{}, nil
	}
	if status == nil {
		status = &ledgerv1alpha1.UpgradeStatus{Phase: upgradePhaseMaintenance, TargetImage: target, TargetSpecHash: computeSpecHash(&ledger.Spec), ExpectedReplicas: *sts.Spec.Replicas}
		ledger.Status.Upgrade = status
	}
	if status.TargetImage != "" && status.TargetImage != target && status.Phase != upgradePhaseComplete {
		status.Phase = upgradePhaseFailed
		status.Error = "coordinated upgrade target changed while an upgrade was active; restore the pinned target before retrying"
		return true, ctrl.Result{}, nil
	}
	if status.TargetSpecHash != "" && status.TargetSpecHash != computeSpecHash(&ledger.Spec) && status.Phase != upgradePhaseComplete {
		status.Phase = upgradePhaseFailed
		status.Error = "Cluster spec changed while a coordinated upgrade was active; restore the original spec before retrying"
		return true, ctrl.Result{}, nil
	}
	status.TargetImage = target

	pod0 := podName(ledger.Name, 0)
	serverAddr := podSelfServerAddr(headlessServiceName(ledger.Name), ledger.Spec.GrpcPort)
	switch status.Phase {
	case "", upgradePhaseMaintenance, upgradePhaseFailed:
		if _, err := exec(ctx, r.Config, r.Clientset, ledger.Namespace, pod0, "ledger", ledgerctlCommand(serverAddr, currentTLSModeFromStatefulSet(sts), "cluster", "maintenance", "enable")); err != nil {
			status.Phase, status.Error = upgradePhaseFailed, fmt.Sprintf("enabling maintenance: %v", err)
			return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
		}
		status.Error = ""
		status.Phase = upgradePhaseBarrier
		return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
	case upgradePhaseBarrier:
		barrier := status.BarrierIndex
		if barrier == 0 {
			var err error
			barrier, err = readCommitBarrier(ctx, r.Config, r.Clientset, ledger, sts, currentTLSModeFromStatefulSet(sts), exec)
			if err != nil {
				status.Phase, status.Error = upgradePhaseFailed, err.Error()
				return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
			}
			status.BarrierIndex = barrier
		}
		ready, err := allMembersPersistedThrough(ctx, r.Config, r.Clientset, ledger, sts, currentTLSModeFromStatefulSet(sts), exec, barrier)
		if err != nil {
			status.Phase, status.Error = upgradePhaseFailed, err.Error()
			return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
		}
		if !ready {
			return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
		}
		if err := r.detachStatefulSetPVCOwners(ctx, ledger.Namespace, sts); err != nil {
			status.Phase, status.Error = upgradePhaseFailed, err.Error()
			return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
		}
		if err := r.suspendStatefulSetForUpgrade(ctx, sts); err != nil {
			status.Phase, status.Error = upgradePhaseFailed, err.Error()
			return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
		}
		status.Error = ""
		status.Phase = upgradePhaseStopping
		return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
	case upgradePhaseStopping:
		pods := &corev1.PodList{}
		if err := r.List(ctx, pods, client.InNamespace(ledger.Namespace), client.MatchingLabels(selectorLabels(ledger))); err != nil {
			status.Error = fmt.Sprintf("listing old pods: %v", err)
			return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
		}
		if sts.Status.Replicas != 0 || sts.Status.CurrentReplicas != 0 || sts.Status.ReadyReplicas != 0 || len(pods.Items) != 0 {
			return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
		}
		if err := r.publishTargetStatefulSet(ctx, ledger, sts, target); err != nil {
			status.Phase, status.Error = upgradePhaseFailed, err.Error()
			return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
		}
		status.Phase = upgradePhaseStarting
		return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
	case upgradePhaseStarting:
		if sts.Spec.Replicas == nil || *sts.Spec.Replicas != status.ExpectedReplicas || sts.Status.ReadyReplicas != status.ExpectedReplicas {
			return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
		}
		pods := &corev1.PodList{}
		if err := r.List(ctx, pods, client.InNamespace(ledger.Namespace), client.MatchingLabels(selectorLabels(ledger))); err != nil {
			return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
		}
		for _, pod := range pods.Items {
			if len(pod.Spec.Containers) == 0 || pod.Spec.Containers[0].Image != target || pod.Status.Phase != corev1.PodRunning {
				return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
			}
		}
		if _, err := exec(ctx, r.Config, r.Clientset, ledger.Namespace, pod0, "ledger", ledgerctlCommand(serverAddr, currentTLSModeFromStatefulSet(sts), "cluster", "maintenance", "disable")); err != nil {
			status.Phase, status.Error = upgradePhaseFailed, fmt.Sprintf("disabling maintenance after target verification: %v", err)
			return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
		}
		status.Phase = upgradePhaseComplete
		return true, ctrl.Result{}, nil
	case upgradePhaseComplete:
		if current == target {
			return false, ctrl.Result{}, nil
		}
		status.Phase = upgradePhaseMaintenance
		status.TargetSpecHash = computeSpecHash(&ledger.Spec)
		status.ExpectedReplicas = *sts.Spec.Replicas
		status.BarrierIndex = 0
		return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
	}
	return true, ctrl.Result{RequeueAfter: upgradeRetryInterval}, nil
}

const upgradeRetryInterval = 2 * time.Second

func statefulSetImage(ledger *ledgerv1alpha1.Cluster) string {
	if ledger.Spec.Image.Digest != "" {
		return ledger.Spec.Image.Repository + "@" + ledger.Spec.Image.Digest
	}
	return ledger.Spec.Image.Repository + ":" + ledger.Spec.Image.Tag
}

func (r *ClusterReconciler) suspendStatefulSetForUpgrade(ctx context.Context, sts *appsv1.StatefulSet) error {
	zero := int32(0)
	sts.Spec.Replicas = &zero
	retain := appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	sts.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{WhenScaled: retain, WhenDeleted: retain}
	return r.Update(ctx, sts)
}

// detachStatefulSetPVCOwners removes StatefulSet owner references before the
// zero-replica suspension. This is required even when the CR requested
// whenScaled=Retain: an older controller may have left owner refs from a
// previous Delete policy, and Kubernetes garbage collection would otherwise
// race the suspension and delete a disk before the new policy is observed.
func (r *ClusterReconciler) detachStatefulSetPVCOwners(ctx context.Context, namespace string, sts *appsv1.StatefulSet) error {
	if sts.UID == "" {
		return fmt.Errorf("invariant: StatefulSet UID is required before detaching PVC owners")
	}
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := r.List(ctx, pvcs, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("listing PVCs before coordinated suspension: %w", err)
	}
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		changed := false
		owners := pvc.OwnerReferences[:0]
		for _, owner := range pvc.OwnerReferences {
			if owner.UID == sts.UID && owner.Kind == "StatefulSet" {
				changed = true
				continue
			}
			owners = append(owners, owner)
		}
		if changed {
			pvc.OwnerReferences = owners
			if err := r.Update(ctx, pvc); err != nil {
				return fmt.Errorf("detaching StatefulSet owner from PVC %s: %w", pvc.Name, err)
			}
		}
	}
	return nil
}

func (r *ClusterReconciler) publishTargetStatefulSet(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, image string) error {
	desired := buildStatefulSetSpec(ledger, computeSpecHash(&ledger.Spec), nil, currentTLSModeFromStatefulSet(sts))
	sts.Spec.Template = desired.Template
	sts.Spec.Replicas = ledger.Spec.Replicas
	// Keep Retain through the entire coordinated operation. Switching back to
	// Delete in the same API update that restores replicas would let the
	// StatefulSet controller interpret the earlier zero-replica state as a
	// deletion request on Kubernetes versions that reconcile retention eagerly.
	retain := appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	sts.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{WhenScaled: retain, WhenDeleted: retain}
	return r.Update(ctx, sts)
}

type upgradeStatusJSON struct {
	State      string `json:"state"`
	RaftStatus struct {
		Commit             uint64 `json:"commit"`
		LastPersistedIndex uint64 `json:"lastPersistedIndex"`
	} `json:"raftStatus"`
}

func readUpgradeStatus(ctx context.Context, cfg *rest.Config, cs kubernetes.Interface, ledger *ledgerv1alpha1.Cluster, pod string, nodeID int32, tlsMode string, exec ledgerctlExec) (upgradeStatusJSON, error) {
	statusArgs := []string{"cluster", "status"}
	if nodeID != 0 {
		statusArgs = append(statusArgs, "--node-id", strconv.Itoa(int(nodeID)))
	}
	statusArgs = append(statusArgs, "--json")
	result, err := exec(ctx, cfg, cs, ledger.Namespace, pod, "ledger", ledgerctlCommand(podSelfServerAddr(headlessServiceName(ledger.Name), ledger.Spec.GrpcPort), tlsMode, statusArgs...))
	if err != nil || result == nil {
		return upgradeStatusJSON{}, fmt.Errorf("reading member %d status: %w", nodeID, err)
	}
	var status upgradeStatusJSON
	if err := json.Unmarshal([]byte(result.Stdout), &status); err != nil {
		return status, fmt.Errorf("decoding member %d status: %w", nodeID, err)
	}
	return status, nil
}

func readCommitBarrier(ctx context.Context, cfg *rest.Config, cs kubernetes.Interface, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, tlsMode string, exec ledgerctlExec) (uint64, error) {
	status, err := readUpgradeStatus(ctx, cfg, cs, ledger, podName(ledger.Name, 0), 0, tlsMode, exec)
	if err != nil {
		return 0, err
	}
	if status.State != "Leader" && status.State != "leader" {
		return 0, fmt.Errorf("maintenance barrier was not observed on a leader")
	}
	if status.RaftStatus.Commit == 0 {
		return 0, fmt.Errorf("leader returned an empty commit index")
	}
	return status.RaftStatus.Commit, nil
}

func allMembersPersistedThrough(ctx context.Context, cfg *rest.Config, cs kubernetes.Interface, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, tlsMode string, exec ledgerctlExec, barrier uint64) (bool, error) {
	if sts.Spec.Replicas == nil {
		return false, fmt.Errorf("StatefulSet has no replica count")
	}
	for ordinal := int32(0); ordinal < *sts.Spec.Replicas; ordinal++ {
		status, err := readUpgradeStatus(ctx, cfg, cs, ledger, podName(ledger.Name, int(ordinal)), ordinal+1, tlsMode, exec)
		if err != nil {
			return false, err
		}
		if status.RaftStatus.LastPersistedIndex < barrier {
			return false, nil
		}
	}
	return true, nil
}
