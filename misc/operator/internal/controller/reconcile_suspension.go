package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	suspensionPhaseQuiescing = "Quiescing"
	suspensionPhaseBarrier   = "Barrier"
	suspensionPhaseStopping  = "Stopping"
	suspensionPhaseSuspended = "Suspended"
	suspensionPhaseStarting  = "Starting"
	suspensionPhaseVerifying = "Verifying"
	suspensionPhaseOpening   = "Opening"
	suspensionPhaseRunning   = "Running"
	conditionSuspended       = "Suspended"
	suspensionRetryInterval  = 2 * time.Second
)

// reconcileSuspension never calls raftScaleDown: replicas=0 is only a workload
// effect, while Cluster.spec.replicas and the committed voter set stay intact.
// Each external effect has a persisted preceding phase so a restarted operator
// can retry it against the observed StatefulSet and Pod state.
func (r *ClusterReconciler) reconcileSuspension(ctx context.Context, ledger *ledgerv1alpha1.Cluster, exec ledgerctlExec) (ctrl.Result, error) {
	status := ledger.Status.Suspension
	if status != nil && status.Phase == suspensionPhaseRunning && !ledger.Spec.Disabled {
		return ctrl.Result{}, nil
	}
	retry := ctrl.Result{RequeueAfter: suspensionRetryInterval}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	sts := &appsv1.StatefulSet{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: ledger.Namespace, Name: resourceName(ledger.Name)}, sts); err != nil {
		r.suspensionBlocked(ledger, fmt.Sprintf("reading StatefulSet: %v", err))

		return retry, nil
	}
	if sts.Spec.Replicas == nil || r.Config == nil || r.Clientset == nil {
		r.suspensionBlocked(ledger, "StatefulSet replicas and pod execution are required")

		return retry, nil
	}
	if status == nil || status.Phase == suspensionPhaseRunning {
		if err := suspensionPreflight(ledger, sts); err != nil {
			r.suspensionBlocked(ledger, err.Error())

			return retry, nil
		}
		if err := r.suspensionInFlightBackups(ctx, ledger); err != nil {
			r.suspensionBlocked(ledger, err.Error())

			return retry, nil
		}
		pvcUIDs, pvUIDs, err := r.suspensionVolumeIdentities(ctx, ledger, sts, *sts.Spec.Replicas, nil)
		if err != nil {
			r.suspensionBlocked(ledger, err.Error())

			return retry, nil
		}
		observed, err := readSuspensionLeaderStatus(ctx, r, ledger, currentTLSModeFromStatefulSet(sts), exec, *sts.Spec.Replicas)
		if err != nil || !suspensionMembershipComplete(observed, *sts.Spec.Replicas) {
			r.suspensionBlocked(ledger, fmt.Sprintf("cannot prove healthy Raft membership before suspension: %v", err))

			return retry, nil
		}
		status = &ledgerv1alpha1.SuspensionStatus{
			Phase:                        suspensionPhaseQuiescing,
			ExpectedReplicas:             *sts.Spec.Replicas,
			PinnedSpecHash:               computeSpecHash(&ledger.Spec),
			PinnedImage:                  suspensionImage(ledger),
			StatefulSetUID:               string(sts.UID),
			PVCUIDs:                      pvcUIDs,
			PVUIDs:                       pvUIDs,
			MaintenancePreviouslyEnabled: observed.MaintenanceMode,
		}
		ledger.Status.Suspension = status
		if err := r.Status().Update(ctx, ledger); err != nil {
			return ctrl.Result{}, fmt.Errorf("persisting suspension intent: %w", err)
		}
	}
	if string(sts.UID) != status.StatefulSetUID {
		r.suspensionBlocked(ledger, "original StatefulSet UID changed during suspension")

		return retry, nil
	}
	if computeSpecHash(&ledger.Spec) != status.PinnedSpecHash ||
		ledger.Spec.Replicas == nil || *ledger.Spec.Replicas != status.ExpectedReplicas ||
		suspensionImage(ledger) != status.PinnedImage {
		r.suspensionBlocked(ledger, "Cluster identity or workload spec changed during suspension; restore the pinned spec before resuming")

		return retry, nil
	}

	status.Error = ""
	tlsMode := currentTLSModeFromStatefulSet(sts)
	pod0 := podName(ledger.Name, 0)
	serverAddr := podSelfServerAddr(headlessServiceName(ledger.Name), ledger.Spec.GrpcPort)
	switch status.Phase {
	case suspensionPhaseQuiescing:
		if *sts.Spec.Replicas != status.ExpectedReplicas {
			r.suspensionBlocked(ledger, "StatefulSet replica count changed before the maintenance barrier")

			return retry, nil
		}
		observed, err := readSuspensionLeaderStatus(ctx, r, ledger, tlsMode, exec, status.ExpectedReplicas)
		if err != nil || !suspensionMembershipComplete(observed, status.ExpectedReplicas) {
			r.suspensionBlocked(ledger, fmt.Sprintf("checking maintenance outcome: %v", err))

			return retry, nil
		}
		if !observed.MaintenanceMode {
			if _, err := exec(ctx, r.Config, r.Clientset, ledger.Namespace, pod0, "ledger", ledgerctlCommand(serverAddr, tlsMode, "cluster", "maintenance", "enable")); err != nil {
				r.suspensionBlocked(ledger, fmt.Sprintf("enabling replicated maintenance: %v", err))

				return retry, nil
			}
		}
		status.Phase = suspensionPhaseBarrier
	case suspensionPhaseBarrier:
		if status.BarrierIndex == 0 {
			leader, err := readSuspensionLeaderStatus(ctx, r, ledger, tlsMode, exec, status.ExpectedReplicas)
			if err != nil {
				r.suspensionBlocked(ledger, err.Error())

				return retry, nil
			}
			if leader.RaftStatus.Commit == 0 {
				r.suspensionBlocked(ledger, "leader returned an empty maintenance commit index")

				return retry, nil
			}
			status.BarrierIndex = leader.RaftStatus.Commit
		}
		ready, err := suspensionMembersPersisted(ctx, r, ledger, sts, tlsMode, exec, status)
		if err != nil {
			r.suspensionBlocked(ledger, err.Error())

			return retry, nil
		}
		if !ready {
			break
		}
		if !ledger.Spec.Disabled {
			// A disable canceled before scale-to-zero still has to prove the
			// replicated maintenance entry durable on every original voter.
			// The workload is already running, so resume verification directly.
			status.Phase = suspensionPhaseVerifying

			break
		}
		status.Phase = suspensionPhaseStopping
		if err := r.Status().Update(ctx, ledger); err != nil {
			return ctrl.Result{}, fmt.Errorf("persisting verified suspension barrier: %w", err)
		}
	case suspensionPhaseStopping:
		if *sts.Spec.Replicas != 0 {
			if !ledger.Spec.Disabled && *sts.Spec.Replicas == status.ExpectedReplicas {
				// The stop checkpoint is durable, but no workload effect has
				// happened yet. Keep the voters running on early cancellation.
				status.Phase = suspensionPhaseVerifying

				break
			}
			if err := suspensionTemplatePinned(sts, status); err != nil {
				r.suspensionBlocked(ledger, err.Error())

				return retry, nil
			}
			if err := r.suspensionInFlightBackups(ctx, ledger); err != nil {
				r.suspensionBlocked(ledger, err.Error())

				return retry, nil
			}
			if *sts.Spec.Replicas != status.ExpectedReplicas {
				r.suspensionBlocked(ledger, "StatefulSet replica count changed during suspension")

				return retry, nil
			}
			ready, err := suspensionMembersPersisted(ctx, r, ledger, sts, tlsMode, exec, status)
			if err != nil || !ready {
				r.suspensionBlocked(ledger, fmt.Sprintf("maintenance barrier no longer verified: %v", err))

				return retry, nil
			}
			if _, _, err := r.suspensionVolumeIdentities(ctx, ledger, sts, status.ExpectedReplicas, status); err != nil {
				r.suspensionBlocked(ledger, err.Error())

				return retry, nil
			}
			if err := r.detachSuspensionPVCOwners(ctx, ledger.Namespace, sts); err != nil {
				r.suspensionBlocked(ledger, err.Error())

				return retry, nil
			}
			if err := r.suspendStatefulSetForSuspension(ctx, sts); err != nil {
				r.suspensionBlocked(ledger, fmt.Sprintf("stopping StatefulSet: %v", err))

				return retry, nil
			}

			break
		}
		pods := &corev1.PodList{}
		if err := reader.List(ctx, pods, client.InNamespace(ledger.Namespace), client.MatchingLabels(selectorLabels(ledger))); err != nil {
			r.suspensionBlocked(ledger, fmt.Sprintf("listing terminating Pods: %v", err))

			return retry, nil
		}
		if len(pods.Items) == 0 && sts.Status.Replicas == 0 && sts.Status.ReadyReplicas == 0 {
			status.Phase = suspensionPhaseSuspended
		}
	case suspensionPhaseSuspended:
		if *sts.Spec.Replicas != 0 {
			r.suspensionBlocked(ledger, "suspended StatefulSet unexpectedly has replicas")

			return retry, nil
		}
		if !ledger.Spec.Disabled {
			status.Phase = suspensionPhaseStarting
			if err := r.Status().Update(ctx, ledger); err != nil {
				return ctrl.Result{}, fmt.Errorf("persisting resume intent: %w", err)
			}
		}
	case suspensionPhaseStarting:
		if err := suspensionTemplatePinned(sts, status); err != nil {
			r.suspensionBlocked(ledger, err.Error())

			return retry, nil
		}
		if _, _, err := r.suspensionVolumeIdentities(ctx, ledger, sts, status.ExpectedReplicas, status); err != nil {
			r.suspensionBlocked(ledger, err.Error())

			return retry, nil
		}
		// The old Pods load keys at boot. Publish current Credentials into
		// their retained mount before any Pod can restart on the pinned template.
		keys, pending, err := r.reconcileAuthKeysProjection(ctx, ledger, true)
		if err != nil {
			r.suspensionBlocked(ledger, fmt.Sprintf("refreshing auth keys before restart: %v", err))

			return retry, nil
		}
		if pending.pending() {
			r.suspensionBlocked(ledger, "auth keys cannot be safely projected before restart: "+pending.conditionMessage())

			return retry, nil
		}
		hasIssuer := ledger.Spec.Auth != nil && (ledger.Spec.Auth.Issuer != "" || len(ledger.Spec.Auth.Issuers) > 0)
		if ledger.Spec.Auth != nil && ledger.Spec.Auth.Enabled != nil && *ledger.Spec.Auth.Enabled && !hasIssuer && len(keys) == 0 {
			r.suspensionBlocked(ledger, "auth-enabled cluster has no resolved keys or OIDC issuer for restart")

			return retry, nil
		}
		if len(keys) == 0 {
			changed := false
			for i := range sts.Spec.Template.Spec.Containers {
				if sts.Spec.Template.Spec.Containers[i].Name != "ledger" {
					continue
				}
				envs := sts.Spec.Template.Spec.Containers[i].Env[:0]
				for _, env := range sts.Spec.Template.Spec.Containers[i].Env {
					if env.Name == "AUTH_ED25519_KEYS" {
						changed = true

						continue
					}
					envs = append(envs, env)
				}
				sts.Spec.Template.Spec.Containers[i].Env = envs
			}
			if changed {
				// The retained empty ConfigMap remains mounted, but boot must
				// not ask LoadEd25519KeySet to parse its empty key set.
				if err := r.Update(ctx, sts); err != nil {
					r.suspensionBlocked(ledger, fmt.Sprintf("dropping empty static-key wiring before restart: %v", err))

					return retry, nil
				}

				return retry, nil
			}
		}
		if *sts.Spec.Replicas == 0 {
			replicas := status.ExpectedReplicas
			sts.Spec.Replicas = &replicas
			if err := r.Update(ctx, sts); err != nil {
				r.suspensionBlocked(ledger, fmt.Sprintf("starting original StatefulSet: %v", err))

				return retry, nil
			}
		}
		status.Phase = suspensionPhaseVerifying
	case suspensionPhaseVerifying:
		if err := suspensionTemplatePinned(sts, status); err != nil {
			r.suspensionBlocked(ledger, err.Error())

			return retry, nil
		}
		if *sts.Spec.Replicas != status.ExpectedReplicas || sts.Status.ReadyReplicas != status.ExpectedReplicas || !rolloutConverged(sts) {
			break
		}
		ready, maintenanceEnabled, err := suspensionResumedMembers(ctx, r, ledger, tlsMode, exec, status)
		if err != nil {
			r.suspensionBlocked(ledger, fmt.Sprintf("cluster not healthy after restart: %v", err))

			return retry, nil
		}
		if !ready {
			break
		}
		if status.MaintenancePreviouslyEnabled && !maintenanceEnabled {
			r.suspensionBlocked(ledger, "pre-existing maintenance was unexpectedly cleared")

			return retry, nil
		}
		if ledger.Spec.Disabled {
			if !maintenanceEnabled {
				status.Phase = suspensionPhaseQuiescing

				break
			}
			status.Phase = suspensionPhaseBarrier
			status.BarrierIndex = 0

			break
		}
		if !status.MaintenancePreviouslyEnabled && maintenanceEnabled {
			if _, err := exec(ctx, r.Config, r.Clientset, ledger.Namespace, pod0, "ledger", ledgerctlCommand(serverAddr, tlsMode, "cluster", "maintenance", "disable")); err != nil {
				r.suspensionBlocked(ledger, fmt.Sprintf("opening write admission: %v", err))

				return retry, nil
			}
			status.Phase = suspensionPhaseOpening

			break
		}
		status.Phase = suspensionPhaseRunning
	case suspensionPhaseOpening:
		if ledger.Spec.Disabled {
			status.Phase = suspensionPhaseQuiescing

			break
		}
		ready, maintenanceEnabled, err := suspensionResumedMembers(ctx, r, ledger, tlsMode, exec, status)
		if err != nil {
			r.suspensionBlocked(ledger, fmt.Sprintf("verifying write admission after resume: %v", err))

			return retry, nil
		}
		if ready && !maintenanceEnabled {
			status.Phase = suspensionPhaseRunning
		}
	case suspensionPhaseRunning:
		// A new suspend request starts a new operation on the next reconcile.
	default:
		return ctrl.Result{}, fmt.Errorf("unknown suspension phase %q", status.Phase)
	}
	r.setSuspensionPhase(ledger)

	return retry, nil
}

func suspensionPreflight(ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet) error {
	if hasHostPathVolume(ledger) {
		return errors.New("reversible suspension requires PVC storage; hostPath is unsupported")
	}
	if retention := ledger.Spec.Persistence.RetentionPolicy; retention != nil && (retention.WhenScaled == "Delete" || retention.WhenDeleted == "Delete") {
		return errors.New("reversible suspension requires PVC retention on scale and deletion")
	}
	if ledger.Status.Upgrade != nil || ledger.Status.Drain != nil || ledger.Status.ClaimTemplateRecovery != nil ||
		currentTLSModeFromStatefulSet(sts) != desiredTLSMode(ledger) {
		return errors.New("workload upgrade, drain, storage recovery or TLS rollout must finish before suspension")
	}
	if sts.Spec.Replicas == nil || *sts.Spec.Replicas < 1 || sts.Status.ReadyReplicas != *sts.Spec.Replicas || !rolloutConverged(sts) {
		return errors.New("all existing members must be ready before suspension")
	}
	if ledger.Spec.Replicas != nil && *ledger.Spec.Replicas != *sts.Spec.Replicas {
		return errors.New("replica change must finish before suspension")
	}
	if len(sts.Spec.Template.Spec.Containers) == 0 || sts.Spec.Template.Spec.Containers[0].Image != suspensionImage(ledger) {
		return errors.New("image change must finish before suspension")
	}
	if sts.Spec.Template.Annotations[annotationSpecHash] != computeSpecHash(&ledger.Spec) {
		return errors.New("pod template update must finish before suspension")
	}

	return nil
}

func suspensionTemplatePinned(sts *appsv1.StatefulSet, status *ledgerv1alpha1.SuspensionStatus) error {
	if len(sts.Spec.Template.Spec.Containers) == 0 || sts.Spec.Template.Spec.Containers[0].Image != status.PinnedImage ||
		sts.Spec.Template.Annotations[annotationSpecHash] != status.PinnedSpecHash {
		return errors.New("original StatefulSet template changed during suspension")
	}

	return nil
}

func suspensionImage(ledger *ledgerv1alpha1.Cluster) string {
	return fmt.Sprintf("%s:%s", ledger.Spec.Image.Repository, ledger.Spec.Image.Tag)
}

func suspensionActive(cluster *ledgerv1alpha1.Cluster) bool {
	return cluster.Spec.Disabled || cluster.Status.Suspension != nil && cluster.Status.Suspension.Phase != suspensionPhaseRunning
}

// Existing jobs may finish before the Pods stop. Pending runs are deferred by
// their controller, so only a reserved Running run holds the stop barrier.
func (r *ClusterReconciler) suspensionInFlightBackups(ctx context.Context, cluster *ledgerv1alpha1.Cluster) error {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	liveCluster := &ledgerv1alpha1.Cluster{}
	if err := reader.Get(ctx, types.NamespacedName{Namespace: cluster.Namespace, Name: cluster.Name}, liveCluster); err != nil {
		return fmt.Errorf("reading Cluster backup admission fence: %w", err)
	}
	backups := &ledgerv1alpha1.BackupList{}
	if err := reader.List(ctx, backups, client.InNamespace(cluster.Namespace)); err != nil {
		return fmt.Errorf("listing Backups before suspension: %w", err)
	}
	owned := make(map[string]bool)
	for _, backup := range backups.Items {
		if backup.Spec.ClusterRef == cluster.Name {
			owned[backup.Name] = true
		}
	}
	runs := &ledgerv1alpha1.BackupRunList{}
	if err := reader.List(ctx, runs, client.InNamespace(cluster.Namespace)); err != nil {
		return fmt.Errorf("listing BackupRuns before suspension: %w", err)
	}
	for _, run := range runs.Items {
		if owned[run.Spec.BackupRef] && run.Status.Phase == ledgerv1alpha1.BackupRunPhaseRunning {
			return fmt.Errorf("BackupRun %s is still running", run.Name)
		}
		if uid, exists := liveCluster.Status.BackupReservations[run.Name]; exists && uid == string(run.UID) && !run.IsTerminal() {
			return fmt.Errorf("BackupRun %s has reserved Cluster admission", run.Name)
		}
	}
	// A reservation for a deleted or terminal run is inert: it cannot create a
	// new Job. The run controller removes it on its terminal reconciliation.

	return nil
}

func suspensionMembershipComplete(status suspensionStatusJSON, replicas int32) bool {
	if status.Leader == 0 || len(status.Nodes) != int(replicas) {
		return false
	}
	seen := make(map[uint32]bool, replicas)
	for _, node := range status.Nodes {
		if node.ID < 1 || node.ID > uint32(replicas) || seen[node.ID] || !strings.EqualFold(node.Suffrage, "voter") {
			return false
		}
		seen[node.ID] = true
	}

	return true
}

type suspensionStatusJSON struct {
	State           string `json:"state"`
	MaintenanceMode bool   `json:"maintenanceMode"`
	Leader          uint32 `json:"leader"`
	Nodes           []struct {
		ID       uint32 `json:"id"`
		Suffrage string `json:"suffrage"`
	} `json:"nodes"`
	RaftStatus struct {
		Commit             uint64 `json:"commit"`
		LastPersistedIndex uint64 `json:"lastPersistedIndex"`
	} `json:"raftStatus"`
}

func (r *ClusterReconciler) readSuspensionStatus(ctx context.Context, ledger *ledgerv1alpha1.Cluster, pod string, nodeID int32, tlsMode string, exec ledgerctlExec) (suspensionStatusJSON, error) {
	args := []string{"cluster", "status"}
	if nodeID != 0 {
		args = append(args, "--node-id", strconv.Itoa(int(nodeID)))
	}
	args = append(args, "--json")
	result, err := exec(ctx, r.Config, r.Clientset, ledger.Namespace, pod, "ledger", ledgerctlCommand(podSelfServerAddr(headlessServiceName(ledger.Name), ledger.Spec.GrpcPort), tlsMode, args...))
	if err != nil || result == nil {
		return suspensionStatusJSON{}, fmt.Errorf("reading member %d status: %w", nodeID, err)
	}
	var status suspensionStatusJSON
	if err := json.Unmarshal([]byte(result.Stdout), &status); err != nil {
		return status, fmt.Errorf("decoding member %d status: %w", nodeID, err)
	}

	return status, nil
}

func (r *ClusterReconciler) suspendStatefulSetForSuspension(ctx context.Context, sts *appsv1.StatefulSet) error {
	zero := int32(0)
	sts.Spec.Replicas = &zero
	retain := appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	sts.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{WhenScaled: retain, WhenDeleted: retain}

	return r.Update(ctx, sts)
}

// detachSuspensionPVCOwners removes stale StatefulSet owner references before
// zero replicas so an earlier Delete retention policy cannot win a GC race.
func (r *ClusterReconciler) detachSuspensionPVCOwners(ctx context.Context, namespace string, sts *appsv1.StatefulSet) error {
	if sts.UID == "" {
		return errors.New("invariant: StatefulSet UID is required before detaching PVC owners")
	}
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := reader.List(ctx, pvcs, client.InNamespace(namespace)); err != nil {
		return fmt.Errorf("listing PVCs before suspension: %w", err)
	}
	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		owners := pvc.OwnerReferences[:0]
		changed := false
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

func readSuspensionLeaderStatus(ctx context.Context, r *ClusterReconciler, ledger *ledgerv1alpha1.Cluster, tlsMode string, exec ledgerctlExec, replicas int32) (suspensionStatusJSON, error) {
	first, err := r.readSuspensionStatus(ctx, ledger, podName(ledger.Name, 0), 1, tlsMode, exec)
	if err != nil {
		return suspensionStatusJSON{}, err
	}
	if first.Leader < 1 || first.Leader > uint32(replicas) {
		return suspensionStatusJSON{}, errors.New("current Raft leader is outside the original voter set")
	}
	leader, err := r.readSuspensionStatus(ctx, ledger, podName(ledger.Name, int(first.Leader-1)), int32(first.Leader), tlsMode, exec)
	if err != nil {
		return suspensionStatusJSON{}, err
	}
	if !strings.EqualFold(leader.State, "leader") || leader.Leader != first.Leader || !suspensionMembershipComplete(leader, replicas) {
		return suspensionStatusJSON{}, errors.New("current leader cannot prove the original complete voter set")
	}

	return leader, nil
}

func suspensionMembersPersisted(ctx context.Context, r *ClusterReconciler, ledger *ledgerv1alpha1.Cluster, _ *appsv1.StatefulSet, tlsMode string, exec ledgerctlExec, status *ledgerv1alpha1.SuspensionStatus) (bool, error) {
	if status.BarrierIndex == 0 {
		return false, errors.New("maintenance barrier was not recorded")
	}
	for ordinal := range status.ExpectedReplicas {
		observed, err := r.readSuspensionStatus(ctx, ledger, podName(ledger.Name, int(ordinal)), ordinal+1, tlsMode, exec)
		if err != nil {
			return false, err
		}
		if !observed.MaintenanceMode {
			return false, fmt.Errorf("member %d has no maintenance gate", ordinal+1)
		}
		if observed.RaftStatus.LastPersistedIndex < status.BarrierIndex {
			return false, nil
		}
	}
	if _, err := readSuspensionLeaderStatus(ctx, r, ledger, tlsMode, exec, status.ExpectedReplicas); err != nil {
		return false, err
	}

	return true, nil
}

func suspensionResumedMembers(ctx context.Context, r *ClusterReconciler, ledger *ledgerv1alpha1.Cluster, tlsMode string, exec ledgerctlExec, status *ledgerv1alpha1.SuspensionStatus) (bool, bool, error) {
	if status.BarrierIndex == 0 || status.ExpectedReplicas < 1 {
		return false, false, errors.New("original maintenance barrier is missing")
	}
	var maintenance *bool
	for ordinal := range status.ExpectedReplicas {
		observed, err := r.readSuspensionStatus(ctx, ledger, podName(ledger.Name, int(ordinal)), ordinal+1, tlsMode, exec)
		if err != nil {
			return false, false, err
		}
		if observed.RaftStatus.LastPersistedIndex < status.BarrierIndex {
			return false, false, nil
		}
		if maintenance != nil && *maintenance != observed.MaintenanceMode {
			return false, false, nil
		}
		value := observed.MaintenanceMode
		maintenance = &value
	}
	if _, err := readSuspensionLeaderStatus(ctx, r, ledger, tlsMode, exec, status.ExpectedReplicas); err != nil {
		return false, false, err
	}

	return true, *maintenance, nil
}

func (r *ClusterReconciler) suspensionVolumeIdentities(ctx context.Context, ledger *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, replicas int32, expected *ledgerv1alpha1.SuspensionStatus) (map[string]string, map[string]string, error) {
	reader := r.APIReader
	if reader == nil {
		reader = r.Client
	}
	// The CR may have been edited while suspended; the live StatefulSet claim
	// templates remain the authority for the original disks.
	if len(sts.Spec.VolumeClaimTemplates) != 2 {
		return nil, nil, errors.New("reversible suspension requires the original WAL and data PVC templates")
	}
	pvcUIDs := make(map[string]string, replicas*2)
	pvUIDs := make(map[string]string, replicas*2)
	for _, claim := range sts.Spec.VolumeClaimTemplates {
		volume := claim.Name
		for ordinal := range replicas {
			name := fmt.Sprintf("%s-%s-%d", volume, resourceName(ledger.Name), ordinal)
			pvc := &corev1.PersistentVolumeClaim{}
			if err := reader.Get(ctx, types.NamespacedName{Namespace: ledger.Namespace, Name: name}, pvc); err != nil {
				return nil, nil, fmt.Errorf("required retained PVC %s: %w", name, err)
			}
			if pvc.DeletionTimestamp != nil || pvc.Status.Phase != corev1.ClaimBound || pvc.Spec.VolumeName == "" {
				return nil, nil, fmt.Errorf("required retained PVC %s is not bound and stable", name)
			}
			pv := &corev1.PersistentVolume{}
			if err := reader.Get(ctx, types.NamespacedName{Name: pvc.Spec.VolumeName}, pv); err != nil {
				return nil, nil, fmt.Errorf("required retained PV %s: %w", pvc.Spec.VolumeName, err)
			}
			if pv.DeletionTimestamp != nil || pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pvc.UID {
				return nil, nil, fmt.Errorf("required retained PV %s no longer binds PVC %s", pv.Name, name)
			}
			pvcUIDs[name] = string(pvc.UID)
			pvUIDs[name] = string(pv.UID)
			if expected != nil && (expected.PVCUIDs[name] != string(pvc.UID) || expected.PVUIDs[name] != string(pv.UID)) {
				return nil, nil, fmt.Errorf("volume identity changed for PVC %s", name)
			}
		}
	}

	return pvcUIDs, pvUIDs, nil
}

func (r *ClusterReconciler) suspensionBlocked(ledger *ledgerv1alpha1.Cluster, message string) {
	if ledger.Status.Suspension != nil {
		ledger.Status.Suspension.Error = message
	}
	ledger.Status.Phase = "SuspensionBlocked"
	meta.SetStatusCondition(&ledger.Status.Conditions, metav1.Condition{
		Type: conditionSuspended, Status: metav1.ConditionFalse, Reason: "Blocked", Message: message,
		ObservedGeneration: ledger.Generation,
	})
}

func (r *ClusterReconciler) setSuspensionPhase(ledger *ledgerv1alpha1.Cluster) {
	status := ledger.Status.Suspension
	if status == nil {
		return
	}
	status.Error = ""
	ledger.Status.Phase = status.Phase
	if status.Phase == suspensionPhaseBarrier {
		ledger.Status.Phase = suspensionPhaseQuiescing
	}
	condition := metav1.Condition{
		Type: conditionSuspended, Status: metav1.ConditionFalse, Reason: status.Phase,
		Message: "reversible suspension transition in progress", ObservedGeneration: ledger.Generation,
	}
	if status.Phase == suspensionPhaseSuspended {
		condition.Status = metav1.ConditionTrue
		condition.Message = "all Ledger Pods are stopped; Raft membership and PVCs are retained"
	}
	meta.SetStatusCondition(&ledger.Status.Conditions, condition)
}
