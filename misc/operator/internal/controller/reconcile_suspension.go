package controller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

const (
	annotationSuspendedReplicas = "ledger.formance.com/suspended-replicas"
	annotationSuspendedJob      = "ledger.formance.com/cluster-suspended"
	suspensionRequeueInterval   = 5 * time.Second
)

// clusterWorkloadsPaused also covers recovery of the old topology, before
// dependent controllers may contact Ledger or provision another backup Job.
func clusterWorkloadsPaused(ctx context.Context, c client.Client, cluster *ledgerv1alpha1.Cluster) (bool, error) {
	if cluster.Spec.Disabled {
		return true, nil
	}
	sts := &appsv1.StatefulSet{}
	if err := c.Get(ctx, types.NamespacedName{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}, sts); err != nil {
		return false, client.IgnoreNotFound(err)
	}

	return sts.Annotations[annotationSuspendedReplicas] != "", nil
}

// reconcileSuspension never calls the normal scale-down path: membership and
// per-ordinal WAL/data are required intact for a cold restart. Its recovery
// cursor is committed on the StatefulSet before replicas can reach zero.
func (r *ClusterReconciler) reconcileSuspension(ctx context.Context, cluster *ledgerv1alpha1.Cluster) (bool, ctrl.Result, error) {
	retry := ctrl.Result{RequeueAfter: suspensionRequeueInterval}
	sts, err := r.fetchExistingStatefulSet(ctx, cluster)
	if err != nil {
		return true, retry, err
	}
	saved := ""
	if sts != nil {
		saved = sts.Annotations[annotationSuspendedReplicas]
	}
	if !cluster.Spec.Disabled && saved == "" {
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: "Suspended", Status: metav1.ConditionFalse, Reason: "Enabled", ObservedGeneration: cluster.Generation})

		return false, ctrl.Result{}, nil
	}
	cluster.Status.Phase = "Suspending"
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: "Suspended", Status: metav1.ConditionFalse, Reason: "StoppingWorkloads", ObservedGeneration: cluster.Generation})
	if sts != nil && !metav1.IsControlledBy(sts, cluster) {
		return true, retry, fmt.Errorf("refusing to suspend foreign StatefulSet %s", sts.Name)
	}
	if !cluster.Spec.Disabled {
		cluster.Status.Phase = "Resuming"
		meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: "Suspended", Status: metav1.ConditionFalse, Reason: "RestoringTopology", ObservedGeneration: cluster.Generation})
		count, err := strconv.ParseInt(saved, 10, 32)
		if err != nil || count < 1 {
			return true, retry, fmt.Errorf("invalid suspended replica cursor %q", saved)
		}
		// Repair authorization before waking Pods. Otherwise an absent auth
		// ConfigMap deadlocks recovery, or revoked keys/scopes become active
		// under the old template until every ordinal is ready.
		credentials, pending, err := r.reconcileAuthKeys(ctx, cluster)
		if err != nil {
			return true, retry, err
		}
		if shouldInjectClusterSecret(currentTLSModeFromStatefulSet(sts)) {
			if err := r.reconcileClusterSecret(ctx, cluster); err != nil {
				return true, retry, err
			}
		}
		if pending.pending() {
			meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: "AuthKeysPending", Status: metav1.ConditionTrue, Reason: string(pending), Message: pending.conditionMessage(), ObservedGeneration: cluster.Generation})

			return true, retry, nil
		}
		meta.RemoveStatusCondition(&cluster.Status.Conditions, "AuthKeysPending")
		template := resumedAuthorizationTemplate(cluster, sts, credentials)
		// Finalize writes RESTORED, which restore-mode boot rejects. Allow the
		// explicit restore -> normal handoff before waiting for old readiness.
		if !cluster.Spec.Restore {
			restoreSpec := cluster.DeepCopy()
			restoreSpec.Spec.Restore = true
			oldRestoreCommand := buildCommand(restoreSpec)
			for i := range template.Spec.Containers {
				c := &template.Spec.Containers[i]
				if c.Name == "ledger" && slices.Equal(c.Command, oldRestoreCommand) {
					c.Command = buildCommand(cluster)
					c.LivenessProbe = mergeProbe(defaultLivenessProbe(), cluster.Spec.LivenessProbe)
					c.ReadinessProbe = mergeProbe(defaultReadinessProbe(), cluster.Spec.ReadinessProbe)
					c.StartupProbe = mergeProbe(defaultStartupProbe(), cluster.Spec.StartupProbe)
				}
			}
		}
		if !reflect.DeepEqual(template, sts.Spec.Template) {
			sts.Spec.Template = template

			return true, retry, r.Update(ctx, sts)
		}
		if ptr.Deref(sts.Spec.Replicas, 1) != int32(count) {
			sts.Spec.Replicas = new(int32(count))

			return true, retry, r.Update(ctx, sts)
		}
		// Keep the old image, TLS mode and storage wiring
		// until the old topology is back. Only then may normal upgrade/scale-down
		// validation act on the newly requested spec.
		if sts.Status.ObservedGeneration < sts.Generation || sts.Status.ReadyReplicas != int32(count) {
			return true, retry, nil
		}
		// Jobs can restart only after the original Ledger topology is ready.
		// Keep the cursor if this update fails, so a new controller retries it.
		if _, err := r.suspendBackupJobs(ctx, cluster, false); err != nil {
			return true, retry, err
		}
		delete(sts.Annotations, annotationSuspendedReplicas)

		return true, retry, r.Update(ctx, sts)
	}
	jobsStopped, err := r.suspendBackupJobs(ctx, cluster, true)
	if err != nil {
		return true, retry, err
	}
	if sts != nil {
		if saved == "" {
			if sts.Annotations == nil {
				sts.Annotations = map[string]string{}
			}
			count := ptr.Deref(sts.Spec.Replicas, 1)
			if count < 1 {
				return true, retry, errors.New("cannot infer pre-suspension topology from zero replicas")
			}
			sts.Annotations[annotationSuspendedReplicas] = strconv.FormatInt(int64(count), 10)
			sts.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{WhenScaled: appsv1.RetainPersistentVolumeClaimRetentionPolicyType, WhenDeleted: appsv1.RetainPersistentVolumeClaimRetentionPolicyType}
			// A separate update is essential: the StatefulSet controller must first
			// remove GC ownership imposed by Delete policies before terminating Pods.
			return true, retry, r.Update(ctx, sts)
		}
		if sts.Spec.PersistentVolumeClaimRetentionPolicy == nil || sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled != appsv1.RetainPersistentVolumeClaimRetentionPolicyType || sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted != appsv1.RetainPersistentVolumeClaimRetentionPolicyType {
			return true, retry, errors.New("suspended StatefulSet lost Retain policy")
		}
		if sts.Status.ObservedGeneration < sts.Generation {
			return true, retry, nil
		}
		claims := &corev1.PersistentVolumeClaimList{}
		if err := r.List(ctx, claims, client.InNamespace(cluster.Namespace)); err != nil {
			return true, retry, err
		}
		for _, claim := range claims.Items {
			for _, owner := range claim.OwnerReferences {
				// StatefulSet's Delete retention owns claims either directly or through
				// an ordinal Pod. Wait for the controller to relinquish both references.
				if owner.UID == sts.UID || (owner.Kind == "Pod" && hasStatefulSetOrdinal(owner.Name, sts.Name)) {
					return true, retry, nil
				}
			}
		}
		if ptr.Deref(sts.Spec.Replicas, 1) != 0 {
			sts.Spec.Replicas = new(int32(0))

			return true, retry, r.Update(ctx, sts)
		}
	}
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(cluster.Namespace)); err != nil {
		return true, retry, err
	}
	for _, pod := range pods.Items {
		if hasStatefulSetOrdinal(pod.Name, resourceName(cluster.Name)) {
			// An absent StatefulSet may be an interrupted orphan/recreate. Fail
			// closed instead of deleting Pods whose claim retention is unknown.
			if sts == nil {
				return true, retry, fmt.Errorf("cannot suspend orphan Pod %s without its StatefulSet retention state", pod.Name)
			}

			return true, retry, nil
		}
	}
	if !jobsStopped {
		return true, retry, nil
	}
	cluster.Status.Phase = "Suspended"
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: "Suspended", Status: metav1.ConditionTrue, Reason: "WorkloadsStopped", Message: "No Ledger or backup workload Pods remain; durable resources and Raft membership are retained", ObservedGeneration: cluster.Generation})

	return true, retry, nil
}

// suspendBackupJobs checks Job -> BackupRun ownership and the persisted Job
// endpoint. Mutable parent specs and labels alone do not identify the target.
func (r *ClusterReconciler) suspendBackupJobs(ctx context.Context, cluster *ledgerv1alpha1.Cluster, suspend bool) (bool, error) {
	jobs := &batchv1.JobList{}
	if err := r.List(ctx, jobs, client.InNamespace(cluster.Namespace)); err != nil {
		return false, err
	}
	stopped := true
	for i := range jobs.Items {
		job := &jobs.Items[i]
		owner := metav1.GetControllerOf(job)
		if owner == nil || owner.Kind != "BackupRun" || owner.APIVersion != ledgerv1alpha1.GroupVersion.String() {
			continue
		}
		run := &ledgerv1alpha1.BackupRun{}
		if err := r.Get(ctx, types.NamespacedName{Name: owner.Name, Namespace: job.Namespace}, run); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}

			return false, err
		}
		if run.UID != owner.UID {
			continue
		}
		// Parent references can change after creation. Match the endpoint
		// this persisted execution actually contacts, and its UID when known.
		if uid := job.Labels[backupJobLabelClusterUID]; uid != "" && uid != string(cluster.UID) {
			continue
		}
		targetsCluster := false
		endpoint := fmt.Sprintf(`--server "%s.%s.svc.cluster.local:`, resourceName(cluster.Name), cluster.Namespace)
		for _, container := range job.Spec.Template.Spec.Containers {
			if container.Name == backupJobContainerName && len(container.Command) == 3 && container.Command[0] == "/bin/sh" && container.Command[1] == "-c" && backupCommandTargetsCluster(container.Command[2], endpoint) {
				targetsCluster = true
			}
		}
		if !targetsCluster {
			continue
		}
		terminal := false
		for _, condition := range job.Status.Conditions {
			if (condition.Type == batchv1.JobComplete || condition.Type == batchv1.JobFailed) && condition.Status == corev1.ConditionTrue {
				terminal = true
			}
		}
		if terminal {
			continue
		}
		if suspend && !ptr.Deref(job.Spec.Suspend, false) {
			if job.Annotations == nil {
				job.Annotations = map[string]string{}
			}
			job.Annotations[annotationSuspendedJob] = "true"
			job.Spec.Suspend = new(true)
			if err := r.Update(ctx, job); err != nil {
				return false, err
			}
		} else if !suspend && job.Annotations[annotationSuspendedJob] == "true" {
			delete(job.Annotations, annotationSuspendedJob)
			job.Spec.Suspend = new(false)
			if err := r.Update(ctx, job); err != nil {
				return false, err
			}
		}
		if suspend {
			pods := &corev1.PodList{}
			if err := r.List(ctx, pods, client.InNamespace(job.Namespace)); err != nil {
				return false, err
			}
			for _, pod := range pods.Items {
				if metav1.IsControlledBy(&pod, job) && pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
					stopped = false
				}
			}
		}
	}

	return stopped, nil
}

// Only authorization may change before source-topology recovery. The source
// image, TLS transport, storage and Raft startup command remain authoritative.
func resumedAuthorizationTemplate(cluster *ledgerv1alpha1.Cluster, sts *appsv1.StatefulSet, credentials []credentialsKeyInfo) corev1.PodTemplateSpec {
	template := *sts.Spec.Template.DeepCopy()
	desired := buildPodTemplate(cluster, "", credentials, currentTLSModeFromStatefulSet(sts))
	template.Spec.Volumes = slices.DeleteFunc(template.Spec.Volumes, func(v corev1.Volume) bool { return v.Name == "auth-keys" })
	for _, volume := range desired.Spec.Volumes {
		if volume.Name == "auth-keys" {
			template.Spec.Volumes = append(template.Spec.Volumes, volume)
		}
	}
	for i := range template.Spec.Containers {
		container := &template.Spec.Containers[i]
		if container.Name != "ledger" {
			continue
		}
		container.Env = slices.DeleteFunc(container.Env, func(v corev1.EnvVar) bool { return strings.HasPrefix(v.Name, "AUTH_") })
		container.VolumeMounts = slices.DeleteFunc(container.VolumeMounts, func(v corev1.VolumeMount) bool { return v.Name == "auth-keys" })
		for _, source := range desired.Spec.Containers {
			if source.Name != "ledger" {
				continue
			}
			for _, env := range source.Env {
				if strings.HasPrefix(env.Name, "AUTH_") {
					container.Env = append(container.Env, env)
				}
			}
			for _, mount := range source.VolumeMounts {
				if mount.Name == "auth-keys" {
					container.VolumeMounts = append(container.VolumeMounts, mount)
				}
			}
		}
	}
	if hash := desired.Annotations[annotationAuthKeysHash]; hash != "" {
		if template.Annotations == nil {
			template.Annotations = map[string]string{}
		}
		template.Annotations[annotationAuthKeysHash] = hash
	} else {
		delete(template.Annotations, annotationAuthKeysHash)
	}

	return template
}

// ledgerctlCommand emits backup arguments before its final --server flag.
// Earlier shell-quoted user arguments can contain flag-looking text; only
// the final generated flag identifies the actual endpoint of a legacy Job.
func backupCommandTargetsCluster(command, endpointPrefix string) bool {
	flag := strings.LastIndex(command, ` --server "`)

	return flag >= 0 && strings.HasPrefix(command[flag+1:], endpointPrefix)
}
