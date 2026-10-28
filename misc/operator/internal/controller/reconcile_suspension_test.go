package controller

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func suspensionFixture(t *testing.T) (*ClusterReconciler, *ledgerv1alpha1.Cluster, *appsv1.StatefulSet) {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, batchv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	cluster := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "ns", UID: "cluster", Generation: 1}, Spec: ledgerv1alpha1.ClusterSpec{Disabled: true, ClusterID: "stable-id", Replicas: new(int32(3))}}
	applyDefaults(cluster)
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName(cluster.Name), Namespace: cluster.Namespace, UID: "sts", Generation: 4, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(cluster, ledgerv1alpha1.GroupVersion.WithKind("Cluster"))}}, Spec: buildStatefulSetSpec(cluster, "original-hash", nil, tlsModeDisabled)}
	sts.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{WhenScaled: appsv1.DeletePersistentVolumeClaimRetentionPolicyType, WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType}
	sts.Status.ReadyReplicas = 3
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster, sts).WithObjects(cluster, sts).Build()

	return &ClusterReconciler{Client: c, Scheme: scheme}, cluster, sts
}

func TestSuspensionRetainsTopologyAndStorageAcrossRepeatedCycles(t *testing.T) {
	t.Parallel()
	r, cluster, original := suspensionFixture(t)
	ctx := t.Context()
	key := client.ObjectKeyFromObject(original)
	sts := &appsv1.StatefulSet{}
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-ledger-test-2", Namespace: "ns", UID: "durable-claim", OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: original.Name, UID: original.UID}}}}
	require.NoError(t, r.Create(ctx, claim))
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: original.Name + "-2", Namespace: "ns", UID: "pod"}}
	for cycle := range 2 {
		cluster.Spec.Disabled = true
		cluster.Spec.Image.Tag = "v3.0.0-rc.99" // Invalid desired upgrade cannot prevent stopping.
		cluster.Spec.Replicas = new(int32(1))
		require.NoError(t, r.Create(ctx, pod.DeepCopy()))
		handled, _, err := r.reconcileSuspension(ctx, cluster)
		require.NoError(t, err)
		require.True(t, handled)
		require.NoError(t, r.Get(ctx, key, sts))
		require.EqualValues(t, 3, *sts.Spec.Replicas)
		require.Equal(t, "3", sts.Annotations[annotationSuspendedReplicas])
		require.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled)
		require.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
		require.Equal(t, original.Spec.Template, sts.Spec.Template)
		_, _, err = r.reconcileSuspension(ctx, cluster)
		require.NoError(t, err)
		require.NoError(t, r.Get(ctx, key, sts))
		require.EqualValues(t, 3, *sts.Spec.Replicas, "wait for retention observation")
		sts.Status.ObservedGeneration = sts.Generation
		require.NoError(t, r.Status().Update(ctx, sts))
		if cycle == 0 {
			_, _, err = r.reconcileSuspension(ctx, cluster)
			require.NoError(t, err)
			require.NoError(t, r.Get(ctx, key, sts))
			require.EqualValues(t, 3, *sts.Spec.Replicas, "GC ownership must be removed first")
			claim.OwnerReferences = []metav1.OwnerReference{{Kind: "Pod", Name: original.Name + "-2", UID: "old-pod"}}
			require.NoError(t, r.Update(ctx, claim))
			_, _, err = r.reconcileSuspension(ctx, cluster)
			require.NoError(t, err)
			require.NoError(t, r.Get(ctx, key, sts))
			require.EqualValues(t, 3, *sts.Spec.Replicas, "ordinal Pod owner must also be removed before zero")
			claim.OwnerReferences = nil
			require.NoError(t, r.Update(ctx, claim))
		}
		_, _, err = r.reconcileSuspension(ctx, cluster)
		require.NoError(t, err)
		require.NoError(t, r.Get(ctx, key, sts))
		require.Zero(t, *sts.Spec.Replicas)
		_, _, err = r.reconcileSuspension(ctx, cluster)
		require.NoError(t, err)
		require.Equal(t, "Suspending", cluster.Status.Phase)
		require.NoError(t, r.Delete(ctx, pod))
		_, _, err = r.reconcileSuspension(ctx, cluster)
		require.NoError(t, err)
		require.Equal(t, "Suspended", cluster.Status.Phase)
		require.True(t, meta.IsStatusConditionTrue(cluster.Status.Conditions, "Suspended"))
		cluster.Spec.Disabled = false
		r = &ClusterReconciler{Client: r.Client, Scheme: r.Scheme} // Controller replacement.
		_, _, err = r.reconcileSuspension(ctx, cluster)
		require.NoError(t, err)
		require.NoError(t, r.Get(ctx, key, sts))
		require.EqualValues(t, 3, *sts.Spec.Replicas, "restore all old voters before desired scale-down to one")
		require.Equal(t, original.Spec.Template, sts.Spec.Template)
		sts.Status.ReadyReplicas = 2
		require.NoError(t, r.Status().Update(ctx, sts))
		_, _, err = r.reconcileSuspension(ctx, cluster)
		require.NoError(t, err)
		require.NoError(t, r.Get(ctx, key, sts))
		require.Equal(t, "3", sts.Annotations[annotationSuspendedReplicas])
		sts.Status.ReadyReplicas = 3
		require.NoError(t, r.Status().Update(ctx, sts))
		_, _, err = r.reconcileSuspension(ctx, cluster)
		require.NoError(t, err)
		require.NoError(t, r.Get(ctx, key, sts))
		require.Empty(t, sts.Annotations[annotationSuspendedReplicas])
		require.Equal(t, original.UID, sts.UID)
		require.Equal(t, "stable-id", cluster.Spec.ClusterID)
		require.NoError(t, r.Get(ctx, client.ObjectKeyFromObject(claim), claim))
		require.Equal(t, types.UID("durable-claim"), claim.UID)
		sts.Generation++ // New generation for next retention handshake.
		require.NoError(t, r.Update(ctx, sts))
	}
}

type failSuspensionWrite struct {
	client.Client

	fail bool
}

func (c *failSuspensionWrite) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	if _, ok := obj.(*appsv1.StatefulSet); ok && c.fail {
		c.fail = false

		return errors.New("injected retention write failure")
	}

	return c.Client.Update(ctx, obj, opts...)
}
func TestSuspensionFailedRetentionWriteNeverScales(t *testing.T) {
	t.Parallel()
	r, cluster, sts := suspensionFixture(t)
	base := r.Client
	r.Client = &failSuspensionWrite{Client: base, fail: true}
	_, _, err := r.reconcileSuspension(t.Context(), cluster)
	require.ErrorContains(t, err, "injected retention")
	stored := &appsv1.StatefulSet{}
	require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(sts), stored))
	require.EqualValues(t, 3, *stored.Spec.Replicas)
	require.Empty(t, stored.Annotations)
	_, _, err = r.reconcileSuspension(t.Context(), cluster)
	require.NoError(t, err)
	require.NoError(t, base.Get(t.Context(), client.ObjectKeyFromObject(sts), stored))
	require.EqualValues(t, 3, *stored.Spec.Replicas)
	require.Equal(t, "3", stored.Annotations[annotationSuspendedReplicas])
}
func TestInitiallyDisabledClusterPersistsIdentityWithoutWorkloads(t *testing.T) {
	t.Parallel()
	r, cluster, sts := suspensionFixture(t)
	require.NoError(t, r.Delete(t.Context(), sts))
	cluster.Spec.ClusterID = ""
	require.NoError(t, r.Update(t.Context(), cluster))
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}
	identity := ""
	for range 2 {
		_, err := r.Reconcile(t.Context(), request)
		require.NoError(t, err)
		require.NoError(t, r.Get(t.Context(), request.NamespacedName, cluster))
		require.NotEmpty(t, cluster.Spec.ClusterID)
		if identity == "" {
			identity = cluster.Spec.ClusterID
		}
		require.Equal(t, identity, cluster.Spec.ClusterID)
		require.Equal(t, "Suspended", cluster.Status.Phase)
		require.True(t, meta.IsStatusConditionFalse(cluster.Status.Conditions, "Ready"))
		sets := &appsv1.StatefulSetList{}
		require.NoError(t, r.List(t.Context(), sets))
		require.Empty(t, sets.Items)
	}
}
func TestSuspensionBackupJobsRetainExecutionIdentity(t *testing.T) {
	t.Parallel()
	r, cluster, _ := suspensionFixture(t)
	backup := &ledgerv1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "backup", Namespace: "ns"}, Spec: ledgerv1alpha1.BackupSpec{ClusterRef: cluster.Name}}
	run := &ledgerv1alpha1.BackupRun{ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: "ns", UID: "run"}, Spec: ledgerv1alpha1.BackupRunSpec{BackupRef: backup.Name}}
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "ns", UID: "job", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(run, ledgerv1alpha1.GroupVersion.WithKind("BackupRun"))}}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "backup-worker", Namespace: "ns", OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(job, batchv1.SchemeGroupVersion.WithKind("Job"))}}}
	job.Spec.Template.Spec.Containers = []corev1.Container{{Name: backupJobContainerName, Command: ledgerctlCommand(backupServerAddr(cluster), tlsModeDisabled)}}
	for _, obj := range []client.Object{backup, run, job, pod} {
		require.NoError(t, r.Create(t.Context(), obj))
	}
	stopped, err := r.suspendBackupJobs(t.Context(), cluster, true)
	require.NoError(t, err)
	require.False(t, stopped)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(job), job))
	require.True(t, *job.Spec.Suspend)
	require.NoError(t, r.Delete(t.Context(), pod))
	stopped, err = r.suspendBackupJobs(t.Context(), cluster, true)
	require.NoError(t, err)
	require.True(t, stopped)
	_, err = r.suspendBackupJobs(t.Context(), cluster, false)
	require.NoError(t, err)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(job), job))
	require.False(t, *job.Spec.Suspend)
	require.Equal(t, types.UID("job"), job.UID)
	job.Spec.Suspend = new(true) // Preserve user suspension.
	require.NoError(t, r.Update(t.Context(), job))
	_, err = r.suspendBackupJobs(t.Context(), cluster, false)
	require.NoError(t, err)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(job), job))
	require.True(t, *job.Spec.Suspend)
}

func TestSuspensionBackupTargetSurvivesMutableParents(t *testing.T) {
	t.Parallel()
	r, cluster, _ := suspensionFixture(t)
	other := cluster.DeepCopy()
	other.Name = "other"
	other.UID = "other"
	backup := &ledgerv1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "backup", Namespace: cluster.Namespace}, Spec: ledgerv1alpha1.BackupSpec{ClusterRef: other.Name}}
	run := &ledgerv1alpha1.BackupRun{ObjectMeta: metav1.ObjectMeta{Name: "run", Namespace: cluster.Namespace, UID: "run"}, Spec: ledgerv1alpha1.BackupRunSpec{BackupRef: backup.Name}}
	// A legacy Job has no Cluster UID label; its persisted command is still
	// authoritative even after both parent references have been changed.
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "legacy", Namespace: cluster.Namespace, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(run, ledgerv1alpha1.GroupVersion.WithKind("BackupRun"))}}, Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: backupJobContainerName, Command: ledgerctlCommand(backupServerAddr(cluster), tlsModeDisabled)}}}}}}
	for _, obj := range []client.Object{backup, run, job} {
		require.NoError(t, r.Create(t.Context(), obj))
	}
	_, err := r.suspendBackupJobs(t.Context(), other, true)
	require.NoError(t, err)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(job), job))
	require.False(t, ptr.Deref(job.Spec.Suspend, false))
	_, err = r.suspendBackupJobs(t.Context(), cluster, true)
	require.NoError(t, err)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(job), job))
	require.True(t, *job.Spec.Suspend)
	run.Spec.BackupRef = "missing-parent"
	require.NoError(t, r.Update(t.Context(), run))
	_, err = r.suspendBackupJobs(t.Context(), cluster, false)
	require.NoError(t, err)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(job), job))
	require.False(t, *job.Spec.Suspend)
}

func TestSuspensionRepairsAuthorizationBeforeRestoringVoters(t *testing.T) {
	t.Parallel()
	r, cluster, sts := suspensionFixture(t)
	cluster.Spec.Disabled = false
	cluster.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{Enabled: new(true)}
	cred := matchingCredentials("client", nil, true, cluster.Namespace, "public-key")
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "public-key", Namespace: cluster.Namespace}, Data: map[string][]byte{"pubkey.hex": []byte("deadbeef"), "key-id": []byte("kid")}}
	for _, obj := range []client.Object{cred, secret} {
		require.NoError(t, r.Create(t.Context(), obj))
	}
	sts.Annotations = map[string]string{annotationSuspendedReplicas: "3"}
	sts.Spec.Replicas = new(int32(0))
	sts.Spec.Template = buildPodTemplate(cluster, "retained", []credentialsKeyInfo{{KeyID: "old"}}, tlsModeRequired)
	require.NoError(t, r.Update(t.Context(), sts))
	// There is intentionally no auth ConfigMap: recovery must repair it before
	// the required volume or revoked metadata can become active on a new Pod.
	_, _, err := r.reconcileSuspension(t.Context(), cluster)
	require.NoError(t, err)
	cm := &corev1.ConfigMap{}
	require.NoError(t, r.Get(t.Context(), types.NamespacedName{Name: authKeysConfigMapName(cluster.Name), Namespace: cluster.Namespace}, cm))
	require.Contains(t, cm.Data["auth-keys.json"], "kid")
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sts), sts))
	require.Zero(t, *sts.Spec.Replicas)
	// Scope reduction must also be persisted while all voters remain stopped.
	cred.Spec.Scopes = []string{"narrow"}
	require.NoError(t, r.Update(t.Context(), cred))
	_, _, err = r.reconcileSuspension(t.Context(), cluster)
	require.NoError(t, err)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(cm), cm))
	require.Contains(t, cm.Data["auth-keys.json"], "narrow")
	require.NotContains(t, cm.Data["auth-keys.json"], `"read"`)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sts), sts))
	require.Zero(t, *sts.Spec.Replicas)
	// Removing the final Credential and explicitly turning auth off must remove
	// the obsolete required volume before startup, rather than deadlocking it.
	require.NoError(t, r.Delete(t.Context(), cred))
	cluster.Spec.Auth.Enabled = new(false)
	_, _, err = r.reconcileSuspension(t.Context(), cluster)
	require.NoError(t, err)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sts), sts))
	require.Zero(t, *sts.Spec.Replicas)
	for _, v := range sts.Spec.Template.Spec.Volumes {
		require.NotEqual(t, "auth-keys", v.Name)
	}
	for _, env := range sts.Spec.Template.Spec.Containers[0].Env {
		require.NotEqual(t, "AUTH_ED25519_KEYS", env.Name)
	}
}

func TestSuspensionAllowsFinalizedRestoreHandoffBeforeReadiness(t *testing.T) {
	t.Parallel()
	r, cluster, sts := suspensionFixture(t)
	cluster.Spec.Restore = true
	sts.Spec.Template = buildPodTemplate(cluster, "restore", nil, tlsModeDisabled)
	sts.Spec.Replicas = new(int32(0))
	sts.Annotations = map[string]string{annotationSuspendedReplicas: "3"}
	require.NoError(t, r.Update(t.Context(), sts))
	cluster.Spec.Disabled = false
	cluster.Spec.Restore = false
	// A finalized restore cannot boot --restore (RESTORED is already present).
	// Neither a ready Pod nor storage inspection is needed for this explicit
	// user-requested handoff; normal startup enforces persisted-state checks.
	_, _, err := r.reconcileSuspension(t.Context(), cluster)
	require.NoError(t, err)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sts), sts))
	require.Zero(t, *sts.Spec.Replicas)
	require.Equal(t, buildCommand(cluster), sts.Spec.Template.Spec.Containers[0].Command)
	require.NotNil(t, sts.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet)
	_, _, err = r.reconcileSuspension(t.Context(), cluster)
	require.NoError(t, err)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sts), sts))
	require.EqualValues(t, 3, *sts.Spec.Replicas)
}

type suspensionReadBarrier struct {
	client.Client

	arrived *sync.WaitGroup
	release <-chan struct{}
}

func (c suspensionReadBarrier) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	err := c.Client.Get(ctx, key, obj, opts...)
	if _, ok := obj.(*appsv1.StatefulSet); ok && err == nil {
		c.arrived.Done()
		<-c.release
	}

	return err
}
func TestSuspensionConcurrentReconcilersPreserveRetentionCursor(t *testing.T) {
	t.Parallel()
	r, cluster, sts := suspensionFixture(t)
	var arrived sync.WaitGroup
	arrived.Add(2)
	release := make(chan struct{})
	results := make(chan error, 2)
	for range 2 {
		worker := &ClusterReconciler{Client: suspensionReadBarrier{Client: r.Client, arrived: &arrived, release: release}, Scheme: r.Scheme}
		workerCluster := cluster.DeepCopy()
		go func() { _, _, err := worker.reconcileSuspension(t.Context(), workerCluster); results <- err }()
	}
	arrived.Wait()
	close(release)
	conflicts := 0
	for range 2 {
		if err := <-results; err != nil {
			require.True(t, apierrors.IsConflict(err), "%v", err)
			conflicts++
		}
	}
	require.Equal(t, 1, conflicts, "exactly one stale resourceVersion must be rejected")
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sts), sts))
	require.EqualValues(t, 3, *sts.Spec.Replicas)
	require.Equal(t, "3", sts.Annotations[annotationSuspendedReplicas])
	require.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled)
	// The losing worker retries from the winning cursor without replacing it.
	_, _, err := r.reconcileSuspension(t.Context(), cluster)
	require.NoError(t, err)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sts), sts))
	require.EqualValues(t, 3, *sts.Spec.Replicas)
}

func TestDisabledClusterStopsBeforeValidationAndCredentialGates(t *testing.T) {
	t.Parallel()
	r, cluster, sts := suspensionFixture(t)
	cluster.Spec.Replicas = new(int32(2))   // Invalid even topology for ordinary reconciliation.
	cluster.Spec.Image.Tag = "v3.0.0-rc.99" // Unsupported predecessor transition.
	cluster.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{Enabled: new(true)}
	require.NoError(t, r.Update(t.Context(), cluster))
	require.NoError(t, r.Create(t.Context(), matchingCredentials("unresolved", nil, false, "", "")))
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
	require.NoError(t, err)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(sts), sts))
	require.Equal(t, "3", sts.Annotations[annotationSuspendedReplicas])
	require.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled)
	require.NoError(t, r.Get(t.Context(), client.ObjectKeyFromObject(cluster), cluster))
	require.Equal(t, "Suspending", cluster.Status.Phase)
	require.EqualValues(t, 2, *cluster.Spec.Replicas, "pause never rewrites desired topology")
}

func TestSuspensionLegacyBackupIgnoresFlagTextInsideArguments(t *testing.T) {
	t.Parallel()
	command := ledgerctlCommand("ledger-A.ns.svc.cluster.local:8888", tlsModeDisabled, "store", "backup", "--s3-bucket", `--server "ledger-B.ns.svc.cluster.local:8888"`)[2]
	require.False(t, backupCommandTargetsCluster(command, `--server "ledger-B.ns.svc.cluster.local:`))
	require.True(t, backupCommandTargetsCluster(command, `--server "ledger-A.ns.svc.cluster.local:`))
}
