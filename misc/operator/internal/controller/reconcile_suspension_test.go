package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestSuspensionPreflightRejectsUnsafeStorageAndUnfinishedChanges(t *testing.T) {
	t.Parallel()
	replicas := int32(3)
	cluster := &ledgerv1alpha1.Cluster{Spec: ledgerv1alpha1.ClusterSpec{
		Replicas: &replicas, Image: ledgerv1alpha1.ImageSpec{Repository: "ledger", Tag: "pinned"},
	}}
	sts := &appsv1.StatefulSet{Spec: appsv1.StatefulSetSpec{
		Replicas: &replicas, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{annotationSpecHash: computeSpecHash(&cluster.Spec)}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ledger", Image: "ledger:pinned", Env: []corev1.EnvVar{{Name: "TLS_MODE", Value: tlsModeDisabled}}}}}},
	}, Status: appsv1.StatefulSetStatus{ReadyReplicas: 3, UpdatedReplicas: 3, ObservedGeneration: 1}}
	sts.Generation = 1
	require.NoError(t, suspensionPreflight(cluster, sts))

	cluster.Spec.Persistence.WAL.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{Path: "/data"}
	require.ErrorContains(t, suspensionPreflight(cluster, sts), "hostPath")
	cluster.Spec.Persistence.WAL.HostPath = nil
	cluster.Spec.Persistence.RetentionPolicy = &ledgerv1alpha1.RetentionPolicySpec{WhenScaled: "Delete"}
	require.ErrorContains(t, suspensionPreflight(cluster, sts), "retention")
	cluster.Spec.Persistence.RetentionPolicy = nil
	sts.Status.ReadyReplicas = 2
	require.ErrorContains(t, suspensionPreflight(cluster, sts), "ready")
	sts.Status.ReadyReplicas = 3
	cluster.Spec.TLS = &ledgerv1alpha1.TLSConfig{Enabled: true}
	cluster.Status.TLSRollout = &ledgerv1alpha1.TLSRolloutStatus{Mode: tlsModeRequired}
	sts.Spec.Template.Annotations[annotationSpecHash] = computeSpecHash(&cluster.Spec)
	require.ErrorContains(t, suspensionPreflight(cluster, sts), "TLS rollout")
	sts.Spec.Template.Spec.Containers[0].Env[0].Value = tlsModeRequired
	require.NoError(t, suspensionPreflight(cluster, sts), "stable TLSRollout status must not block suspension")
}

func TestSuspensionMembershipRequiresTheOriginalVoters(t *testing.T) {
	t.Parallel()
	var status suspensionStatusJSON
	status.Leader = 1
	for id := uint32(1); id <= 3; id++ {
		status.Nodes = append(status.Nodes, struct {
			ID       uint32 `json:"id"`
			Suffrage string `json:"suffrage"`
		}{ID: id, Suffrage: "Voter"})
	}
	require.True(t, suspensionMembershipComplete(status, 3))
	status.Nodes[2].Suffrage = "Learner"
	require.False(t, suspensionMembershipComplete(status, 3))
	status.Nodes[2].Suffrage = "Voter"
	status.Nodes[2].ID = 2
	require.False(t, suspensionMembershipComplete(status, 3))
}

func TestBackupAdmissionReservationFencesSuspensionIntent(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	cluster := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default"}}
	backup := &ledgerv1alpha1.Backup{ObjectMeta: metav1.ObjectMeta{Name: "daily", Namespace: "default"}, Spec: ledgerv1alpha1.BackupSpec{ClusterRef: "test"}}
	run := &ledgerv1alpha1.BackupRun{ObjectMeta: metav1.ObjectMeta{Name: "run-1", Namespace: "default", UID: "run-uid"}, Spec: ledgerv1alpha1.BackupRunSpec{BackupRef: "daily"}}
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, backup, run).
		WithStatusSubresource(&ledgerv1alpha1.Cluster{}, &ledgerv1alpha1.BackupRun{}).Build()
	runController := &BackupRunReconciler{Client: c, APIReader: c}
	clusterController := &ClusterReconciler{Client: c, APIReader: c}
	stale := &ledgerv1alpha1.Cluster{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, stale))
	admitted, err := runController.reserveClusterBackup(ctx, "test", run)
	require.NoError(t, err)
	require.True(t, admitted)
	require.Error(t, c.Status().Update(ctx, stale), "a stale suspension checkpoint must lose the resource-version race")
	require.ErrorContains(t, clusterController.suspensionInFlightBackups(ctx, cluster), "reserved Cluster admission")

	latest := &ledgerv1alpha1.Cluster{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, latest))
	latest.Spec.Disabled = true
	require.NoError(t, c.Update(ctx, latest))
	second := &ledgerv1alpha1.BackupRun{ObjectMeta: metav1.ObjectMeta{Name: "run-2", Namespace: "default", UID: "second-uid"}}
	admitted, err = runController.reserveClusterBackup(ctx, "test", second)
	require.NoError(t, err)
	require.False(t, admitted, "a new run cannot reserve admission after disable")

	currentRun := &ledgerv1alpha1.BackupRun{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: run.Name, Namespace: run.Namespace}, currentRun))
	currentRun.Status.Phase = ledgerv1alpha1.BackupRunPhaseSucceeded
	require.NoError(t, c.Status().Update(ctx, currentRun))
	require.NoError(t, clusterController.suspensionInFlightBackups(ctx, cluster))
	require.NoError(t, runController.releaseClusterBackupReservation(ctx, currentRun))
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "test", Namespace: "default"}, latest))
	require.Empty(t, latest.Status.BackupReservations)
}

func TestSuspensionResumeRevokesKeysBeforeStartingPods(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	replicas, zero := int32(1), int32(0)
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "cluster-original"},
		Spec: ledgerv1alpha1.ClusterSpec{Replicas: &replicas, ClusterID: "original-id", Image: ledgerv1alpha1.ImageSpec{Repository: "ledger", Tag: "pinned"},
			Auth: &ledgerv1alpha1.AuthorizationConfig{Enabled: new(true), Issuers: []string{"https://issuer.example"}}},
	}
	wal, data := boundSuspensionPVC("wal-ledger-test-0"), boundSuspensionPVC("data-ledger-test-0")
	cluster.Status.Suspension = &ledgerv1alpha1.SuspensionStatus{
		Phase: suspensionPhaseStarting, ExpectedReplicas: replicas, BarrierIndex: 10,
		PinnedSpecHash: computeSpecHash(&cluster.Spec), PinnedImage: "ledger:pinned", StatefulSetUID: "sts-original",
		PVCUIDs: map[string]string{wal.Name: string(wal.UID), data.Name: string(data.UID)},
		PVUIDs:  map[string]string{wal.Name: string(boundSuspensionPV(wal).UID), data.Name: string(boundSuspensionPV(data).UID)},
	}
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "ledger-test", Namespace: "default", UID: "sts-original"},
		Spec: appsv1.StatefulSetSpec{Replicas: &zero,
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{annotationSpecHash: computeSpecHash(&cluster.Spec)}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ledger", Image: "ledger:pinned", Env: []corev1.EnvVar{{Name: "TLS_MODE", Value: tlsModeDisabled}, {Name: "AUTH_ED25519_KEYS", Value: "/auth-keys/auth-keys.json"}}}}}},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "wal"}}, {ObjectMeta: metav1.ObjectMeta{Name: "data"}}}}}
	staleKeys := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: authKeysConfigMapName(cluster.Name), Namespace: cluster.Namespace,
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "ledger.formance.com/v1alpha1", Kind: "Cluster", Name: cluster.Name, UID: cluster.UID, Controller: new(true)}}},
		Data: map[string]string{"revoked": "old-key"}}
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, sts, wal, data, boundSuspensionPV(wal), boundSuspensionPV(data), staleKeys).
		WithStatusSubresource(&ledgerv1alpha1.Cluster{}, &appsv1.StatefulSet{}).Build()
	credential := matchingCredentials("revoked-credential", nil, true, cluster.Namespace, "revoked-secret")
	require.NoError(t, c.Create(ctx, credential))
	require.NoError(t, c.Delete(ctx, credential)) // Revoked while the original Pods are stopped.
	r := &ClusterReconciler{Client: c, APIReader: c, Scheme: scheme, Config: &rest.Config{}, Clientset: fake.NewSimpleClientset()}
	_, err := r.reconcileSuspension(ctx, cluster, nil)
	require.NoError(t, err)
	updatedKeys := &corev1.ConfigMap{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: staleKeys.Name, Namespace: staleKeys.Namespace}, updatedKeys))
	require.JSONEq(t, `{"keys":[]}`, updatedKeys.Data["auth-keys.json"], "revoked keys must be removed before replicas are restored")
	updatedSTS := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}, updatedSTS))
	require.Equal(t, zero, *updatedSTS.Spec.Replicas, "remove stale static-key boot wiring before restoring replicas")
	require.NotContains(t, updatedSTS.Spec.Template.Spec.Containers[0].Env, corev1.EnvVar{Name: "AUTH_ED25519_KEYS", Value: "/auth-keys/auth-keys.json"})
	_, err = r.reconcileSuspension(ctx, cluster, nil)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}, updatedSTS))
	require.Equal(t, replicas, *updatedSTS.Spec.Replicas)
}

func TestSuspensionCycleRetainsIdentityVolumesAndMembership(t *testing.T) {
	t.Parallel()
	runSuspensionCycle(t, "")
}

func TestSuspensionEarlyResumeAfterQuiescing(t *testing.T) {
	t.Parallel()
	runSuspensionCycle(t, suspensionPhaseBarrier)
}

func TestSuspensionEarlyResumeAfterStoppingCheckpoint(t *testing.T) {
	t.Parallel()
	runSuspensionCycle(t, suspensionPhaseStopping)
}

func runSuspensionCycle(t *testing.T, cancelPhase string) {
	t.Helper()
	ctx := t.Context()
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	replicas := int32(1)
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "default", UID: "cluster-original"},
		Spec:       ledgerv1alpha1.ClusterSpec{Disabled: true, Replicas: &replicas, ClusterID: "original-id", Image: ledgerv1alpha1.ImageSpec{Repository: "ledger", Tag: "pinned"}},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ledger-test", Namespace: "default", UID: "sts-original", Generation: 1},
		Spec: appsv1.StatefulSetSpec{
			Replicas:             &replicas,
			Template:             corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Annotations: map[string]string{annotationSpecHash: computeSpecHash(&cluster.Spec)}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ledger", Image: "ledger:pinned", Env: []corev1.EnvVar{{Name: "TLS_MODE", Value: tlsModeDisabled}}}}}},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "wal"}}, {ObjectMeta: metav1.ObjectMeta{Name: "data"}}},
		},
		Status: appsv1.StatefulSetStatus{Replicas: 1, ReadyReplicas: 1, UpdatedReplicas: 1, ObservedGeneration: 1},
	}
	wal := boundSuspensionPVC("wal-ledger-test-0")
	data := boundSuspensionPVC("data-ledger-test-0")
	c := clientfake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, sts, wal, data, boundSuspensionPV(wal), boundSuspensionPV(data)).
		WithStatusSubresource(&ledgerv1alpha1.Cluster{}, &appsv1.StatefulSet{}).Build()
	r := &ClusterReconciler{Client: c, APIReader: c, Scheme: scheme, Config: &rest.Config{}, Clientset: fake.NewSimpleClientset()}
	maintenance := false
	enableCalls := 0
	exec := func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, _, _, _ string, command []string) (*execResult, error) {
		script := command[len(command)-1]
		switch {
		case strings.Contains(script, "'enable'"):
			enableCalls++
			maintenance = true

			return nil, errors.New("injected lost maintenance response")
		case strings.Contains(script, "'disable'"):
			maintenance = false

			return &execResult{}, nil
		case strings.Contains(script, "'status'"):
			return &execResult{Stdout: fmt.Sprintf(`{"state":"Leader","leader":1,"maintenanceMode":%t,"nodes":[{"id":1,"suffrage":"Voter"}],"raftStatus":{"commit":10,"lastPersistedIndex":10}}`, maintenance)}, nil
		default:
			t.Fatalf("unexpected ledgerctl command: %v", command)

			return nil, nil
		}
	}
	key := types.NamespacedName{Name: "test", Namespace: "default"}
	stsKey := types.NamespacedName{Name: "ledger-test", Namespace: "default"}
	stopped := false
	for step := range 16 {
		current := &ledgerv1alpha1.Cluster{}
		require.NoError(t, c.Get(ctx, key, current))
		if cancelPhase != "" && current.Status.Suspension != nil && current.Status.Suspension.Phase == cancelPhase && current.Spec.Disabled {
			current.Spec.Disabled = false
			require.NoError(t, c.Update(ctx, current))
		}
		if cancelPhase == "" && current.Status.Suspension != nil && current.Status.Suspension.Phase == suspensionPhaseSuspended && current.Spec.Disabled {
			stopped := &appsv1.StatefulSet{}
			require.NoError(t, c.Get(ctx, stsKey, stopped))
			require.Equal(t, int32(0), *stopped.Spec.Replicas)
			require.True(t, maintenance)
			current.Spec.Disabled = false
			require.NoError(t, c.Update(ctx, current))
		}
		if current.Status.Suspension != nil && current.Status.Suspension.Phase == suspensionPhaseRunning && !current.Spec.Disabled {
			break
		}
		_, err := r.reconcileSuspension(ctx, current, exec)
		require.NoError(t, err, "step %d", step)
		require.NoError(t, r.updateStatus(ctx, current))
		liveSTS := &appsv1.StatefulSet{}
		require.NoError(t, c.Get(ctx, stsKey, liveSTS))
		if *liveSTS.Spec.Replicas == 0 && liveSTS.Status.Replicas != 0 {
			stopped = true
			liveSTS.Status.Replicas, liveSTS.Status.ReadyReplicas, liveSTS.Status.UpdatedReplicas = 0, 0, 0
			require.NoError(t, c.Status().Update(ctx, liveSTS))
		} else if *liveSTS.Spec.Replicas == 1 && liveSTS.Status.Replicas == 0 {
			liveSTS.Status.Replicas, liveSTS.Status.ReadyReplicas, liveSTS.Status.UpdatedReplicas = 1, 1, 1
			liveSTS.Status.ObservedGeneration = liveSTS.Generation
			require.NoError(t, c.Status().Update(ctx, liveSTS))
		}
	}
	final := &ledgerv1alpha1.Cluster{}
	require.NoError(t, c.Get(ctx, key, final))
	require.NotNil(t, final.Status.Suspension)
	require.Equal(t, suspensionPhaseRunning, final.Status.Suspension.Phase)
	require.Positive(t, final.Status.Suspension.BarrierIndex)
	if cancelPhase != "" {
		require.False(t, stopped, "canceling disable before the barrier must not stop the StatefulSet")
	}
	require.Equal(t, "original-id", final.Spec.ClusterID)
	require.False(t, maintenance)
	require.Equal(t, 1, enableCalls, "lost response must be resolved by observing maintenance, not by repeating the command")
	for _, name := range []string{wal.Name, data.Name} {
		gotPVC := &corev1.PersistentVolumeClaim{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Name: name, Namespace: "default"}, gotPVC))
		require.Equal(t, final.Status.Suspension.PVCUIDs[name], string(gotPVC.UID))
		gotPV := &corev1.PersistentVolume{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Name: gotPVC.Spec.VolumeName}, gotPV))
		require.Equal(t, final.Status.Suspension.PVUIDs[name], string(gotPV.UID))
	}
}

func boundSuspensionPVC(name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", UID: types.UID("uid-" + name)},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-" + name},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
}

func boundSuspensionPV(pvc *corev1.PersistentVolumeClaim) *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvc.Spec.VolumeName, UID: types.UID("uid-" + pvc.Spec.VolumeName)},
		Spec:       corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Name: pvc.Name, Namespace: pvc.Namespace, UID: pvc.UID}},
	}
}
