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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestMandatoryStopVersionBoundary(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		source, target string
		stop           bool
	}{
		{"v3.0.0-rc.1", "v3.0.0-rc.2", true},
		{"v3.0.0-rc.1", "v3.0.0-rc.3", true}, // Selection crosses the boundary; admission still rejects the RC jump.
		{"v3.0.0-rc.2", "v3.0.0-rc.3", false},
		{"v3.0.0-rc.2", "v3.0.0-rc.2", false},
		{"v3.0.0-rc.3", "v3.0.0-rc.2", false},
		{"latest", "v3.0.0-rc.2", false},
	} {
		require.Equal(t, tt.stop, requiresStoppedUpgrade(tt.source, tt.target), "%s -> %s", tt.source, tt.target)
	}
	require.True(t, requiresPinnedUpgradeTarget("v3.0.0-rc.1", "latest"))
	require.True(t, requiresPinnedUpgradeTarget("v3.0.0-rc.1", "3.0.0-rc.2"))
	require.False(t, requiresPinnedUpgradeTarget("v3.0.0-rc.2", "latest"))
}

func TestStoppedUpgradeRepairsDependenciesBeforeWorkloadGate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := newMinimalCluster()
	cluster.UID = types.UID("cluster-uid")
	cluster.Spec.Image.Tag = "v3.0.0-rc.2"
	cluster.Spec.TLS = &ledgerv1alpha1.TLSConfig{Enabled: true}
	cluster.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{Enabled: new(bool)}
	*cluster.Spec.Auth.Enabled = true
	cluster.Labels = map[string]string{"tier": "gold"}
	cluster.Spec.Replicas = new(int32(1))
	applyDefaults(cluster)
	cluster.Status.Upgrade = &ledgerv1alpha1.ClusterUpgradeStatus{
		Source: "v3.0.0-rc.1", Target: cluster.Spec.Image.Tag,
		StatefulSetUID: "old-sts", TargetSpecHash: computeSpecHash(&cluster.Spec),
		ExpectedReplicas: 1, Phase: "Maintenance",
	}
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	cred := matchingCredentials("test-cred", cluster.Labels, true, cluster.Namespace, "test-cred-secret")
	keySecret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "test-cred-secret", Namespace: cluster.Namespace}, Data: map[string][]byte{"pubkey.hex": []byte("deadbeef"), "key-id": []byte("kid-123")}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster, cred, keySecret).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}

	for attempt := range 2 {
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.ErrorContains(t, err, "source StatefulSet disappeared before maintenance barrier")

		svc := &corev1.Service{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Name: headlessServiceName(cluster.Name), Namespace: cluster.Namespace}, svc))
		require.Equal(t, corev1.ClusterIPNone, svc.Spec.ClusterIP)
		secret := &corev1.Secret{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Name: clusterSecretName(cluster.Name), Namespace: cluster.Namespace}, secret))
		require.NotEmpty(t, secret.Data[clusterSecretKey])
		sa := &corev1.ServiceAccount{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Name: serviceAccountName(cluster), Namespace: cluster.Namespace}, sa))
		cm := &corev1.ConfigMap{}
		require.NoError(t, c.Get(ctx, types.NamespacedName{Name: authKeysConfigMapName(cluster.Name), Namespace: cluster.Namespace}, cm))
		require.Contains(t, cm.Data["auth-keys.json"], "kid-123")
		sts := &appsv1.StatefulSet{}
		require.True(t, apierrors.IsNotFound(c.Get(ctx, types.NamespacedName{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}, sts)))

		if attempt == 0 {
			require.NoError(t, c.Delete(ctx, svc))
			require.NoError(t, c.Delete(ctx, secret))
			require.NoError(t, c.Delete(ctx, sa))
			require.NoError(t, c.Delete(ctx, cm))
		}
	}

	// Disabling the Service mid-upgrade must not delete the DNS dependency
	// needed to recover the already-entered stopped upgrade.
	current := &ledgerv1alpha1.Cluster{}
	require.NoError(t, c.Get(ctx, key, current))
	current.Spec.HeadlessService.Enabled = new(bool)
	require.NoError(t, c.Update(ctx, current))
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.ErrorContains(t, err, "source StatefulSet disappeared before maintenance barrier")
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: headlessServiceName(cluster.Name), Namespace: cluster.Namespace}, &corev1.Service{}))

	// An existing Pod may still mount the ConfigMap after Credentials stop
	// matching. The stopped-upgrade repair pass must not delete it.
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: cred.Name}, cred))
	cred.Spec.Selector.MatchLabels = map[string]string{"tier": "silver"}
	require.NoError(t, c.Update(ctx, cred))
	_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.ErrorContains(t, err, "source StatefulSet disappeared before maintenance barrier")
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: authKeysConfigMapName(cluster.Name), Namespace: cluster.Namespace}, &corev1.ConfigMap{}))
}

func TestStoppedUpgradeRequiresHeadlessService(t *testing.T) {
	t.Parallel()
	cluster := newMinimalCluster()
	cluster.Spec.Image.Tag = "v3.0.0-rc.2"
	cluster.Spec.HeadlessService.Enabled = new(bool)
	sts := rcStatefulSet("ghcr.io/formancehq/ledger:v3.0.0-rc.1", true)
	r := &ClusterReconciler{}
	proceed, _, err := r.reconcileRequiredUpgrade(context.Background(), cluster, sts)
	require.False(t, proceed)
	require.ErrorContains(t, err, "spec.headlessService.enabled to remain true")
	require.Nil(t, cluster.Status.Upgrade)
}

func TestStoppedUpgradeWaitsForOldPodsAndRetainsClaim(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := newMinimalCluster()
	cluster.UID = types.UID("cluster-uid")
	cluster.Spec.Image.Tag = "v3.0.0-rc.2"
	cluster.Spec.Replicas = new(int32(1))
	cluster.Spec.Persistence.RetentionPolicy = &ledgerv1alpha1.RetentionPolicySpec{WhenDeleted: "Delete", WhenScaled: "Delete"}
	sts := rcStatefulSet("ghcr.io/formancehq/ledger:v3.0.0-rc.1", true)
	sts.Name, sts.Namespace, sts.UID = resourceName(cluster.Name), cluster.Namespace, types.UID("old-sts")
	sts.Generation = 1
	sts.OwnerReferences = []metav1.OwnerReference{{APIVersion: "ledger.formance.com/v1alpha1", Kind: "Cluster", Name: cluster.Name, UID: cluster.UID, Controller: new(true)}}
	sts.Spec.Replicas = new(int32(1))
	sts.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: "grpc", ContainerPort: 8888}}
	sts.Spec.VolumeClaimTemplates = []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "wal"}}, {ObjectMeta: metav1.ObjectMeta{Name: "data"}}}
	sts.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType, WhenScaled: appsv1.DeletePersistentVolumeClaimRetentionPolicyType}
	sts.Status.ObservedGeneration = 0
	claimName := "wal-" + sts.Name + "-0"
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: claimName, Namespace: cluster.Namespace, UID: types.UID("claim-uid"), OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: sts.Name, UID: sts.UID}}}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-1"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-1", UID: types.UID("pv-uid")}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Name: claimName, Namespace: cluster.Namespace, UID: claim.UID}}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}}
	dataClaimName := "data-" + sts.Name + "-0"
	dataClaim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: dataClaimName, Namespace: cluster.Namespace, UID: types.UID("data-claim-uid")}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "pv-2"}, Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound}}
	dataPV := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv-2", UID: types.UID("data-pv-uid")}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{Name: dataClaimName, Namespace: cluster.Namespace, UID: dataClaim.UID}}, Status: corev1.PersistentVolumeStatus{Phase: corev1.VolumeBound}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: sts.Name + "-0", Namespace: cluster.Namespace, UID: types.UID("old-pod")}}
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster, sts).WithObjects(cluster, sts, claim, pv, dataClaim, dataPV, pod).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme, Config: &rest.Config{}, Clientset: kubernetesfake.NewSimpleClientset()}
	maintenance := false
	var commands []string
	exec := func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, _, _, _ string, command []string) (*execResult, error) {
		cmd := command[len(command)-1]
		commands = append(commands, cmd)
		switch {
		case strings.Contains(cmd, "'maintenance' 'enable'"):
			maintenance = true

			return &execResult{}, nil
		case strings.Contains(cmd, "'maintenance' 'disable'"):
			if !maintenance {
				return nil, errors.New("maintenance was not enabled")
			}
			maintenance = false

			return &execResult{}, nil
		case strings.Contains(cmd, "'cluster' 'status'"):
			if !maintenance {
				return nil, errors.New("status read before maintenance")
			}

			return &execResult{Stdout: `{"state":"Leader","maintenanceMode":true,"raftStatus":{"commit":"12","lastPersistedIndex":"12"}}`}, nil
		default:
			return nil, fmt.Errorf("unexpected command: %s", cmd)
		}
	}
	key := types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}
	// The intent is durable before maintenance, barrier and Retain mutations.
	proceed, _, err := r.reconcileRequiredUpgradeWithExec(ctx, cluster, sts, exec)
	require.NoError(t, err)
	require.False(t, proceed)
	require.Equal(t, "Barrier", cluster.Status.Upgrade.Phase)
	require.True(t, maintenance)
	require.Contains(t, commands[0], "'maintenance' 'enable'")
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, sts, exec)
	require.NoError(t, err)
	require.False(t, proceed)
	require.Equal(t, uint64(12), cluster.Status.Upgrade.BarrierIndex)
	require.Contains(t, commands[1], "'cluster' 'status'")
	require.Equal(t, "Retaining", cluster.Status.Upgrade.Phase)
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, sts, exec)
	require.NoError(t, err)
	require.False(t, proceed)
	require.Equal(t, "Retaining", cluster.Status.Upgrade.Phase)
	require.NoError(t, c.Get(ctx, key, sts))
	require.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, sts.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: claimName, Namespace: cluster.Namespace}, claim))
	require.NotEmpty(t, claim.OwnerReferences)
	// The StatefulSet controller must observe Retain before owner refs are detached.
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, sts, exec)
	require.NoError(t, err)
	require.False(t, proceed)
	require.Equal(t, "Retaining", cluster.Status.Upgrade.Phase)
	sts.Status.ObservedGeneration = sts.Generation
	require.NoError(t, c.Status().Update(ctx, sts))
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, sts, exec)
	require.NoError(t, err)
	require.False(t, proceed)
	require.Equal(t, "Deleting", cluster.Status.Upgrade.Phase)
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: claimName, Namespace: cluster.Namespace}, claim))
	require.Empty(t, claim.OwnerReferences)
	// Restart the reconciler. Deletion cannot cause immediate recreation.
	r = &ClusterReconciler{Client: c, Scheme: scheme, Config: &rest.Config{}, Clientset: kubernetesfake.NewSimpleClientset()}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, cluster))
	r.Client = failingStatefulSetDelete{Client: c}
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, sts, exec)
	require.False(t, proceed)
	require.ErrorContains(t, err, "API delete unavailable")
	require.NoError(t, c.Get(ctx, key, sts))
	r.Client = c
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, sts, exec)
	require.NoError(t, err)
	require.False(t, proceed)
	_, err = r.fetchExistingStatefulSet(ctx, cluster)
	require.NoError(t, err)
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, nil, exec)
	require.NoError(t, err)
	require.False(t, proceed) // old pod still exists
	require.NoError(t, c.Delete(ctx, pod))
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, nil, exec)
	require.NoError(t, err)
	require.True(t, proceed)
	require.Equal(t, "Recreating", cluster.Status.Upgrade.Phase)
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: claimName, Namespace: cluster.Namespace}, claim))
	require.Equal(t, types.UID("claim-uid"), claim.UID)
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "pv-1"}, pv))
	require.Equal(t, types.UID("pv-uid"), pv.UID)
	require.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, buildRetentionPolicy(cluster).WhenDeleted)
	// A replacement with the target image converges without deleting it again.
	replacement := rcStatefulSet("ledger:v3.0.0-rc.2", true)
	replacement.Name, replacement.Namespace, replacement.UID = sts.Name, sts.Namespace, types.UID("new-sts")
	replacement.OwnerReferences = sts.OwnerReferences
	replacement.Spec.Replicas = new(int32(1))
	replacement.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: "grpc", ContainerPort: 8888}}
	replacement.Generation = 1
	replacement.Status.ObservedGeneration = 1
	replacement.Status.UpdatedReplicas = 1
	replacement.Status.ReadyReplicas = 1
	// Simulate a replacement created with the requested Delete policy by an
	// older controller. Repair it before progressing to verification.
	replacement.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{
		WhenDeleted: appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
		WhenScaled:  appsv1.DeletePersistentVolumeClaimRetentionPolicyType,
	}
	require.NoError(t, c.Create(ctx, replacement))
	require.NoError(t, c.Status().Update(ctx, replacement))
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, replacement, exec)
	require.NoError(t, err)
	require.False(t, proceed)
	require.Equal(t, "Recreating", cluster.Status.Upgrade.Phase)
	require.NoError(t, c.Get(ctx, key, replacement))
	require.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, replacement.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted)
	require.Equal(t, appsv1.RetainPersistentVolumeClaimRetentionPolicyType, replacement.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled)
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, replacement, exec)
	require.NoError(t, err)
	require.False(t, proceed)
	require.Equal(t, "Verifying", cluster.Status.Upgrade.Phase)
	// A converged replacement at the wrong replica count cannot release writes.
	replacement.Spec.Replicas = new(int32(2))
	replacement.Status.ReadyReplicas, replacement.Status.UpdatedReplicas = 2, 2
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, replacement, exec)
	require.ErrorContains(t, err, "restore the expected count")
	require.False(t, proceed)
	require.Equal(t, "Verifying", cluster.Status.Upgrade.Phase)
	require.True(t, maintenance)
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: "Upgrade", Status: metav1.ConditionFalse, Reason: "Blocked", Message: "replica count differs"})
	require.NoError(t, c.Status().Update(ctx, cluster))
	replacement.Spec.Replicas = new(int32(1))
	replacement.Status.ReadyReplicas, replacement.Status.UpdatedReplicas = 0, 0
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, replacement, exec)
	require.NoError(t, err)
	require.False(t, proceed)
	require.Equal(t, "Verifying", cluster.Status.Upgrade.Phase)
	require.Equal(t, metav1.ConditionTrue, meta.FindStatusCondition(cluster.Status.Conditions, "Upgrade").Status)
	replacement.Status.ReadyReplicas, replacement.Status.UpdatedReplicas = 1, 1
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, replacement, exec)
	require.NoError(t, err)
	require.False(t, proceed)
	require.Equal(t, "Unblocking", cluster.Status.Upgrade.Phase)
	replacement.Spec.Template.Spec.Containers[0].Image = "ledger:unexpected"
	replacement.Status.ReadyReplicas = 0
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: "Upgrade", Status: metav1.ConditionFalse, Reason: "Blocked", Message: "replacement image changed"})
	require.NoError(t, c.Status().Update(ctx, cluster))
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, replacement, exec)
	require.False(t, proceed)
	require.ErrorContains(t, err, "replacement StatefulSet identity changed")
	require.Equal(t, metav1.ConditionFalse, meta.FindStatusCondition(cluster.Status.Conditions, "Upgrade").Status)
	require.True(t, maintenance)
	replacement.Spec.Template.Spec.Containers[0].Image = "ledger:v3.0.0-rc.2"
	replacement.Status.ReadyReplicas = 1
	// A replacement can itself disappear. Wait for its old Pods, then recreate
	// while keeping the maintenance fence and original volume identities.
	require.NoError(t, c.Delete(ctx, replacement))
	replacementPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: sts.Name + "-0", Namespace: cluster.Namespace}}
	require.NoError(t, c.Create(ctx, replacementPod))
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, nil, exec)
	require.NoError(t, err)
	require.False(t, proceed)
	require.Equal(t, "Unblocking", cluster.Status.Upgrade.Phase)
	require.True(t, maintenance)
	require.NoError(t, c.Delete(ctx, replacementPod))
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, nil, exec)
	require.NoError(t, err)
	require.True(t, proceed)
	require.Equal(t, "Recreating", cluster.Status.Upgrade.Phase)
	require.True(t, maintenance)
	replacement.ResourceVersion = ""
	replacement.UID = types.UID("newer-sts")
	require.NoError(t, c.Create(ctx, replacement))
	require.NoError(t, c.Status().Update(ctx, replacement))
	for _, phase := range []string{"Verifying", "Unblocking"} {
		proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, replacement, exec)
		require.NoError(t, err)
		require.False(t, proceed)
		require.Equal(t, phase, cluster.Status.Upgrade.Phase)
	}
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, replacement, exec)
	require.NoError(t, err)
	require.True(t, proceed)
	require.Nil(t, cluster.Status.Upgrade)
	require.Equal(t, appsv1.DeletePersistentVolumeClaimRetentionPolicyType, buildRetentionPolicy(cluster).WhenDeleted)
	require.False(t, maintenance)
	require.Contains(t, commands[len(commands)-1], "'maintenance' 'disable'")
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, replacement, exec)
	require.NoError(t, err)
	require.True(t, proceed)
	require.NoError(t, c.Get(ctx, key, replacement))
	require.Equal(t, types.UID("newer-sts"), replacement.UID)
}

type failingStatefulSetDelete struct{ client.Client }

func (f failingStatefulSetDelete) Delete(_ context.Context, _ client.Object, _ ...client.DeleteOption) error {
	return errors.New("API delete unavailable")
}

func TestStoppedUpgradeRejectsChangedTargetAndMissingClaim(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := newMinimalCluster()
	cluster.Spec.Image.Tag = "v3.0.0-rc.3"
	cluster.Spec.Replicas = new(int32(1))
	cluster.Status.Upgrade = &ledgerv1alpha1.ClusterUpgradeStatus{Source: "v3.0.0-rc.1", Target: "v3.0.0-rc.2", StatefulSetUID: "old", ExpectedReplicas: 1, Phase: "Deleting"}
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster).WithObjects(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	proceed, _, err := r.reconcileRequiredUpgrade(ctx, cluster, nil)
	require.False(t, proceed)
	require.ErrorContains(t, err, "restore spec.image.tag")
	cluster.Spec.Image.Tag = "v3.0.0-rc.2"
	cluster.Status.Upgrade.TargetSpecHash = computeSpecHash(&cluster.Spec)
	cluster.Status.Upgrade.ClaimUIDs = map[string]string{"wal-ledger-0": "original"}
	proceed, _, err = r.reconcileRequiredUpgrade(ctx, cluster, nil)
	require.False(t, proceed)
	require.ErrorContains(t, err, "missing")
}

func TestStoppedUpgradeBlocksUnpinnedTarget(t *testing.T) {
	t.Parallel()
	cluster := newMinimalCluster()
	cluster.Spec.Image.Tag = "latest"
	sts := rcStatefulSet("ledger:v3.0.0-rc.1", true)
	r := &ClusterReconciler{}
	proceed, _, err := r.reconcileRequiredUpgrade(context.Background(), cluster, sts)
	require.False(t, proceed)
	require.ErrorContains(t, err, "pin a canonical version tag")
}

func TestStoppedUpgradeTargetDriftUpdatesCondition(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := newMinimalCluster()
	cluster.Spec.Image.Tag = "v3.0.0-rc.3"
	cluster.Spec.Replicas = new(int32(3))
	cluster.Status.Upgrade = &ledgerv1alpha1.ClusterUpgradeStatus{Source: "v3.0.0-rc.1", Target: "v3.0.0-rc.2", Phase: "Verifying"}
	meta.SetStatusCondition(&cluster.Status.Conditions, metav1.Condition{Type: "Upgrade", Status: metav1.ConditionTrue, Reason: "Verifying", Message: "in progress"})
	sts := rcStatefulSet("ledger:v3.0.0-rc.1", true)
	sts.Name, sts.Namespace = resourceName(cluster.Name), cluster.Namespace
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(cluster, sts).WithObjects(cluster, sts).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}})
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}, cluster))
	condition := meta.FindStatusCondition(cluster.Status.Conditions, "Upgrade")
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, "Blocked", condition.Reason)
	require.Contains(t, condition.Message, "restore spec.image.tag")
}

func TestStoppedUpgradeUsesLiveGRPCPort(t *testing.T) {
	t.Parallel()
	cluster := newMinimalCluster()
	cluster.Spec.GrpcPort = 9999
	sts := rcStatefulSet("ledger:v3.0.0-rc.1", true)
	sts.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: "grpc", ContainerPort: 8888}}
	r := &ClusterReconciler{Config: &rest.Config{}, Clientset: kubernetesfake.NewSimpleClientset()}
	exec := func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, _, _, _ string, command []string) (*execResult, error) {
		require.Contains(t, command[len(command)-1], ":8888")
		require.NotContains(t, command[len(command)-1], ":9999")

		return &execResult{}, nil
	}
	require.NoError(t, r.execUpgradeCommand(context.Background(), cluster, sts, exec, "cluster", "maintenance", "enable"))
}

func TestStoppedUpgradeWaitsForEveryVoter(t *testing.T) {
	t.Parallel()
	cluster := newMinimalCluster()
	sts := rcStatefulSet("ledger:v3.0.0-rc.1", true)
	sts.Spec.Replicas = new(int32(3))
	sts.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{Name: "grpc", ContainerPort: 8888}}
	r := &ClusterReconciler{Config: &rest.Config{}, Clientset: kubernetesfake.NewSimpleClientset()}
	lagging := true
	var seen []string
	exec := func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, _, _, _ string, command []string) (*execResult, error) {
		cmd := command[len(command)-1]
		seen = append(seen, cmd)
		persisted := 12
		if lagging && strings.Contains(cmd, "'--node-id' '2'") {
			persisted = 11
		}

		return &execResult{Stdout: fmt.Sprintf(`{"state":"Leader","maintenanceMode":true,"raftStatus":{"commit":"12","lastPersistedIndex":"%d"}}`, persisted)}, nil
	}
	ready, err := r.upgradeVotersPersisted(context.Background(), cluster, sts, exec, 12)
	require.NoError(t, err)
	require.False(t, ready)
	require.Len(t, seen, 2)
	require.Contains(t, seen[1], "'--node-id' '2'")
	lagging = false
	seen = nil
	ready, err = r.upgradeVotersPersisted(context.Background(), cluster, sts, exec, 12)
	require.NoError(t, err)
	require.True(t, ready)
	require.Len(t, seen, 3)
	require.Contains(t, seen[2], "'--node-id' '3'")
}
