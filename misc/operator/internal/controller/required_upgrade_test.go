package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
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
}

func TestStoppedUpgradeWaitsForOldPodsAndRetainsClaim(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster := newMinimalCluster()
	cluster.UID = types.UID("cluster-uid")
	cluster.Spec.Image.Tag = "v3.0.0-rc.2"
	cluster.Spec.Replicas = new(int32(1))
	sts := rcStatefulSet("ghcr.io/formancehq/ledger:v3.0.0-rc.1", true)
	sts.Name, sts.Namespace, sts.UID = resourceName(cluster.Name), cluster.Namespace, types.UID("old-sts")
	sts.Generation = 1
	sts.OwnerReferences = []metav1.OwnerReference{{APIVersion: "ledger.formance.com/v1alpha1", Kind: "Cluster", Name: cluster.Name, UID: cluster.UID, Controller: new(true)}}
	sts.Spec.Replicas = new(int32(1))
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
	exec := func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, _, _, _ string, _ []string) (*execResult, error) {
		return &execResult{Stdout: `{"state":"Leader","maintenanceMode":true,"raftStatus":{"commit":"12","lastPersistedIndex":"12"}}`}, nil
	}
	key := types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}
	// The intent is durable before maintenance, barrier and Retain mutations.
	proceed, _, err := r.reconcileRequiredUpgradeWithExec(ctx, cluster, sts, exec)
	require.NoError(t, err)
	require.False(t, proceed)
	require.Equal(t, "Barrier", cluster.Status.Upgrade.Phase)
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, sts, exec)
	require.NoError(t, err)
	require.False(t, proceed)
	require.Equal(t, uint64(12), cluster.Status.Upgrade.BarrierIndex)
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
	// A replacement with the target image converges without deleting it again.
	replacement := rcStatefulSet("ledger:v3.0.0-rc.2", true)
	replacement.Name, replacement.Namespace, replacement.UID = sts.Name, sts.Namespace, types.UID("new-sts")
	replacement.OwnerReferences = sts.OwnerReferences
	replacement.Spec.Replicas = new(int32(1))
	replacement.Generation = 1
	replacement.Status.ObservedGeneration = 1
	replacement.Status.UpdatedReplicas = 1
	replacement.Status.ReadyReplicas = 1
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
	proceed, _, err = r.reconcileRequiredUpgradeWithExec(ctx, cluster, replacement, exec)
	require.NoError(t, err)
	require.True(t, proceed)
	require.NoError(t, c.Get(ctx, key, replacement))
	require.Equal(t, types.UID("new-sts"), replacement.UID)
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
