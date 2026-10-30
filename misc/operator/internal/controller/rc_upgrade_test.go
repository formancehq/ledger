package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func rcStatefulSet(image string, converged bool) *appsv1.StatefulSet {
	replicas := int32(3)
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Generation: 2},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{
				{Name: "ledger", Image: image},
			}}},
		},
		Status: appsv1.StatefulSetStatus{ObservedGeneration: 2, UpdatedReplicas: replicas, ReadyReplicas: replicas},
	}
	if !converged {
		sts.Status.ReadyReplicas--
	}

	return sts
}

func TestValidateRCUpgrade(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		source    string
		target    string
		converged bool
		wantError string
	}{
		{"adjacent", "v3.0.0-rc.1", "v3.0.0-rc.2", true, ""},
		{"generic release line", "v4.12.7-rc.9", "v4.12.7-rc.10", true, ""},
		{"skip", "v3.0.0-rc.1", "v3.0.0-rc.3", true, "immediately next RC"},
		{"downgrade", "v3.0.0-rc.2", "v3.0.0-rc.1", true, "immediately next RC"},
		{"different release line", "v3.0.0-rc.2", "v3.1.0-rc.1", true, "same release line"},
		{"source unknown", "latest", "v3.0.0-rc.2", true, "cannot verify source RC"},
		{"malformed target", "v3.0.0-rc.1", "v3.0.0-rc.02", true, "invalid target RC"},
		{"malformed RC suffix", "v3.0.0-rc.1", "v3.0.0-rc2", true, "invalid target RC"},
		{"unfinished rollout", "v3.0.0-rc.1", "v3.0.0-rc.2", false, "rollout is complete"},
		{"accepted target", "v3.0.0-rc.2", "v3.0.0-rc.2", false, ""},
		{"non-RC target", "v3.0.0-rc.2", "v3.0.0", true, ""},
		{"non-RC change", "latest", "latest-new", true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sts := rcStatefulSet("ghcr.io/formancehq/ledger:"+tt.source, tt.converged)
			err := validateRCUpgrade(tt.target, sts)
			if tt.wantError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, tt.wantError)
			}
		})
	}
}

func TestValidateRCUpgradeUnknownImage(t *testing.T) {
	t.Parallel()

	require.NoError(t, validateRCUpgrade("v3.0.0-rc.2", nil))
	require.ErrorContains(t,
		validateRCUpgrade("v3.0.0-rc.2", rcStatefulSet("ghcr.io/formancehq/ledger@sha256:abc", true)),
		"cannot determine the running RC",
	)
	require.NoError(t,
		validateRCUpgrade("latest", rcStatefulSet("ghcr.io/formancehq/ledger@sha256:abc", true)),
	)
}

func TestReconcileRejectsSkippedRCBeforeChangingStatefulSet(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := newMinimalCluster()
	replicas := int32(3)
	cluster.Spec.Replicas = &replicas
	cluster.Spec.Image.Tag = "v3.0.0-rc.3"
	sts := controlledByCluster(rcStatefulSet("ghcr.io/formancehq/ledger:v3.0.0-rc.1", true), cluster)
	sts.Name = resourceName(cluster.Name)
	sts.Namespace = cluster.Namespace
	sts.CreationTimestamp = metav1.Now()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&ledgerv1alpha1.Cluster{}).
		WithObjects(cluster, sts).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	stsKey := types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}

	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Zero(t, result)
	require.NoError(t, c.Get(ctx, key, cluster))
	condition := meta.FindStatusCondition(cluster.Status.Conditions, "ConfigValid")
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Contains(t, condition.Message, "immediately next RC")
	require.Equal(t, "Error", cluster.Status.Phase)
	actual := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, stsKey, actual))
	require.Equal(t, sts.Spec, actual.Spec)
	require.Equal(t, sts.ResourceVersion, actual.ResourceVersion)
}

func TestReconcileRejectsSkippedRCWhenStatefulSetIsBeingRecreated(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := newMinimalCluster()
	replicas := int32(3)
	cluster.Spec.Replicas = &replicas
	cluster.Spec.Image.Tag = "v3.0.0-rc.3"
	orphan := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      resourceName(cluster.Name) + "-0",
			Namespace: cluster.Namespace,
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "ledger", Image: "ghcr.io/formancehq/ledger:v3.0.0-rc.1",
		}}},
	}
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&ledgerv1alpha1.Cluster{}).
		WithObjects(cluster, orphan).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme} // New reconciler after orphan deletion.
	key := types.NamespacedName{Name: cluster.Name, Namespace: cluster.Namespace}
	stsKey := types.NamespacedName{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}

	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Zero(t, result)
	require.NoError(t, c.Get(ctx, key, cluster))
	condition := meta.FindStatusCondition(cluster.Status.Conditions, "ConfigValid")
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Contains(t, condition.Message, "restore the source image tag first")
	require.Equal(t, "Error", cluster.Status.Phase)
	sts := &appsv1.StatefulSet{}
	require.Error(t, c.Get(ctx, stsKey, sts))

	cluster.Spec.Image.Tag = "v3.0.0-rc.1"
	require.NoError(t, c.Update(ctx, cluster))
	_, err = r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, stsKey, sts))
	require.Equal(t, "ghcr.io/formancehq/ledger:v3.0.0-rc.1", sts.Spec.Template.Spec.Containers[0].Image)
}

func TestMissingStatefulSetWithPVCKeepsRCSourceUnknown(t *testing.T) {
	t.Parallel()

	cluster := newMinimalCluster()
	cluster.Spec.Image.Tag = "v3.0.0-rc.3"
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: "wal-" + resourceName(cluster.Name) + "-0", Namespace: cluster.Namespace,
	}}
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(claim).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	require.ErrorContains(t, r.validateRCUpgradeWithoutStatefulSet(context.Background(), cluster), "cannot verify RC source")
}

func TestRCVolumeClaimRecreationWaitsForRollout(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	cluster := newMinimalCluster()
	cluster.Spec.Image.Repository = "ghcr.io/formancehq/ledger"
	cluster.Spec.Image.Tag = "v3.0.0-rc.2"
	applyDefaults(cluster)
	old := cluster.DeepCopy()
	old.Spec.Image.Tag = "v3.0.0-rc.1"
	old.Spec.Persistence.Data.HostPath = &ledgerv1alpha1.HostPathVolumeSpec{Path: "/mnt/data"}
	sts := rcStatefulSet("ghcr.io/formancehq/ledger:v3.0.0-rc.1", true)
	sts.Spec = buildStatefulSetSpec(old, "old", nil, "disabled")
	sts.Name = resourceName(cluster.Name)
	sts.Namespace = cluster.Namespace
	sts.CreationTimestamp = metav1.Now()
	controlledByCluster(sts, cluster)
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithStatusSubresource(&appsv1.StatefulSet{}).
		WithObjects(sts).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	key := types.NamespacedName{Name: sts.Name, Namespace: sts.Namespace}

	// The volume templates differ, but the first pass must publish rc.2
	// without orphaning the rc.1 Pods.
	_, err := r.reconcileStatefulSetWithExec(ctx, cluster, "hash", nil, nil)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, sts))
	require.Equal(t, "ghcr.io/formancehq/ledger:v3.0.0-rc.2", sts.Spec.Template.Spec.Containers[0].Image)
	require.Len(t, sts.Spec.VolumeClaimTemplates, 1)
	require.Equal(t, "wal", sts.Spec.VolumeClaimTemplates[0].Name)
	require.NotNil(t, storageVolume(sts.Spec.Template.Spec.Volumes, "data"))
	require.Equal(t, "$(POD_INDEX)", storageMount(sts.Spec.Template.Spec.Containers[0].VolumeMounts, "data").SubPathExpr)

	// An interrupted mixed-image rollout must not delete the StatefulSet.
	sts.Status.ObservedGeneration = sts.Generation
	sts.Status.UpdatedReplicas = 2
	sts.Status.ReadyReplicas = 2
	require.NoError(t, c.Status().Update(ctx, sts))
	r = &ClusterReconciler{Client: c, Scheme: scheme} // Restart the controller.
	_, err = r.reconcileStatefulSetWithExec(ctx, cluster, "hash", nil, nil)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, sts))
	require.Equal(t, "ghcr.io/formancehq/ledger:v3.0.0-rc.2", sts.Spec.Template.Spec.Containers[0].Image)

	// Only after all Pods use rc.2 may the volume-template path orphan them.
	sts.Status.ObservedGeneration = sts.Generation
	sts.Status.UpdatedReplicas = 3
	sts.Status.ReadyReplicas = 3
	require.NoError(t, c.Status().Update(ctx, sts))
	_, err = r.reconcileStatefulSetWithExec(ctx, cluster, "hash", nil, nil)
	require.NoError(t, err)
	require.True(t, apierrors.IsNotFound(c.Get(ctx, key, sts)))
}

func storageVolume(volumes []corev1.Volume, name string) *corev1.Volume {
	for i := range volumes {
		if volumes[i].Name == name {
			return &volumes[i]
		}
	}

	return nil
}

func storageMount(mounts []corev1.VolumeMount, name string) *corev1.VolumeMount {
	for i := range mounts {
		if mounts[i].Name == name {
			return &mounts[i]
		}
	}

	return nil
}
