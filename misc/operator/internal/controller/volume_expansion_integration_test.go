//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestVolumeExpansionReconcilerPatchesPVCInEnvtest(t *testing.T) {
	namespace := createTestNamespace(t)
	allowExpansion := true
	storageClassName := "expandable-" + namespace
	storageClass := &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: storageClassName},
		Provisioner:          "ebs.csi.aws.com",
		AllowVolumeExpansion: &allowExpansion,
	}
	require.NoError(t, k8sClient.Create(ctx, storageClass))
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, storageClass) }) //nolint:errcheck // best-effort cleanup

	replicas := int32(1)
	maximum := resource.MustParse("200Gi")
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "auto-expand", Namespace: namespace},
		Spec: ledgerv1alpha1.ClusterSpec{
			Replicas: &replicas,
			Persistence: ledgerv1alpha1.PersistenceSpec{
				Data: ledgerv1alpha1.VolumeSpec{
					Size:         resource.MustParse("100Gi"),
					StorageClass: storageClassName,
					AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{
						Enabled:     true,
						MaximumSize: &maximum,
					},
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, cluster))

	request := resource.MustParse("100Gi")
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data-ledger-auto-expand-0", Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &storageClassName,
			VolumeName:       "pv-auto-expand",
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: request},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, pvc))
	pvc.Status.Phase = corev1.ClaimBound
	pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: request}
	require.NoError(t, k8sClient.Status().Update(ctx, pvc))

	// The volume reconciler watches Cluster objects, not PVCs. Wait until the
	// primary reconciler has created the live StatefulSet, then enqueue one
	// deterministic Cluster update after the PVC is Bound.
	requireEventually(t, func() bool {
		var statefulSet appsv1.StatefulSet

		return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "ledger-auto-expand"}, &statefulSet) == nil
	}, "StatefulSet should exist before triggering volume expansion")
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: cluster.Name}, cluster))
	if cluster.Annotations == nil {
		cluster.Annotations = map[string]string{}
	}
	cluster.Annotations["ledger.formance.com/test-volume-expansion-trigger"] = "ready"
	require.NoError(t, k8sClient.Update(ctx, cluster))

	requireEventually(t, func() bool {
		var current corev1.PersistentVolumeClaim
		if err := k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: pvc.Name}, &current); err != nil {
			return false
		}
		requested := current.Spec.Resources.Requests[corev1.ResourceStorage]

		return requested.Cmp(resource.MustParse("146Gi")) == 0 &&
			current.Annotations[annotationLastExpansionTarget] == "146Gi"
	}, "PVC should be expanded after the explicit Cluster reconcile")
}
