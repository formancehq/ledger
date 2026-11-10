//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestPersistenceStorageFloorPatchesPVCInEnvtest(t *testing.T) {
	namespace := createTestNamespace(t)
	className := "floor-" + namespace
	class := &storagev1.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: className}, Provisioner: "example.csi", AllowVolumeExpansion: new(true)}
	require.NoError(t, k8sClient.Create(ctx, class))
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, class) }) // best-effort cleanup
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "explicit-floor", Namespace: namespace},
		Spec: ledgerv1alpha1.ClusterSpec{Replicas: new(int32(1)), Persistence: ledgerv1alpha1.PersistenceSpec{
			WAL:  ledgerv1alpha1.VolumeSpec{HostPath: &ledgerv1alpha1.HostPathVolumeSpec{Path: "/data/wal"}},
			Data: ledgerv1alpha1.VolumeSpec{Size: resource.MustParse("150Gi"), StorageClass: className},
		}},
	}
	require.NoError(t, k8sClient.Create(ctx, cluster))
	var sts appsv1.StatefulSet
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "ledger-explicit-floor"}, &sts) == nil
	}, "StatefulSet should exist before explicit storage reconciliation")
	stsUID := sts.UID
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data-ledger-explicit-floor-0", Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &className, VolumeName: "pv-" + namespace,
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources:   corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("100Gi")}},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, pvc))
	pvcUID := pvc.UID
	pvc.Status.Phase = corev1.ClaimBound
	pvc.Status.Capacity = corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("100Gi")}
	require.NoError(t, k8sClient.Status().Update(ctx, pvc))
	require.NoError(t, k8sClient.Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "ledger-explicit-floor-0", Namespace: namespace, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(&sts, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))}},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ledger", Image: "test"}},
			Volumes:    []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name}}}},
		},
	}))
	// The manually provisioned retained claim is below the configured floor.
	// A Cluster event drives the real uncached-reader reconciliation path.
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster))
	beforeTrigger := cluster.DeepCopy()
	if cluster.Annotations == nil {
		cluster.Annotations = make(map[string]string)
	}
	cluster.Annotations["ledger.formance.com/test-storage-trigger"] = "ready"
	// Patch only this annotation without a resourceVersion precondition: cached
	// reads can lag the continuously changing status even across conflict retries.
	// Other annotations and every spec/status field remain server-owned.
	require.NoError(t, k8sClient.Patch(ctx, cluster, client.MergeFrom(beforeTrigger)))
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(pvc), pvc); err != nil {
			return false
		}
		request := pvc.Spec.Resources.Requests[corev1.ResourceStorage]

		return request.Cmp(resource.MustParse("150Gi")) == 0
	}, "explicit storage floor should grow the retained claim without auto-expansion")
	require.Equal(t, pvcUID, pvc.UID)
	require.Empty(t, pvc.OwnerReferences, "Retain semantics must remain unchanged")
	require.Equal(t, "pv-"+namespace, pvc.Spec.VolumeName)
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(cluster), cluster); err != nil {
			return false
		}
		condition := meta.FindStatusCondition(cluster.Status.Conditions, dataStorageCondition)

		return condition != nil && condition.Reason == "StorageUpdatePending"
	}, "capacity lag should remain visible after the resize request is accepted")
	require.NoError(t, k8sClient.Get(ctx, client.ObjectKeyFromObject(&sts), &sts))
	require.Equal(t, stsUID, sts.UID)
}

func TestPersistenceStableVolumeAttributesAPIInEnvtest(t *testing.T) {
	namespace := createTestNamespace(t)
	class := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "storage.k8s.io/v1", "kind": "VolumeAttributesClass",
		"metadata":   map[string]any{"name": "stable-" + namespace},
		"driverName": "example.csi", "parameters": map[string]any{"iops": "5000"},
	}}
	if err := k8sClient.Create(ctx, class); meta.IsNoMatchError(err) || k8serrors.IsNotFound(err) {
		t.Skip("Kubernetes does not serve the stable VolumeAttributesClass API")
	} else {
		require.NoError(t, err)
	}
	t.Cleanup(func() { _ = k8sClient.Delete(ctx, class) }) // best-effort cleanup
	r := &persistenceStorageReconciler{reader: k8sClient}
	driver, err := r.volumeAttributesDriver(ctx, class.GetName())
	require.NoError(t, err)
	require.Equal(t, "example.csi", driver)
}
