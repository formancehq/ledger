//go:build integration

package controller

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// createExpandableStorageClass creates a StorageClass with volume expansion
// enabled. The PersistentVolumeClaimResize admission plugin only accepts
// storage growth on PVCs whose class allows expansion.
func createExpandableStorageClass(t *testing.T, name string) {
	t.Helper()
	allow := true
	sc := &storagev1.StorageClass{
		ObjectMeta:           metav1.ObjectMeta{Name: name},
		Provisioner:          "test.ledger.formance.com/fake",
		AllowVolumeExpansion: &allow,
	}
	require.NoError(t, k8sClient.Create(ctx, sc))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, sc)
	})
}

// createBoundPVC simulates the PVC the StatefulSet controller (absent from
// envtest) would have provisioned for an ordinal, then marks it Bound — the
// resize admission plugin rejects growth on unbound claims.
func createBoundPVC(t *testing.T, namespace, name, storageClass, size string) {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &storageClass,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: resource.MustParse(size),
				},
			},
		},
	}
	require.NoError(t, k8sClient.Create(ctx, pvc))
	pvc.Status.Phase = corev1.ClaimBound
	pvc.Status.Capacity = corev1.ResourceList{
		corev1.ResourceStorage: resource.MustParse(size),
	}
	require.NoError(t, k8sClient.Status().Update(ctx, pvc))
}

func TestReconcile_DataVolumeSizeGrow(t *testing.T) {
	ns := createTestNamespace(t)
	sc := "expandable-" + ns
	createExpandableStorageClass(t, sc)

	ls := newCluster("pvc-grow", ns)
	ls.Spec.Persistence.Data.StorageClass = sc
	ls.Spec.Persistence.Data.Size = resource.MustParse("1Gi")
	require.NoError(t, k8sClient.Create(ctx, ls))

	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-pvc-grow", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")
	initialUID := sts.UID

	for i := range 3 {
		createBoundPVC(t, ns, fmt.Sprintf("data-ledger-pvc-grow-%d", i), sc, "1Gi")
	}

	updated := &ledgerv1alpha1.Cluster{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "pvc-grow", Namespace: ns}, updated))
	updated.Spec.Persistence.Data.Size = resource.MustParse("2Gi")
	require.NoError(t, k8sClient.Update(ctx, updated))

	want := resource.MustParse("2Gi")
	for i := range 3 {
		name := fmt.Sprintf("data-ledger-pvc-grow-%d", i)
		requireEventually(t, func() bool {
			pvc := &corev1.PersistentVolumeClaim{}
			if err := k8sClient.Get(ctx, types.NamespacedName{Name: name, Namespace: ns}, pvc); err != nil {
				return false
			}
			got := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
			return got.Cmp(want) == 0
		}, "PVC %s should be grown to 2Gi", name)
	}

	// envtest runs no garbage collector, so the orphan finalizer set by
	// DeletePropagationOrphan never clears; strip it once the deletion is
	// underway, as the GC controller would in a real cluster.
	requireEventually(t, func() bool {
		current := &appsv1.StatefulSet{}
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-pvc-grow", Namespace: ns}, current); err != nil {
			return apierrors.IsNotFound(err)
		}
		if current.UID != initialUID {
			return true
		}
		if current.DeletionTimestamp.IsZero() {
			return false
		}
		if len(current.Finalizers) > 0 {
			patch := client.MergeFrom(current.DeepCopy())
			current.Finalizers = nil
			_ = k8sClient.Patch(ctx, current, patch)
		}
		return false
	}, "StatefulSet should be deleted with orphan propagation")

	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-pvc-grow", Namespace: ns}, sts); err != nil {
			return false
		}
		if sts.UID == initialUID {
			return false
		}
		for _, tmpl := range sts.Spec.VolumeClaimTemplates {
			if tmpl.Name == "data" {
				got := tmpl.Spec.Resources.Requests[corev1.ResourceStorage]
				return got.Cmp(want) == 0
			}
		}
		return false
	}, "StatefulSet should be recreated with the grown data template")
}

func TestReconcile_DataVolumeSizeShrinkIgnored(t *testing.T) {
	ns := createTestNamespace(t)
	ls := newCluster("pvc-shrink", ns)
	ls.Spec.Persistence.Data.Size = resource.MustParse("2Gi")
	require.NoError(t, k8sClient.Create(ctx, ls))

	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-pvc-shrink", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")
	initialUID := sts.UID

	updated := &ledgerv1alpha1.Cluster{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "pvc-shrink", Namespace: ns}, updated))
	updated.Spec.Persistence.Data.Size = resource.MustParse("1Gi")
	require.NoError(t, k8sClient.Update(ctx, updated))

	requireEventually(t, func() bool {
		events := &corev1.EventList{}
		if err := k8sClient.List(ctx, events, client.InNamespace(ns)); err != nil {
			return false
		}
		for _, event := range events.Items {
			if event.Reason == "VolumeShrinkIgnored" && event.InvolvedObject.Name == "pvc-shrink" {
				return true
			}
		}
		return false
	}, "shrinking a volume should emit a VolumeShrinkIgnored warning event")

	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-pvc-shrink", Namespace: ns}, sts))
	assert.Equal(t, initialUID, sts.UID, "StatefulSet should not be recreated on shrink")
	for _, tmpl := range sts.Spec.VolumeClaimTemplates {
		if tmpl.Name == "data" {
			got := tmpl.Spec.Resources.Requests[corev1.ResourceStorage]
			want := resource.MustParse("2Gi")
			assert.Zero(t, got.Cmp(want), "data template should keep its original size")
		}
	}
}
