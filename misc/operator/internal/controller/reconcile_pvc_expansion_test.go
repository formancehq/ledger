package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
)

// vct builds a volumeClaimTemplate carrying only the storage request the
// expansion pass compares on.
func vct(name, size string) corev1.PersistentVolumeClaim {
	return corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			},
		},
	}
}

func volPVCName(vol string, ordinal int) string {
	return vol + "-" + testStsName + "-" + string(rune('0'+ordinal))
}

// volPVC builds a PVC for the given volume/ordinal at a size, mirroring the
// StatefulSet naming convention <volume>-<stsName>-<ordinal>.
func volPVC(vol string, ordinal int, namespace, size string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: volPVCName(vol, ordinal), Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			},
		},
	}
}

func dataPVC(ordinal int, namespace, size string) *corev1.PersistentVolumeClaim {
	return volPVC("data", ordinal, namespace, size)
}

func volStorage(t *testing.T, cs *fake.Clientset, vol string, ordinal int) *resource.Quantity {
	t.Helper()
	got, err := cs.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), volPVCName(vol, ordinal), metav1.GetOptions{})
	require.NoError(t, err)

	return got.Spec.Resources.Requests.Storage()
}

// requireQuantity asserts a quantity equals the parsed want string.
func requireQuantity(t *testing.T, got *resource.Quantity, want string) {
	t.Helper()
	w := resource.MustParse(want)
	require.Equal(t, 0, got.Cmp(w), "got %s, want %s", got.String(), want)
}

func TestReconcilePVCExpansion_GrowsAllPVCsAndSignalsRecreate(t *testing.T) {
	t.Parallel()

	cs := fake.NewClientset(dataPVC(0, "ns", "2Ti"), dataPVC(1, "ns", "2Ti"), dataPVC(2, "ns", "2Ti"))

	grew, err := reconcilePVCExpansion(context.Background(), cs, nil, nil, "ns", testStsName, 3,
		[]corev1.PersistentVolumeClaim{vct("data", "2Ti")},
		[]corev1.PersistentVolumeClaim{vct("data", "3Ti")},
	)
	require.NoError(t, err)
	require.True(t, grew, "a template size increase must signal a StatefulSet recreate")

	for ordinal := range 3 {
		requireQuantity(t, volStorage(t, cs, "data", ordinal), "3Ti")
	}
}

func TestReconcilePVCExpansion_NoChangeIsNoop(t *testing.T) {
	t.Parallel()

	cs := fake.NewClientset(dataPVC(0, "ns", "2Ti"))
	patches := countPatches(cs, "persistentvolumeclaims")

	grew, err := reconcilePVCExpansion(context.Background(), cs, nil, nil, "ns", testStsName, 1,
		[]corev1.PersistentVolumeClaim{vct("data", "2Ti")},
		[]corev1.PersistentVolumeClaim{vct("data", "2Ti")},
	)
	require.NoError(t, err)
	require.False(t, grew)
	require.Zero(t, *patches, "equal sizes must not patch any PVC")
}

func TestReconcilePVCExpansion_ShrinkIsRejectedAndClampedToLiveSize(t *testing.T) {
	t.Parallel()

	cs := fake.NewClientset(dataPVC(0, "ns", "3Ti"))
	patches := countPatches(cs, "persistentvolumeclaims")
	rec := record.NewFakeRecorder(4)
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "ns"}}

	desired := []corev1.PersistentVolumeClaim{vct("data", "2Ti")}
	grew, err := reconcilePVCExpansion(context.Background(), cs, rec, obj, "ns", testStsName, 1,
		[]corev1.PersistentVolumeClaim{vct("data", "3Ti")}, desired)
	require.NoError(t, err)
	require.False(t, grew, "a shrink must not trigger a recreate")
	require.Zero(t, *patches, "a shrink must never patch the PVC")

	// The desired template is clamped back up to the live disk size in place.
	requireQuantity(t, desired[0].Spec.Resources.Requests.Storage(), "3Ti")
	requireQuantity(t, volStorage(t, cs, "data", 0), "3Ti")

	select {
	case ev := <-rec.Events:
		require.Contains(t, ev, "VolumeShrinkRejected")
	default:
		t.Fatal("expected a VolumeShrinkRejected event")
	}
}

func TestReconcilePVCExpansion_MissingPVCSkipped(t *testing.T) {
	t.Parallel()

	// Only ordinal 0 exists; ordinals 1 and 2 are not yet scaled out.
	cs := fake.NewClientset(dataPVC(0, "ns", "2Ti"))

	grew, err := reconcilePVCExpansion(context.Background(), cs, nil, nil, "ns", testStsName, 3,
		[]corev1.PersistentVolumeClaim{vct("data", "2Ti")},
		[]corev1.PersistentVolumeClaim{vct("data", "3Ti")},
	)
	require.NoError(t, err)
	require.True(t, grew)

	requireQuantity(t, volStorage(t, cs, "data", 0), "3Ti")
	// Missing ordinals must not error and must not be created by this pass.
	_, err = cs.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), volPVCName("data", 1), metav1.GetOptions{})
	require.Error(t, err, "the expansion pass must not create absent PVCs")
}

func TestReconcilePVCExpansion_SpecBelowLargestLiveDiskClampsUp(t *testing.T) {
	t.Parallel()

	// A PVC sits at 4Ti (e.g. resized out of band) while the template was 2Ti and
	// the new spec asks for 3Ti. 3Ti is below the live disk, so it is a shrink
	// relative to what exists: clamp the template up to 4Ti, warn, never patch the
	// 4Ti PVC down. templateGrew because 4Ti > the old 2Ti template.
	cs := fake.NewClientset(dataPVC(0, "ns", "4Ti"))
	patches := countPatches(cs, "persistentvolumeclaims")
	rec := record.NewFakeRecorder(4)
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "ns"}}

	desired := []corev1.PersistentVolumeClaim{vct("data", "3Ti")}
	grew, err := reconcilePVCExpansion(context.Background(), cs, rec, obj, "ns", testStsName, 1,
		[]corev1.PersistentVolumeClaim{vct("data", "2Ti")}, desired)
	require.NoError(t, err)
	require.True(t, grew, "clamped template 4Ti exceeds the old 2Ti template")
	require.Zero(t, *patches, "a PVC already at/above the desired size must not be patched")

	requireQuantity(t, desired[0].Spec.Resources.Requests.Storage(), "4Ti")
	requireQuantity(t, volStorage(t, cs, "data", 0), "4Ti")
	require.Len(t, rec.Events, 1, "clamping below a live disk warns once")
}

func TestReconcilePVCExpansion_ShrinkWarnsOncePerVolume(t *testing.T) {
	t.Parallel()

	// A spec shrink across all three ordinals must warn exactly once for the
	// volume, not once per PVC.
	cs := fake.NewClientset(dataPVC(0, "ns", "3Ti"), dataPVC(1, "ns", "3Ti"), dataPVC(2, "ns", "3Ti"))
	rec := record.NewFakeRecorder(8)
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "ns"}}

	grew, err := reconcilePVCExpansion(context.Background(), cs, rec, obj, "ns", testStsName, 3,
		[]corev1.PersistentVolumeClaim{vct("data", "3Ti")},
		[]corev1.PersistentVolumeClaim{vct("data", "2Ti")},
	)
	require.NoError(t, err)
	require.False(t, grew)
	require.Len(t, rec.Events, 1, "shrink must warn exactly once per volume, not per PVC")
}

// TestReconcilePVCExpansion_MixedGrowShrinkDoesNotLeakShrink is the regression
// for the mixed grow/shrink case: growing one volume forces a StatefulSet
// recreate that rebuilds every template from spec, so the rejected shrink on the
// other volume must be clamped back to its live size and must not leak.
func TestReconcilePVCExpansion_MixedGrowShrinkDoesNotLeakShrink(t *testing.T) {
	t.Parallel()

	cs := fake.NewClientset(dataPVC(0, "ns", "2Ti"), volPVC("wal", 0, "ns", "5Gi"))
	rec := record.NewFakeRecorder(8)
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "ns"}}

	desired := []corev1.PersistentVolumeClaim{vct("data", "3Ti"), vct("wal", "4Gi")}
	grew, err := reconcilePVCExpansion(context.Background(), cs, rec, obj, "ns", testStsName, 1,
		[]corev1.PersistentVolumeClaim{vct("data", "2Ti"), vct("wal", "5Gi")}, desired)
	require.NoError(t, err)
	require.True(t, grew, "the data grow must still signal a recreate")

	// data grows; wal is clamped back to its live 5Gi and must NOT carry the
	// requested 4Gi into the (about to be recreated) template.
	requireQuantity(t, desired[0].Spec.Resources.Requests.Storage(), "3Ti")
	requireQuantity(t, desired[1].Spec.Resources.Requests.Storage(), "5Gi")
	requireQuantity(t, volStorage(t, cs, "data", 0), "3Ti")
	requireQuantity(t, volStorage(t, cs, "wal", 0), "5Gi")
	require.Len(t, rec.Events, 2, "one VolumeExpanded for data, one VolumeShrinkRejected for wal")
}

// TestReconcilePVCExpansion_ClampSurvivesRecreatePath models the reconcile right
// after the orphan delete: the StatefulSet is gone (existingTemplates nil) but
// the live PVC persists, so the clamp still floors the rebuilt template.
func TestReconcilePVCExpansion_ClampSurvivesRecreatePath(t *testing.T) {
	t.Parallel()

	cs := fake.NewClientset(volPVC("wal", 0, "ns", "5Gi"))
	rec := record.NewFakeRecorder(4)
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "ns"}}

	desired := []corev1.PersistentVolumeClaim{vct("wal", "4Gi")}
	grew, err := reconcilePVCExpansion(context.Background(), cs, rec, obj, "ns", testStsName, 1,
		nil, desired)
	require.NoError(t, err)
	require.False(t, grew, "no existing template to compare against on the create path")

	requireQuantity(t, desired[0].Spec.Resources.Requests.Storage(), "5Gi")
	require.Len(t, rec.Events, 1, "clamp still warns on the recreate path")
}
