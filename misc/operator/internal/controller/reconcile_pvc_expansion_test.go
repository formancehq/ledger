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

// dataPVC builds a bound-looking data PVC for the given ordinal at a size.
func dataPVC(ordinal int, namespace, size string) *corev1.PersistentVolumeClaim {
	pvc := dataPVCName(ordinal)

	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvc, Namespace: namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse(size)},
			},
		},
	}
}

func dataPVCName(ordinal int) string {
	return "data-" + testStsName + "-" + string(rune('0'+ordinal))
}

func pvcStorage(t *testing.T, cs *fake.Clientset, ordinal int) *resource.Quantity {
	t.Helper()
	got, err := cs.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), dataPVCName(ordinal), metav1.GetOptions{})
	require.NoError(t, err)

	return got.Spec.Resources.Requests.Storage()
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

	want := resource.MustParse("3Ti")
	for ordinal := range 3 {
		require.Equal(t, 0, pvcStorage(t, cs, ordinal).Cmp(want),
			"PVC ordinal %d must be grown to 3Ti", ordinal)
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

func TestReconcilePVCExpansion_ShrinkIsRejectedAndPVCUntouched(t *testing.T) {
	t.Parallel()

	cs := fake.NewClientset(dataPVC(0, "ns", "3Ti"))
	patches := countPatches(cs, "persistentvolumeclaims")
	rec := record.NewFakeRecorder(4)
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "ns"}}

	grew, err := reconcilePVCExpansion(context.Background(), cs, rec, obj, "ns", testStsName, 1,
		[]corev1.PersistentVolumeClaim{vct("data", "3Ti")},
		[]corev1.PersistentVolumeClaim{vct("data", "2Ti")},
	)
	require.NoError(t, err)
	require.False(t, grew, "a shrink must not trigger a recreate")
	require.Zero(t, *patches, "a shrink must never patch the PVC")

	want := resource.MustParse("3Ti")
	require.Equal(t, 0, pvcStorage(t, cs, 0).Cmp(want), "PVC must be left at its original size")

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

	want := resource.MustParse("3Ti")
	require.Equal(t, 0, pvcStorage(t, cs, 0).Cmp(want), "existing PVC is grown")
	// Missing ordinals must not error and must not be created by this pass.
	_, err = cs.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), dataPVCName(1), metav1.GetOptions{})
	require.Error(t, err, "the expansion pass must not create absent PVCs")
}

func TestReconcilePVCExpansion_GrowOnlyPVCAlreadyLargerLeftAlone(t *testing.T) {
	t.Parallel()

	// PVC already manually grown past the template — must never be shrunk to it,
	// and (crucially) must not be misread as a shrink request: template grew
	// 2Ti->3Ti but the live PVC sits at 4Ti, above the new desired size.
	cs := fake.NewClientset(dataPVC(0, "ns", "4Ti"))
	patches := countPatches(cs, "persistentvolumeclaims")
	rec := record.NewFakeRecorder(4)
	obj := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "ls", Namespace: "ns"}}

	grew, err := reconcilePVCExpansion(context.Background(), cs, rec, obj, "ns", testStsName, 1,
		[]corev1.PersistentVolumeClaim{vct("data", "2Ti")},
		[]corev1.PersistentVolumeClaim{vct("data", "3Ti")},
	)
	require.NoError(t, err)
	require.True(t, grew, "template still grew 2Ti->3Ti so the template must refresh")
	require.Zero(t, *patches, "a PVC already larger than desired must not be patched")

	want := resource.MustParse("4Ti")
	require.Equal(t, 0, pvcStorage(t, cs, 0).Cmp(want))

	select {
	case ev := <-rec.Events:
		t.Fatalf("an over-expanded PVC must not emit any event, got %q", ev)
	default:
	}
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
