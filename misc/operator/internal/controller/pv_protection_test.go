package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
)

// stsName mirrors the prefixed StatefulSet name the reconciler passes
// (resourceName(ledger.Name)); the PVC names derive from it as
// <volume>-<stsName>-<ordinal>.
const (
	testStsName = "ledger-my-ledger"
	testPVCName = "data-ledger-my-ledger-0"
)

func boundPVCAndPV(pvcName, pvName, namespace string) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: pvcName, Namespace: namespace},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: pvName},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: namespace, Name: pvcName},
		},
	}

	return pvc, pv
}

func countPatches(cs *fake.Clientset, resource string) *int {
	n := 0
	cs.PrependReactor("patch", resource, func(clienttesting.Action) (bool, runtime.Object, error) {
		n++

		return false, nil, nil
	})

	return &n
}

func TestReconcileVolumeProtection_StampsBoundPVCAndPV(t *testing.T) {
	t.Parallel()

	boundPVC, boundPV := boundPVCAndPV(testPVCName, "pv-0", "ns")
	cs := fake.NewClientset(boundPVC, boundPV)

	pending, err := reconcileVolumeProtection(context.Background(), cs, "ns", testStsName, 1, []string{"data"}, true)
	require.NoError(t, err)
	require.False(t, pending, "a bound PVC leaves nothing pending")

	gotPVC, err := cs.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, labelDeletionProtectionValue, gotPVC.Labels[labelDeletionProtection])

	gotPV, err := cs.CoreV1().PersistentVolumes().Get(context.Background(), "pv-0", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, labelDeletionProtectionValue, gotPV.Labels[labelDeletionProtection])
}

func TestReconcileVolumeProtection_UnstampsWhenDisabled(t *testing.T) {
	t.Parallel()

	boundPVC, boundPV := boundPVCAndPV(testPVCName, "pv-0", "ns")
	boundPVC.Labels = map[string]string{labelDeletionProtection: labelDeletionProtectionValue}
	boundPV.Labels = map[string]string{labelDeletionProtection: labelDeletionProtectionValue}
	cs := fake.NewClientset(boundPVC, boundPV)

	pending, err := reconcileVolumeProtection(context.Background(), cs, "ns", testStsName, 1, []string{"data"}, false)
	require.NoError(t, err)
	require.False(t, pending, "disabling protection never requeues")

	gotPVC, err := cs.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, gotPVC.Labels, labelDeletionProtection)

	gotPV, err := cs.CoreV1().PersistentVolumes().Get(context.Background(), "pv-0", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, gotPV.Labels, labelDeletionProtection)
}

func TestReconcileVolumeProtection_SkipsPVBoundToDifferentClaim(t *testing.T) {
	t.Parallel()

	boundPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: testPVCName, Namespace: "ns"},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-0"},
	}
	// PV reused by an unrelated claim: must not inherit ledger protection labels.
	rebound := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pv-0"},
		Spec: corev1.PersistentVolumeSpec{
			ClaimRef: &corev1.ObjectReference{Namespace: "other", Name: "someone-else-0"},
		},
	}
	cs := fake.NewClientset(boundPVC, rebound)

	pending, err := reconcileVolumeProtection(context.Background(), cs, "ns", testStsName, 1, []string{"data"}, true)
	require.NoError(t, err)
	require.False(t, pending, "a bound PVC leaves nothing pending even when its PV is rebound elsewhere")

	gotPV, err := cs.CoreV1().PersistentVolumes().Get(context.Background(), "pv-0", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, gotPV.Labels, labelDeletionProtection)
}

func TestReconcileVolumeProtection_NoOpWhenAlreadyInDesiredState(t *testing.T) {
	t.Parallel()

	boundPVC, boundPV := boundPVCAndPV(testPVCName, "pv-0", "ns")
	boundPVC.Labels = map[string]string{labelDeletionProtection: labelDeletionProtectionValue}
	boundPV.Labels = map[string]string{labelDeletionProtection: labelDeletionProtectionValue}
	cs := fake.NewClientset(boundPVC, boundPV)

	pvcPatches := countPatches(cs, "persistentvolumeclaims")
	pvPatches := countPatches(cs, "persistentvolumes")

	pending, err := reconcileVolumeProtection(context.Background(), cs, "ns", testStsName, 1, []string{"data"}, true)
	require.NoError(t, err)
	require.False(t, pending)
	require.Zero(t, *pvcPatches, "already-stamped PVC must not be patched")
	require.Zero(t, *pvPatches, "already-stamped PV must not be patched")
}

func TestReconcileVolumeProtection_NoOpWhenDisabledAndUnlabeled(t *testing.T) {
	t.Parallel()

	boundPVC, boundPV := boundPVCAndPV(testPVCName, "pv-0", "ns")
	cs := fake.NewClientset(boundPVC, boundPV)

	pvcPatches := countPatches(cs, "persistentvolumeclaims")
	pvPatches := countPatches(cs, "persistentvolumes")

	pending, err := reconcileVolumeProtection(context.Background(), cs, "ns", testStsName, 1, []string{"data"}, false)
	require.NoError(t, err)
	require.False(t, pending)
	require.Zero(t, *pvcPatches, "unlabeled PVC must not be patched when protection is off")
	require.Zero(t, *pvPatches, "unlabeled PV must not be patched when protection is off")
}

func TestReconcileVolumeProtection_StampsUnboundPVCAndReportsPending(t *testing.T) {
	t.Parallel()

	unboundPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: testPVCName, Namespace: "ns"},
		// no Spec.VolumeName -> not yet bound
	}
	cs := fake.NewClientset(unboundPVC)

	// ordinal 0 PVC is unbound (PVC stamped, PV skipped); ordinal 1 PVC does not
	// exist yet -> both leave a PV to stamp once they bind, so pending is true.
	pending, err := reconcileVolumeProtection(context.Background(), cs, "ns", testStsName, 2, []string{"data"}, true)
	require.NoError(t, err)
	require.True(t, pending, "an unbound/absent PVC under protection must requeue")

	got, err := cs.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), testPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, labelDeletionProtectionValue, got.Labels[labelDeletionProtection])
}

func TestReconcileVolumeProtection_UnboundPVCNotPendingWhenDisabled(t *testing.T) {
	t.Parallel()

	unboundPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: testPVCName, Namespace: "ns"},
	}
	cs := fake.NewClientset(unboundPVC)

	// replicas=1 so the only ordinal is the existing unbound PVC (no absent ordinal).
	// Protection off: its future PV is born without our label (the provisioner does
	// not copy PVC labels), so there is nothing to unstamp and no need to requeue.
	pending, err := reconcileVolumeProtection(context.Background(), cs, "ns", testStsName, 1, []string{"data"}, false)
	require.NoError(t, err)
	require.False(t, pending, "with protection off an unbound PVC must not requeue")
}

func TestReconcileVolumeProtection_RequeuesAbsentPVCWhenDisabled(t *testing.T) {
	t.Parallel()

	// No PVC exists yet. With protection off we must still requeue: a PVC born from
	// a still-labeled (immutable) VCT after an opt-out scale-out has to be unstamped,
	// otherwise it stays selected by the policy and its deletion is wrongly blocked.
	cs := fake.NewClientset()

	pending, err := reconcileVolumeProtection(context.Background(), cs, "ns", testStsName, 1, []string{"data"}, false)
	require.NoError(t, err)
	require.True(t, pending, "an absent PVC must requeue even when protection is off, to unstamp stale-VCT scale-out PVCs")
}
