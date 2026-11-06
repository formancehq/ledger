package controller

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

// Supply API-server identities explicitly: the typed fake does not allocate or
// enforce them. Normal reconciliation tests above intentionally keep its model.
func retainedProtectionPair() (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume, map[string]retainedClaimIdentity) {
	pvc, pv := boundPVCAndPV(testPVCName, "pv-0", "ns")
	pvc.UID, pvc.ResourceVersion = "saved-pvc", "11"
	pvc.Status.Phase = corev1.ClaimBound
	pv.UID, pv.ResourceVersion = "saved-pv", "22"
	pv.Spec.ClaimRef.UID = pvc.UID

	return pvc, pv, map[string]retainedClaimIdentity{pvc.Name: {
		Name: pvc.Name, UID: pvc.UID, VolumeName: pv.Name, PVUID: pv.UID,
	}}
}

func TestRecoveryVolumeProtectionRejectsChangedIdentitiesBeforeWrites(t *testing.T) {
	t.Parallel()
	for _, protect := range []bool{true, false} {
		for _, tt := range []struct {
			name string
			edit func(*corev1.PersistentVolumeClaim, *corev1.PersistentVolume)
			want string
		}{
			{"replaced PVC", func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) { pvc.UID = "replacement" }, "retained PVC"},
			{"deleting PVC", func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) {
				now := metav1.Now()
				pvc.DeletionTimestamp = &now
			}, "retained PVC"},
			{"rebound PVC", func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) { pvc.Spec.VolumeName = "other-pv" }, "retained PVC"},
			{"unbound PVC", func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) {
				pvc.Status.Phase = corev1.ClaimPending
			}, "retained PVC"},
			{"PVC without version", func(pvc *corev1.PersistentVolumeClaim, _ *corev1.PersistentVolume) { pvc.ResourceVersion = "" }, "retained PVC"},
			{"replaced PV", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) { pv.UID = "replacement" }, "retained PV"},
			{"deleting PV", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
				now := metav1.Now()
				pv.DeletionTimestamp = &now
			}, "retained PV"},
			{"PV without version", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) { pv.ResourceVersion = "" }, "retained PV"},
			{"PV claim UID", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
				pv.Spec.ClaimRef.UID = "other-claim"
			}, "retained PV"},
			{"PV claim name", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
				pv.Spec.ClaimRef.Name = "other-claim"
			}, "retained PV"},
			{"PV claim namespace", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) {
				pv.Spec.ClaimRef.Namespace = "other"
			}, "retained PV"},
			{"PV without claim", func(_ *corev1.PersistentVolumeClaim, pv *corev1.PersistentVolume) { pv.Spec.ClaimRef = nil }, "retained PV"},
		} {
			t.Run(tt.name+"/"+strconv.FormatBool(protect), func(t *testing.T) {
				t.Parallel()
				pvc, pv, inventory := retainedProtectionPair()
				pvc.Labels, pv.Labels = map[string]string{"keep": "pvc"}, map[string]string{"keep": "pv"}
				if !protect {
					pvc.Labels[labelDeletionProtection], pv.Labels[labelDeletionProtection] = labelDeletionProtectionValue, labelDeletionProtectionValue
				}
				tt.edit(pvc, pv)
				cs := fake.NewClientset(pvc, pv)
				pvcPatches, pvPatches := countPatches(cs, "persistentvolumeclaims"), countPatches(cs, "persistentvolumes")
				_, err := reconcileVolumeProtectionWithIdentities(t.Context(), cs, "ns", testStsName, 1, []string{"data"}, protect, inventory)
				require.ErrorContains(t, err, tt.want)
				require.Zero(t, *pvcPatches, "validate the PV as well before changing even the matching PVC")
				require.Zero(t, *pvPatches)
				gotPVC, err := cs.CoreV1().PersistentVolumeClaims("ns").Get(t.Context(), pvc.Name, metav1.GetOptions{})
				require.NoError(t, err)
				gotPV, err := cs.CoreV1().PersistentVolumes().Get(t.Context(), pv.Name, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, pvc, gotPVC)
				require.Equal(t, pv, gotPV)
			})
		}
	}
}

func TestRecoveryVolumeProtectionRequiresSavedInventory(t *testing.T) {
	t.Parallel()
	pvc, pv, _ := retainedProtectionPair()
	cs := fake.NewClientset(pvc, pv)
	patches := countPatches(cs, "persistentvolumeclaims")
	_, err := reconcileVolumeProtectionWithIdentities(t.Context(), cs, "ns", testStsName, 1, []string{"data"}, true, map[string]retainedClaimIdentity{})
	require.ErrorContains(t, err, "missing or invalid retained identity")
	require.Zero(t, *patches)
}

func TestRecoveryVolumeProtectionFencesLateReplacementAndRebind(t *testing.T) {
	t.Parallel()
	for _, resource := range []string{"persistentvolumeclaims", "persistentvolumes"} {
		for _, change := range []string{"replacement", "rebind"} {
			for _, protect := range []bool{true, false} {
				t.Run(resource+"/"+change+"/"+strconv.FormatBool(protect), func(t *testing.T) {
					t.Parallel()
					pvc, pv, inventory := retainedProtectionPair()
					if !protect {
						pvc.Labels = map[string]string{labelDeletionProtection: labelDeletionProtectionValue}
						pv.Labels = map[string]string{labelDeletionProtection: labelDeletionProtectionValue}
					}
					cs := fake.NewClientset(pvc, pv)
					attempts := 0
					fenceFailure := errors.New("patch identity/version precondition rejected")
					cs.PrependReactor("patch", resource, func(action clienttesting.Action) (bool, runtime.Object, error) {
						attempts++
						patchAction := action.(clienttesting.PatchAction)
						require.Equal(t, types.MergePatchType, patchAction.GetPatchType())
						var patch struct {
							Metadata metav1.ObjectMeta `json:"metadata"`
						}
						require.NoError(t, json.Unmarshal(patchAction.GetPatch(), &patch))
						var original metav1.Object
						var changed runtime.Object
						if resource == "persistentvolumeclaims" {
							original = pvc
							late := pvc.DeepCopy()
							if change == "replacement" {
								late.UID = "late-replacement"
							} else {
								late.Spec.VolumeName, late.ResourceVersion = "other-volume", "12"
							}
							changed = late
						} else {
							original = pv
							late := pv.DeepCopy()
							if change == "replacement" {
								late.UID = "late-replacement"
							} else {
								late.Spec.ClaimRef.UID, late.ResourceVersion = "other-claim", "23"
							}
							changed = late
						}
						require.Equal(t, original.GetUID(), patch.Metadata.UID, "the UID fence must survive even reuse of a resourceVersion")
						require.Equal(t, original.GetResourceVersion(), patch.Metadata.ResourceVersion, "binding and deletion changes must conflict")
						// Fake clients do not enforce API-server patch preconditions.
						// Interpose the late change and model the server rejecting it.
						require.NoError(t, cs.Tracker().Update(action.GetResource(), changed, action.GetNamespace()))

						return true, nil, fenceFailure
					})
					_, err := reconcileVolumeProtectionWithIdentities(t.Context(), cs, "ns", testStsName, 1, []string{"data"}, protect, inventory)
					require.ErrorIs(t, err, fenceFailure)
					require.Equal(t, 1, attempts)
					changed, err := cs.Tracker().Get(corev1.SchemeGroupVersion.WithResource(resource), "", pv.Name)
					if resource == "persistentvolumeclaims" {
						changed, err = cs.Tracker().Get(corev1.SchemeGroupVersion.WithResource(resource), pvc.Namespace, pvc.Name)
					}
					require.NoError(t, err)
					metadata := changed.(metav1.Object)
					require.Equal(t, !protect, metadata.GetLabels()[labelDeletionProtection] == labelDeletionProtectionValue, "never change the replacement/rebound resource's label")
					_, err = reconcileVolumeProtectionWithIdentities(t.Context(), cs, "ns", testStsName, 1, []string{"data"}, protect, inventory)
					require.ErrorContains(t, err, "identity or binding changed")
					require.Equal(t, 1, attempts, "a fresh read must reject the new identity without another patch")
				})
			}
		}
	}
}

func TestRecoveryVolumeProtectionConflictThenReconcileSucceeds(t *testing.T) {
	t.Parallel()
	for _, resource := range []string{"persistentvolumeclaims", "persistentvolumes"} {
		t.Run(resource, func(t *testing.T) {
			t.Parallel()
			pvc, pv, inventory := retainedProtectionPair()
			cs := fake.NewClientset(pvc, pv)
			attempts := 0
			cs.PrependReactor("patch", resource, func(action clienttesting.Action) (bool, runtime.Object, error) {
				attempts++
				var patch struct {
					Metadata metav1.ObjectMeta `json:"metadata"`
				}
				require.NoError(t, json.Unmarshal(action.(clienttesting.PatchAction).GetPatch(), &patch))
				wantVersion := "22"
				if resource == "persistentvolumeclaims" {
					wantVersion = "11"
				}
				if attempts == 1 {
					require.Equal(t, wantVersion, patch.Metadata.ResourceVersion)
					var changed runtime.Object
					if resource == "persistentvolumeclaims" {
						fresh := pvc.DeepCopy()
						fresh.ResourceVersion = "33"
						fresh.Labels = map[string]string{"concurrent": "kept"}
						changed = fresh
					} else {
						fresh := pv.DeepCopy()
						fresh.ResourceVersion = "33"
						fresh.Labels = map[string]string{"concurrent": "kept"}
						changed = fresh
					}
					require.NoError(t, cs.Tracker().Update(action.GetResource(), changed, action.GetNamespace()))

					return true, nil, apierrors.NewConflict(action.GetResource().GroupResource(), action.(clienttesting.PatchAction).GetName(), errors.New("concurrent label write"))
				}
				require.Equal(t, "33", patch.Metadata.ResourceVersion, "the next reconcile must reread the new version")

				return false, nil, nil
			})
			_, err := reconcileVolumeProtectionWithIdentities(t.Context(), cs, "ns", testStsName, 1, []string{"data"}, true, inventory)
			require.True(t, apierrors.IsConflict(err))
			pending, err := reconcileVolumeProtectionWithIdentities(t.Context(), cs, "ns", testStsName, 1, []string{"data"}, true, inventory)
			require.NoError(t, err)
			require.False(t, pending)
			require.Equal(t, 2, attempts)
			gotPVC, err := cs.CoreV1().PersistentVolumeClaims("ns").Get(t.Context(), pvc.Name, metav1.GetOptions{})
			require.NoError(t, err)
			gotPV, err := cs.CoreV1().PersistentVolumes().Get(t.Context(), pv.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, labelDeletionProtectionValue, gotPVC.Labels[labelDeletionProtection])
			require.Equal(t, labelDeletionProtectionValue, gotPV.Labels[labelDeletionProtection])
			if resource == "persistentvolumeclaims" {
				require.Equal(t, "kept", gotPVC.Labels["concurrent"])
			} else {
				require.Equal(t, "kept", gotPV.Labels["concurrent"])
			}
			_, err = reconcileVolumeProtectionWithIdentities(t.Context(), cs, "ns", testStsName, 1, []string{"data"}, true, inventory)
			require.NoError(t, err)
			require.Equal(t, 2, attempts, "the converged pass must not patch again")
		})
	}
}
