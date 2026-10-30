package controller

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	crfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

type scaleDownIdentityFixture struct {
	ledger *ledgerv1alpha1.Cluster
	sts    *appsv1.StatefulSet
	pvc    *corev1.PersistentVolumeClaim
	pv     *corev1.PersistentVolume
	cs     *fake.Clientset
	reader client.Client
	plan   *scaleDownPVCPlan
}

func newScaleDownIdentityFixture(t *testing.T) *scaleDownIdentityFixture {
	t.Helper()
	ledger := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "test", Namespace: "test", UID: "cluster"}}
	ledger.Spec.Persistence.RetentionPolicy = &ledgerv1alpha1.RetentionPolicySpec{WhenScaled: "Delete"}
	ledger.Spec.Replicas = new(int32(1))
	sts := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "ledger-test", Namespace: "test", UID: "source", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: ledgerv1alpha1.GroupVersion.String(), Kind: "Cluster", Name: ledger.Name, UID: ledger.UID, Controller: new(true)}}}, Spec: appsv1.StatefulSetSpec{Replicas: new(int32(2)), PersistentVolumeClaimRetentionPolicy: buildRetentionPolicy(ledger), VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "data"}}}}}
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "data-ledger-test-1", Namespace: "test", UID: "original", ResourceVersion: "1", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: sts.Name, UID: sts.UID, Controller: new(true)}}}, Spec: corev1.PersistentVolumeClaimSpec{VolumeName: "original-pv"}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "ledger-test-1", Namespace: "test", UID: "source-pod", OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: sts.Name, UID: sts.UID, Controller: new(true)}}}, Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: pvc.Name}}}}}}
	pv := &corev1.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pvc.Spec.VolumeName, UID: "original-pv", ResourceVersion: "1"}, Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID}}}
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	reader := crfake.NewClientBuilder().WithScheme(scheme).WithObjects(sts, pod, pvc, pv).Build()
	cs := fake.NewSimpleClientset(pvc, pv, pod)
	plan, err := prepareScaleDownPVCPlan(t.Context(), cs, reader, ledger, sts, 1)
	require.NoError(t, err)
	encoded, err := json.Marshal(plan)
	require.NoError(t, err)
	sts.Annotations = map[string]string{annotationScaleDownPVCFrom: "2", annotationScaleDownPVCPlan: string(encoded)}
	sts.Spec.Replicas = new(int32(1))
	require.NoError(t, reader.Update(t.Context(), sts))

	return &scaleDownIdentityFixture{ledger: ledger, sts: sts, pvc: pvc, pv: pv, cs: cs, reader: reader, plan: plan}
}

func (f *scaleDownIdentityFixture) cleanup(t *testing.T) error {
	t.Helper()

	return deleteScaledDownPVCs(t.Context(), f.cs, f.reader, f.ledger, f.sts, f.plan)
}

func TestScaleDownPVCDeletionFencesReplacementAtDelete(t *testing.T) {
	t.Parallel()
	f := newScaleDownIdentityFixture(t)
	attempts := 0
	f.cs.PrependReactor("delete", "persistentvolumeclaims", func(action ktesting.Action) (bool, runtime.Object, error) {
		attempts++
		a := action.(ktesting.DeleteAction)
		replacement := f.pvc.DeepCopy()
		replacement.UID = "replacement"
		replacement.ResourceVersion = "2"
		require.NoError(t, f.cs.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), replacement, "test"))
		options := a.GetDeleteOptions()
		if options.Preconditions != nil && options.Preconditions.UID != nil && options.Preconditions.ResourceVersion != nil && (*options.Preconditions.UID != replacement.UID || *options.Preconditions.ResourceVersion != replacement.ResourceVersion) {
			return true, nil, kerrors.NewConflict(schema.GroupResource{Resource: "persistentvolumeclaims"}, f.pvc.Name, errors.New("identity precondition"))
		}

		return false, nil, nil
	})
	require.Error(t, f.cleanup(t), "a same-name replacement must not be deleted")
	require.Equal(t, 1, attempts)
	remaining, err := f.cs.CoreV1().PersistentVolumeClaims("test").Get(t.Context(), f.pvc.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, types.UID("replacement"), remaining.UID)
	// A fresh attempt still uses the frozen original identity, never the replacement.
	require.ErrorContains(t, f.cleanup(t), "identity changed")
	require.Equal(t, 1, attempts)
}

func TestScaleDownPVCDeletionRejectsChangedAuthority(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"pvc_uid", "foreign_owner", "pvc_rebind", "pv_uid", "pv_claimref", "pv_deleting", "source_owner", "source_plan"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newScaleDownIdentityFixture(t)
			switch name {
			case "pvc_uid":
				f.pvc.UID = "replacement"
			case "foreign_owner":
				f.pvc.OwnerReferences[0].UID = "foreign"
			case "pvc_rebind":
				f.pvc.Spec.VolumeName = "different"
			case "pv_uid":
				f.pv.UID = "replacement"
			case "pv_claimref":
				f.pv.Spec.ClaimRef.UID = "foreign"
			case "pv_deleting":
				f.pv.DeletionTimestamp = new(metav1.Now())
			case "source_owner":
				s := f.sts.DeepCopy()
				s.OwnerReferences[0].UID = "foreign"
				require.NoError(t, f.reader.Update(t.Context(), s))
			case "source_plan":
				s := f.sts.DeepCopy()
				s.Annotations[annotationScaleDownPVCPlan] = "different"
				require.NoError(t, f.reader.Update(t.Context(), s))
			}
			require.NoError(t, f.cs.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), f.pvc, "test"))
			require.NoError(t, f.cs.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), f.pv, ""))
			f.cs.ClearActions()
			require.Error(t, f.cleanup(t))
			for _, a := range f.cs.Actions() {
				require.NotEqual(t, "delete", a.GetVerb())
			}
		})
	}
}

func TestScaleDownPVCDeletionMetadataConflictRetriesOriginal(t *testing.T) {
	t.Parallel()
	f := newScaleDownIdentityFixture(t)
	attempts := 0
	f.cs.PrependReactor("delete", "persistentvolumeclaims", func(action ktesting.Action) (bool, runtime.Object, error) {
		attempts++
		options := action.(ktesting.DeleteAction).GetDeleteOptions()
		require.NotNil(t, options.Preconditions)
		require.Equal(t, f.pvc.UID, *options.Preconditions.UID)
		if attempts == 1 {
			updated := f.pvc.DeepCopy()
			updated.ResourceVersion = "2"
			updated.Labels = map[string]string{"updated": "yes"}
			require.NoError(t, f.cs.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), updated, "test"))
			require.Equal(t, "1", *options.Preconditions.ResourceVersion)

			return true, nil, kerrors.NewConflict(schema.GroupResource{Resource: "persistentvolumeclaims"}, f.pvc.Name, errors.New("RV conflict"))
		}
		require.Equal(t, "2", *options.Preconditions.ResourceVersion)

		return false, nil, nil
	})
	require.ErrorContains(t, f.cleanup(t), "RV conflict")
	// Retry from the durable JSON rather than the original in-memory plan.
	p, err := readScaleDownPVCPlan(f.sts, f.ledger)
	require.NoError(t, err)
	f.plan = p
	require.NoError(t, f.cleanup(t))
	require.Equal(t, 2, attempts)
	_, err = f.cs.CoreV1().PersistentVolumeClaims("test").Get(t.Context(), f.pvc.Name, metav1.GetOptions{})
	require.True(t, kerrors.IsNotFound(err))
	require.NoError(t, f.cleanup(t))
	require.Equal(t, 2, attempts)
}

func TestScaleDownPVCDeletionResponseLossRecovers(t *testing.T) {
	t.Parallel()
	f := newScaleDownIdentityFixture(t)
	attempts := 0
	f.cs.PrependReactor("delete", "persistentvolumeclaims", func(action ktesting.Action) (bool, runtime.Object, error) {
		attempts++
		require.NoError(t, f.cs.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), "test", f.pvc.Name))

		return true, nil, errors.New("delete response lost")
	})
	require.ErrorContains(t, f.cleanup(t), "response lost")
	require.NoError(t, f.cleanup(t))
	require.Equal(t, 1, attempts)
}

func TestScaleDownPVCCleanupDoesNotAdoptCountOnlyMarker(t *testing.T) {
	t.Parallel()
	f := newScaleDownIdentityFixture(t)
	delete(f.sts.Annotations, annotationScaleDownPVCPlan)
	require.NoError(t, f.reader.Update(t.Context(), f.sts))
	f.cs.ClearActions()
	r := &ClusterReconciler{Client: f.reader, APIReader: f.reader, Clientset: f.cs}
	require.ErrorContains(t, r.cleanupPendingScaleDownPVCs(t.Context(), f.ledger, f.sts), "no valid identity inventory")
	for _, a := range f.cs.Actions() {
		require.NotEqual(t, "delete", a.GetVerb())
	}
	// Opting out cancels remaining operator deletion, retaining the actual PVC.
	f.ledger.Spec.Persistence.RetentionPolicy.WhenScaled = "Retain"
	require.NoError(t, r.cleanupPendingScaleDownPVCs(t.Context(), f.ledger, f.sts))
	require.NotContains(t, f.sts.Annotations, annotationScaleDownPVCFrom)
	require.NotContains(t, f.sts.Annotations, annotationScaleDownPVCPlan)
	_, err := f.cs.CoreV1().PersistentVolumeClaims("test").Get(t.Context(), f.pvc.Name, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestScaleDownPVCPlanRejectsStaleReplicaSnapshot(t *testing.T) {
	t.Parallel()
	f := newScaleDownIdentityFixture(t)
	stale := f.sts.DeepCopy()
	stale.Spec.Replicas = new(int32(2))
	_, err := prepareScaleDownPVCPlan(t.Context(), f.cs, f.reader, f.ledger, stale, 1)
	require.Error(t, err, "a stale replica snapshot must not reauthorize claims after reduction")
}

func TestScaleDownPVCPlanDoesNotRecaptureBeforeReplicaReduction(t *testing.T) {
	t.Parallel()
	f := newScaleDownIdentityFixture(t)
	// A preparatory write committed, then a Raft step failed with replicas unchanged.
	f.sts.Spec.Replicas = new(int32(2))
	require.NoError(t, f.reader.Update(t.Context(), f.sts))
	originalPlan := f.sts.Annotations[annotationScaleDownPVCPlan]
	f.pvc.UID = "replacement"
	f.pvc.ResourceVersion = "2"
	f.pv.Spec.ClaimRef.UID = f.pvc.UID
	require.NoError(t, f.cs.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), f.pvc, "test"))
	require.NoError(t, f.cs.Tracker().Update(corev1.SchemeGroupVersion.WithResource("persistentvolumes"), f.pv, ""))
	_, err := prepareScaleDownPVCPlan(t.Context(), f.cs, f.reader, f.ledger, f.sts, 1)
	require.Error(t, err, "a pending inventory must not capture a replacement after a failed Raft step")
	require.Equal(t, originalPlan, f.sts.Annotations[annotationScaleDownPVCPlan])
}

func TestScaleDownPVCPlanNeverAdoptsLateClaim(t *testing.T) {
	t.Parallel()
	f := newScaleDownIdentityFixture(t)
	f.sts.Spec.Replicas = new(int32(2))
	f.sts.Annotations = nil
	require.NoError(t, f.reader.Update(t.Context(), f.sts))
	require.NoError(t, f.cs.Tracker().Delete(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), "test", f.pvc.Name))
	empty, err := prepareScaleDownPVCPlan(t.Context(), f.cs, f.reader, f.ledger, f.sts, 1)
	require.NoError(t, err)
	require.Empty(t, empty.Claims)
	encoded, err := json.Marshal(empty)
	require.NoError(t, err)
	f.sts.Annotations = map[string]string{annotationScaleDownPVCFrom: "2", annotationScaleDownPVCPlan: string(encoded)}
	require.NoError(t, f.reader.Update(t.Context(), f.sts))
	late := f.pvc.DeepCopy()
	late.UID = "late-claim"
	late.ResourceVersion = "3"
	require.NoError(t, f.cs.Tracker().Add(late))
	resumed, err := prepareScaleDownPVCPlan(t.Context(), f.cs, f.reader, f.ledger, f.sts, 1)
	require.NoError(t, err)
	require.Empty(t, resumed.Claims)
	f.sts.Spec.Replicas = new(int32(1))
	require.NoError(t, f.reader.Update(t.Context(), f.sts))
	f.plan = resumed
	require.NoError(t, f.cleanup(t))
	kept, err := f.cs.CoreV1().PersistentVolumeClaims("test").Get(t.Context(), late.Name, metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, late.UID, kept.UID)
}

func TestScaleDownPVCCleanupRejectsIncompleteOrChangedPreparation(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"count_only", "changed_target"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			f := newScaleDownIdentityFixture(t)
			f.sts.Spec.Replicas = new(int32(2))
			if mode == "count_only" {
				delete(f.sts.Annotations, annotationScaleDownPVCPlan)
			} else {
				f.ledger.Spec.Replicas = new(int32(3))
			}
			require.NoError(t, f.reader.Update(t.Context(), f.sts))
			r := &ClusterReconciler{Client: f.reader, APIReader: f.reader, Clientset: f.cs}
			require.Error(t, r.cleanupPendingScaleDownPVCs(t.Context(), f.ledger, f.sts))
			require.NotEmpty(t, f.sts.Annotations[annotationScaleDownPVCFrom])
			f.ledger.Spec.Persistence.RetentionPolicy.WhenScaled = "Retain"
			require.NoError(t, r.cleanupPendingScaleDownPVCs(t.Context(), f.ledger, f.sts))
			require.Empty(t, f.sts.Annotations[annotationScaleDownPVCFrom])
		})
	}
}
