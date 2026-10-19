package controller

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// scaleDownOwnershipFixture is a Cluster with spec.resourceName "payments"
// whose owned StatefulSet is scaled from 3 to 1 replicas.
func scaleDownOwnershipFixture() (*ledgerv1alpha1.Cluster, *appsv1.StatefulSet) {
	cluster := clusterWithResourceName("orders", "payments")
	cluster.Namespace = "ns"
	cluster.UID = "cluster-uid"
	applyDefaults(cluster)
	sts := controlledByCluster(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "ns", UID: "sts-uid"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: new(int32(3)),
			Selector: &metav1.LabelSelector{MatchLabels: selectorLabels(cluster)},
		},
	}, cluster)

	return cluster, sts
}

// scaledDownPVC is the claim "data-payments-<ordinal>" as the StatefulSet
// controller creates it: the StatefulSet selector labels, no owner reference.
func scaledDownPVC(cluster *ledgerv1alpha1.Cluster, ordinal int32, uid types.UID) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Name: pvcName(cluster, "data", ordinal), Namespace: cluster.Namespace, UID: uid,
		Labels: selectorLabels(cluster),
	}}
}

// deletedPVCs lists the PVC deletes the clientset received, with the UID
// precondition each carried ("" without one).
func deletedPVCs(clientset *k8sfake.Clientset) map[string]types.UID {
	deleted := map[string]types.UID{}
	for _, action := range clientset.Actions() {
		del, ok := action.(k8stesting.DeleteAction)
		if !ok || action.GetResource().Resource != "persistentvolumeclaims" {
			continue
		}
		var uid types.UID
		if pre := del.GetDeleteOptions().Preconditions; pre != nil && pre.UID != nil {
			uid = *pre.UID
		}
		deleted[del.GetName()] = uid
	}

	return deleted
}

// TestDeleteScaledDownPVCsOwnershipPolicy proves scale-down deletes only the
// claims the Cluster's StatefulSet created for a removed ordinal (its selector
// labels; no owner other than that StatefulSet or the ordinal's Pod), pinned to
// the inspected UID, and keeps and reports every other claim under the derived
// name.
func TestDeleteScaledDownPVCsOwnershipPolicy(t *testing.T) {
	t.Parallel()
	cluster, sts := scaleDownOwnershipFixture()
	other := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "ns", UID: "other-uid"}}

	for _, tc := range []struct {
		name       string
		pvc        func() *corev1.PersistentVolumeClaim
		wantDelete bool
		wantReason string
	}{
		{
			name:       "claim created by the StatefulSet is deleted",
			pvc:        func() *corev1.PersistentVolumeClaim { return scaledDownPVC(cluster, 2, "owned-uid") },
			wantDelete: true,
		},
		{
			name: "claim with retention-policy owner references is deleted",
			pvc: func() *corev1.PersistentVolumeClaim {
				pvc := scaledDownPVC(cluster, 2, "owned-uid")
				pvc.OwnerReferences = []metav1.OwnerReference{
					{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "payments", UID: "sts-uid"},
					{APIVersion: "v1", Kind: "Pod", Name: "payments-2", UID: "pod-uid"},
				}

				return pvc
			},
			wantDelete: true,
		},
		{
			name: "foreign claim under the derived name is kept",
			pvc: func() *corev1.PersistentVolumeClaim {
				pvc := scaledDownPVC(cluster, 2, "foreign-uid")
				pvc.Labels = map[string]string{"app": "someone-else"}

				return pvc
			},
			wantReason: "does not match the StatefulSet selector",
		},
		{
			name: "claim of another Cluster with the same base name is kept",
			pvc: func() *corev1.PersistentVolumeClaim {
				pvc := scaledDownPVC(cluster, 2, "foreign-uid")
				pvc.Labels = selectorLabels(other)

				return pvc
			},
			wantReason: `label app.kubernetes.io/instance="other"`,
		},
		{
			name: "claim owned by the Pod of another ordinal is kept",
			pvc: func() *corev1.PersistentVolumeClaim {
				pvc := scaledDownPVC(cluster, 2, "foreign-uid")
				pvc.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: "payments-1", UID: "pod-1-uid"}}

				return pvc
			},
			wantReason: "owned by Pod/payments-1 (uid pod-1-uid)",
		},
		{
			name: "claim owned by another StatefulSet is kept",
			pvc: func() *corev1.PersistentVolumeClaim {
				pvc := scaledDownPVC(cluster, 2, "foreign-uid")
				pvc.OwnerReferences = []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "payments", UID: "old-sts-uid"}}

				return pvc
			},
			wantReason: "owned by StatefulSet/payments (uid old-sts-uid)",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pvc := tc.pvc()
			clientset := k8sfake.NewClientset(pvc)

			skipped, err := deleteScaledDownPVCs(context.Background(), clientset, cluster, sts, 3, 2, []string{"data"})
			require.NoError(t, err)

			_, getErr := clientset.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), pvc.Name, metav1.GetOptions{})
			if tc.wantDelete {
				require.Empty(t, skipped)
				require.Equal(t, map[string]types.UID{pvc.Name: pvc.UID}, deletedPVCs(clientset), "the delete must be pinned to the inspected UID")
				require.True(t, apierrors.IsNotFound(getErr), "the owned claim must be deleted, got %v", getErr)

				return
			}
			require.Empty(t, deletedPVCs(clientset), "a claim the StatefulSet did not create must never be deleted")
			require.NoError(t, getErr)
			require.Len(t, skipped, 1)
			require.Contains(t, skipped[0], pvc.Name)
			require.Contains(t, skipped[0], tc.wantReason)
		})
	}
}

// TestDeleteScaledDownPVCsNeverDeletesAReplacement proves a claim replaced
// under the derived name between the ownership read and the delete survives:
// the read sees the owned claim, the API server holds a replacement with
// another UID, and the delete's UID precondition fails. The fake clientset
// ignores delete preconditions, so the reactor enforces the UID one the way
// the API server does, with a Conflict when the live UID differs.
func TestDeleteScaledDownPVCsNeverDeletesAReplacement(t *testing.T) {
	t.Parallel()
	cluster, sts := scaleDownOwnershipFixture()
	inspected := scaledDownPVC(cluster, 1, "owned-uid")
	replacement := scaledDownPVC(cluster, 1, "replacement-uid")
	clientset := k8sfake.NewClientset(replacement)
	clientset.PrependReactor("get", "persistentvolumeclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, inspected.DeepCopy(), nil
	})
	clientset.PrependReactor("delete", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		del, ok := action.(k8stesting.DeleteAction)
		require.True(t, ok)
		pre := del.GetDeleteOptions().Preconditions
		if pre != nil && pre.UID != nil && *pre.UID != replacement.UID {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "persistentvolumeclaims"}, del.GetName(),
				fmt.Errorf("Precondition failed: UID in precondition: %s, UID in object meta: %s", *pre.UID, replacement.UID))
		}

		return false, nil, nil
	})

	skipped, err := deleteScaledDownPVCs(context.Background(), clientset, cluster, sts, 2, 1, []string{"data"})

	require.NoError(t, err)
	require.Equal(t, []string{"data-payments-1 (replaced after its ownership was checked)"}, skipped)
	require.Equal(t, map[string]types.UID{"data-payments-1": "owned-uid"}, deletedPVCs(clientset))
	live, err := clientset.Tracker().Get(corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims"), "ns", "data-payments-1")
	require.NoError(t, err, "the replacement was deleted")
	require.Equal(t, replacement.UID, live.(*corev1.PersistentVolumeClaim).UID)
}

// TestScaleDownNeverExecsIntoPodOfAnotherStatefulSet proves the Raft
// scale-down checks pod-0 itself: the Cluster controls the StatefulSet, but
// "payments-0" is still controlled by an older StatefulSet UID, so no
// ledgerctl command runs, replicas are not reduced and no PVC is deleted.
func TestScaleDownNeverExecsIntoPodOfAnotherStatefulSet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := resourceNameTestScheme(t)
	cluster, sts := scaleDownOwnershipFixture()
	cluster.Spec.Replicas = new(int32(1))
	oldStatefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "ns", UID: "old-sts-uid"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, sts).Build()
	clientset := k8sfake.NewClientset(pod0ControlledBy(oldStatefulSet), scaledDownPVC(cluster, 1, "pvc-1"), scaledDownPVC(cluster, 2, "pvc-2"))
	r := &ClusterReconciler{Client: c, Scheme: scheme, Config: &rest.Config{}, Clientset: clientset}
	execCalls := 0
	exec := func(context.Context, *rest.Config, kubernetes.Interface, string, string, string, []string) (*execResult, error) {
		execCalls++

		return &execResult{}, nil
	}

	_, err := r.reconcileStatefulSetWithExec(ctx, cluster, "hash", nil, exec)

	require.ErrorIs(t, err, errPodNotOwned)
	require.ErrorContains(t, err, "pod ns/payments-0 is controlled by StatefulSet/payments (uid old-sts-uid), not StatefulSet payments (uid sts-uid)")
	require.Zero(t, execCalls, "must not exec into a pod the Cluster's StatefulSet does not control")
	live := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(sts), live))
	require.Equal(t, int32(3), *live.Spec.Replicas, "replicas must not drop before the Raft removal")
	require.Empty(t, deletedPVCs(clientset))
}

// TestDiskUsageAndSinkExecsRefusePodOfAnotherStatefulSet covers the remaining
// exec paths: the volume-expansion disk-usage probe and the EventSink
// ledgerctl helper (ClusterReconciler.ledgerctlExecOutput) fail with
// errPodNotOwned before any exec when the target pod is controlled by an older
// StatefulSet UID. Config is nil, so reaching the real exec would fail with
// another error.
func TestDiskUsageAndSinkExecsRefusePodOfAnotherStatefulSet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, sts := scaleDownOwnershipFixture()
	oldStatefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "ns", UID: "old-sts-uid"}}
	clientset := k8sfake.NewClientset(pod0ControlledBy(oldStatefulSet))

	t.Run("volume expansion disk usage", func(t *testing.T) {
		t.Parallel()
		r := &VolumeExpansionReconciler{Clientset: clientset}

		results := r.collectDiskUsage(ctx, cluster, sts, tlsModeDisabled, 1)

		require.Len(t, results, 1)
		require.Equal(t, "payments-0", results[0].Pod)
		require.ErrorIs(t, results[0].Err, errPodNotOwned)
		require.ErrorContains(t, results[0].Err, "uid old-sts-uid")
	})

	t.Run("EventSink ledgerctl", func(t *testing.T) {
		t.Parallel()
		scheme := resourceNameTestScheme(t)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts.DeepCopy()).Build()
		r := &ClusterReconciler{Client: c, Scheme: scheme, Clientset: clientset}

		_, err := r.ledgerctlExecOutput(ctx, cluster, cluster.Spec.GrpcPort, "events", "list", "--json")

		require.ErrorIs(t, err, errPodNotOwned)
		require.ErrorContains(t, err, "uid old-sts-uid")
	})
}

// TestScaledDownPVCMismatchWithoutSelectorLabels proves a StatefulSet without
// selector labels matches no claim: with nothing to compare, every claim under
// the derived name would otherwise pass the label check.
func TestScaledDownPVCMismatchWithoutSelectorLabels(t *testing.T) {
	t.Parallel()
	cluster, sts := scaleDownOwnershipFixture()
	pvc := scaledDownPVC(cluster, 2, "owned-uid")
	for name, selector := range map[string]*metav1.LabelSelector{
		"nil selector":         nil,
		"empty matchLabels":    {},
		"only matchExpression": {MatchExpressions: []metav1.LabelSelectorRequirement{{Key: labelName, Operator: metav1.LabelSelectorOpExists}}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			withoutLabels := sts.DeepCopy()
			withoutLabels.Spec.Selector = selector

			require.Equal(t, "the StatefulSet has no selector labels to match", scaledDownPVCMismatch(pvc, withoutLabels, "payments-2"))
		})
	}
}

// TestRequirePodControlledByRejectsPodWithoutController proves an orphaned pod
// (no controller reference, e.g. after the VolumeClaimTemplate
// orphan-and-recreate path, before the new StatefulSet adopts it) is refused.
func TestRequirePodControlledByRejectsPodWithoutController(t *testing.T) {
	t.Parallel()
	_, sts := scaleDownOwnershipFixture()
	orphan := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "payments-0", Namespace: "ns", UID: "pod-uid"}}

	err := requirePodControlledBy(context.Background(), k8sfake.NewClientset(orphan), sts, "ns", "payments-0")

	require.ErrorIs(t, err, errPodNotOwned)
	require.EqualError(t, err, "pod not controlled by the Cluster StatefulSet: pod ns/payments-0 has no controller, expected StatefulSet payments (uid sts-uid)")
}

// scaleDownReconcileFixture is a Cluster ("payments" StatefulSet, scaled from
// 3 replicas) wired to a ClusterReconciler over fake clients: the
// controller-runtime client holds the Cluster and its StatefulSet (whose spec
// matches the desired one, so no recreate), the clientset holds pod-0
// (controlled by the StatefulSet) and the given claims.
type scaleDownReconcileFixture struct {
	cluster   *ledgerv1alpha1.Cluster
	client    client.Client
	clientset *k8sfake.Clientset
	recorder  *record.FakeRecorder
}

func newScaleDownReconcileFixture(t *testing.T, liveReplicas, desiredReplicas int32, annotations map[string]string, claims ...*corev1.PersistentVolumeClaim) *scaleDownReconcileFixture {
	t.Helper()
	cluster, sts := scaleDownOwnershipFixture()
	cluster.Spec.Persistence.DeletionProtection = new(false)
	sts.Spec = buildStatefulSetSpec(cluster, "hash", nil, tlsModeDisabled)
	sts.Spec.Replicas = new(liveReplicas)
	sts.Annotations = annotations
	cluster.Spec.Replicas = new(desiredReplicas)
	objects := []runtime.Object{pod0ControlledBy(sts)}
	for _, claim := range claims {
		objects = append(objects, claim)
	}

	return &scaleDownReconcileFixture{
		cluster:   cluster,
		client:    fake.NewClientBuilder().WithScheme(resourceNameTestScheme(t)).WithObjects(cluster, sts).Build(),
		clientset: k8sfake.NewClientset(objects...),
		recorder:  record.NewFakeRecorder(10),
	}
}

// reconcile runs one StatefulSet pass with a fresh reconciler, as a new
// operator process would. Membership already lacks the removed nodes, so the
// Raft scale-down succeeds without removing anything.
func (f *scaleDownReconcileFixture) reconcile(t *testing.T) (ctrl.Result, error) {
	t.Helper()
	r := &ClusterReconciler{Client: f.client, Scheme: f.client.Scheme(), Config: &rest.Config{}, Clientset: f.clientset, Recorder: f.recorder}
	exec := func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, _, _, _ string, command []string) (*execResult, error) {
		if strings.Contains(command[2], "'cluster' 'status'") {
			return &execResult{Stdout: `{"state":"Leader","nodes":[{"id":1}]}`}, nil
		}

		return &execResult{}, nil
	}

	return r.reconcileStatefulSetWithExec(context.Background(), f.cluster, "hash", nil, exec)
}

func (f *scaleDownReconcileFixture) statefulSet(t *testing.T) *appsv1.StatefulSet {
	t.Helper()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, f.client.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "payments"}, sts))

	return sts
}

func (f *scaleDownReconcileFixture) claimExists(t *testing.T, name string) bool {
	t.Helper()
	_, err := f.clientset.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false
	}
	require.NoError(t, err)

	return true
}

// drainEvents returns the events recorded since the last call.
func (f *scaleDownReconcileFixture) drainEvents() []string {
	var events []string
	for {
		select {
		case event := <-f.recorder.Events:
			events = append(events, event)
		default:
			return events
		}
	}
}

func ownedClaims(cluster *ledgerv1alpha1.Cluster, ordinals ...int32) []*corev1.PersistentVolumeClaim {
	var claims []*corev1.PersistentVolumeClaim
	for _, ordinal := range ordinals {
		for _, volume := range []string{"wal", "data"} {
			claims = append(claims, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
				Name: pvcName(cluster, volume, ordinal), Namespace: "ns", UID: types.UID(pvcName(cluster, volume, ordinal) + "-uid"),
				Labels: selectorLabels(cluster),
			}})
		}
	}

	return claims
}

// TestScaleDownPVCCleanupResumesAfterTransientError proves the cleanup of the
// removed ordinals' PVCs survives an API error: the first Delete of one claim
// fails, the pass still handles the other claims and returns the error, the
// replica drop and the scaled-down-from record are persisted, and the next
// pass, which is no longer a scale-down, deletes the remaining claim and clears
// the record.
func TestScaleDownPVCCleanupResumesAfterTransientError(t *testing.T) {
	t.Parallel()
	cluster, _ := scaleDownOwnershipFixture()
	f := newScaleDownReconcileFixture(t, 3, 1, nil, ownedClaims(cluster, 1, 2)...)
	attempts := map[string]int{}
	f.clientset.PrependReactor("delete", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		name := action.(k8stesting.DeleteAction).GetName()
		attempts[name]++
		if name == "data-payments-2" && attempts[name] == 1 {
			return true, nil, apierrors.NewServerTimeout(schema.GroupResource{Resource: "persistentvolumeclaims"}, "delete", 1)
		}

		return false, nil, nil
	})

	_, err := f.reconcile(t)

	require.ErrorContains(t, err, "deleting PVC data-payments-2")
	require.True(t, apierrors.IsServerTimeout(err), "the transient API error must be returned for a retry: %v", err)
	require.True(t, f.claimExists(t, "data-payments-2"))
	for _, name := range []string{"wal-payments-2", "wal-payments-1", "data-payments-1"} {
		require.False(t, f.claimExists(t, name), "%s must be deleted despite the failure on another claim", name)
	}
	sts := f.statefulSet(t)
	require.Equal(t, int32(1), *sts.Spec.Replicas)
	require.Equal(t, "3", sts.Annotations[annotationScaledDownFrom], "the pending range must survive the failed pass")

	_, err = f.reconcile(t)

	require.NoError(t, err)
	require.False(t, f.claimExists(t, "data-payments-2"), "the next pass must resume the cleanup")
	require.Equal(t, map[string]int{"data-payments-2": 2, "wal-payments-2": 1, "wal-payments-1": 1, "data-payments-1": 1}, attempts)
	require.NotContains(t, f.statefulSet(t).Annotations, annotationScaledDownFrom)
	require.Nil(t, meta.FindStatusCondition(f.cluster.Status.Conditions, conditionScaleDownPVCsRetained))
}

// TestScaleDownRetainedPVCReportedUntilGone proves a kept claim is a lasting
// record: ScaleDownPVCsRetained stays True on every later pass (one
// ScaleDownPVCNotDeleted event, not one per pass), and is cleared with the
// scaled-down-from record once the claim is gone.
func TestScaleDownRetainedPVCReportedUntilGone(t *testing.T) {
	t.Parallel()
	cluster, _ := scaleDownOwnershipFixture()
	claims := ownedClaims(cluster, 1, 2)
	claims[3].Labels = map[string]string{"app": "someone-else"} // data-payments-2
	f := newScaleDownReconcileFixture(t, 3, 1, nil, claims...)

	for pass := range 2 {
		_, err := f.reconcile(t)
		require.NoError(t, err)

		condition := meta.FindStatusCondition(f.cluster.Status.Conditions, conditionScaleDownPVCsRetained)
		require.NotNil(t, condition, "pass %d", pass)
		require.Equal(t, metav1.ConditionTrue, condition.Status)
		require.Equal(t, "PVCsRetained", condition.Reason)
		require.Contains(t, condition.Message, "data-payments-2")
		require.True(t, f.claimExists(t, "data-payments-2"))
		require.Equal(t, "3", f.statefulSet(t).Annotations[annotationScaledDownFrom])
		events := f.drainEvents()
		if pass == 0 {
			require.Len(t, events, 1)
			require.Contains(t, events[0], "Warning ScaleDownPVCNotDeleted")
			require.Contains(t, events[0], "data-payments-2")
		} else {
			require.Empty(t, events, "an unchanged condition must not repeat the event")
		}
	}

	require.NoError(t, f.clientset.CoreV1().PersistentVolumeClaims("ns").Delete(context.Background(), "data-payments-2", metav1.DeleteOptions{}))
	_, err := f.reconcile(t)

	require.NoError(t, err)
	require.Nil(t, meta.FindStatusCondition(f.cluster.Status.Conditions, conditionScaleDownPVCsRetained))
	require.NotContains(t, f.statefulSet(t).Annotations, annotationScaledDownFrom)
}

// TestScaleUpRefusedWhileRemovedOrdinalClaimRemains proves a scale-up never
// remounts a claim kept from a scale-down: replicas stay at the live count,
// ScaleDownPVCsRetained reports ScaleUpBlocked with a ScaleUpBlocked event, the
// kept claim is not stamped with the deletion-protection label (the user must
// be able to delete it), and the scale-up proceeds once the claim is gone.
func TestScaleUpRefusedWhileRemovedOrdinalClaimRemains(t *testing.T) {
	t.Parallel()
	cluster, _ := scaleDownOwnershipFixture()
	foreign := scaledDownPVC(cluster, 1, "foreign-uid")
	foreign.Labels = map[string]string{"app": "someone-else"}
	f := newScaleDownReconcileFixture(t, 1, 3, map[string]string{annotationScaledDownFrom: "3"}, foreign)
	f.cluster.Spec.Persistence.DeletionProtection = new(true)

	result, err := f.reconcile(t)

	require.NoError(t, err)
	require.NotZero(t, result.RequeueAfter)
	require.LessOrEqual(t, result.RequeueAfter, nameConflictRequeueInterval)
	require.Equal(t, int32(1), *f.statefulSet(t).Spec.Replicas, "the scale-up must be refused")
	condition := meta.FindStatusCondition(f.cluster.Status.Conditions, conditionScaleDownPVCsRetained)
	require.NotNil(t, condition)
	require.Equal(t, "ScaleUpBlocked", condition.Reason)
	require.Contains(t, condition.Message, "refusing to scale up to 3 replicas")
	require.Contains(t, condition.Message, "data-payments-1")
	events := f.drainEvents()
	require.Len(t, events, 1)
	require.Contains(t, events[0], "Warning ScaleUpBlocked")
	kept, err := f.clientset.CoreV1().PersistentVolumeClaims("ns").Get(context.Background(), "data-payments-1", metav1.GetOptions{})
	require.NoError(t, err)
	require.NotContains(t, kept.Labels, labelDeletionProtection)

	require.NoError(t, f.clientset.CoreV1().PersistentVolumeClaims("ns").Delete(context.Background(), "data-payments-1", metav1.DeleteOptions{}))
	_, err = f.reconcile(t)

	require.NoError(t, err)
	require.Equal(t, int32(3), *f.statefulSet(t).Spec.Replicas)
	require.Nil(t, meta.FindStatusCondition(f.cluster.Status.Conditions, conditionScaleDownPVCsRetained))
	require.NotContains(t, f.statefulSet(t).Annotations, annotationScaledDownFrom)
}
