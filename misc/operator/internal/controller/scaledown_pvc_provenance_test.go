package controller

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"
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
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

var pvcResource = corev1.SchemeGroupVersion.WithResource("persistentvolumeclaims")

// liveClaim returns the claim name as the fake API server holds it.
func liveClaim(t *testing.T, clientset *k8sfake.Clientset, name string) *corev1.PersistentVolumeClaim {
	t.Helper()
	obj, err := clientset.Tracker().Get(pvcResource, "ns", name)
	require.NoError(t, err)

	return obj.(*corev1.PersistentVolumeClaim)
}

// TestStampOwnedClaimsPolicy proves the provenance stamp is written only for a
// claim that a Ready, non-terminal pod controlled by the Cluster's StatefulSet
// mounts under the ordinal's derived name, and that a claim stamped by another
// Cluster UID is re-stamped under the same condition. Mounting or starting is
// not enough: a pod on another Cluster's data starts and crash-loops.
func TestStampOwnedClaimsPolicy(t *testing.T) {
	t.Parallel()
	cluster, sts := scaleDownOwnershipFixture()
	sts.Spec.Replicas = new(int32(1))
	olderStatefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "payments", Namespace: "ns", UID: "old-sts-uid"}}

	for _, tc := range []struct {
		name      string
		pod       func() *corev1.Pod
		stamp     string
		wantStamp string
	}{
		{
			name:      "claim mounted by a Ready owned pod is stamped",
			pod:       func() *corev1.Pod { return readyPod(cluster, sts, 0) },
			wantStamp: "cluster-uid",
		},
		{
			name:      "claim stamped by a previous Cluster is re-stamped",
			pod:       func() *corev1.Pod { return readyPod(cluster, sts, 0) },
			stamp:     "old-cluster-uid",
			wantStamp: "cluster-uid",
		},
		{
			name: "claim of a pod Ready again after a crash is stamped",
			pod: func() *corev1.Pod {
				pod := readyPod(cluster, sts, 0)
				pod.Status.ContainerStatuses[0].RestartCount = 3
				pod.Status.ContainerStatuses[0].LastTerminationState = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}

				return pod
			},
			wantStamp: "cluster-uid",
		},
		{
			name: "claim of a pod whose ledger container is Ready is stamped without the PodReady condition",
			pod: func() *corev1.Pod {
				pod := readyPod(cluster, sts, 0)
				pod.Status.Conditions = nil

				return pod
			},
			wantStamp: "cluster-uid",
		},
		{
			name: "claim of a crash-looping pod that never became Ready is not stamped",
			pod:  func() *corev1.Pod { return crashLooping(readyPod(cluster, sts, 0)) },
		},
		{
			name: "claim of a Pending pod is not stamped",
			pod: func() *corev1.Pod {
				pod := readyPod(cluster, sts, 0)
				pod.Status = corev1.PodStatus{Phase: corev1.PodPending}

				return pod
			},
		},
		{
			name: "claim of a terminal pod is not stamped, even with a stale Ready status",
			pod: func() *corev1.Pod {
				pod := readyPod(cluster, sts, 0)
				pod.Status.Phase = corev1.PodFailed

				return pod
			},
		},
		{
			name: "claim of a pod controlled by another StatefulSet is not stamped",
			pod: func() *corev1.Pod {
				pod := readyPod(cluster, sts, 0)
				pod.OwnerReferences = []metav1.OwnerReference{*metav1.NewControllerRef(olderStatefulSet, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))}

				return pod
			},
			stamp: "old-cluster-uid",
			// The previous stamp is kept, not cleared.
			wantStamp: "old-cluster-uid",
		},
		{
			name: "claim the pod does not mount is not stamped",
			pod: func() *corev1.Pod {
				pod := readyPod(cluster, sts, 0)
				pod.Spec.Volumes = nil

				return pod
			},
		},
		{
			name: "claim without a pod is not stamped",
			pod:  func() *corev1.Pod { return nil },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			claim := scaledDownPVC(cluster, 0, "claim-uid")
			delete(claim.Annotations, annotationClusterUID)
			if tc.stamp != "" {
				claim.Annotations[annotationClusterUID] = tc.stamp
			}
			objects := []runtime.Object{claim}
			if pod := tc.pod(); pod != nil {
				objects = append(objects, pod)
			}
			clientset := k8sfake.NewClientset(objects...)

			require.NoError(t, stampOwnedClaims(context.Background(), clientset, cluster, sts, []string{"data"}))

			got, stamped := liveClaim(t, clientset, claim.Name).Annotations[annotationClusterUID]
			if tc.wantStamp == "" {
				require.False(t, stamped, "the claim must not be stamped, got %q", got)

				return
			}
			require.Equal(t, tc.wantStamp, got)
		})
	}
}

// TestStampOwnedClaimsNeverStampsAReplacement proves the stamp is bound to
// the claim that was read: the read sees the claim at resourceVersion 1, the
// API server holds a replacement at resourceVersion 2, and the merge patch
// carrying resourceVersion 1 fails with a Conflict that is returned for a
// retry. The fake clientset ignores resourceVersion on patches, so the reactor
// enforces it the way the API server does.
func TestStampOwnedClaimsNeverStampsAReplacement(t *testing.T) {
	t.Parallel()
	cluster, sts := scaleDownOwnershipFixture()
	sts.Spec.Replicas = new(int32(1))
	inspected := ownedClaims(cluster, 0)[1] // data-payments-0, resourceVersion 1
	replacement := inspected.DeepCopy()
	replacement.UID, replacement.ResourceVersion = "replacement-uid", "2"
	clientset := k8sfake.NewClientset(replacement, readyPod(cluster, sts, 0))
	clientset.PrependReactor("get", "persistentvolumeclaims", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, inspected.DeepCopy(), nil
	})
	clientset.PrependReactor("patch", "persistentvolumeclaims", func(action k8stesting.Action) (bool, runtime.Object, error) {
		patch := action.(k8stesting.PatchAction)
		var body struct {
			Metadata struct {
				ResourceVersion string `json:"resourceVersion"`
			} `json:"metadata"`
		}
		require.NoError(t, json.Unmarshal(patch.GetPatch(), &body))
		if body.Metadata.ResourceVersion != replacement.ResourceVersion {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: "persistentvolumeclaims"}, patch.GetName(),
				errors.New("the object has been modified; please apply your changes to the latest version and try again"))
		}

		return false, nil, nil
	})

	err := stampOwnedClaims(context.Background(), clientset, cluster, sts, []string{"data"})

	require.True(t, apierrors.IsConflict(err), "the conflict must be returned for a retry: %v", err)
	require.ErrorContains(t, err, "PVC data-payments-0 changed or was replaced after it was read")
	live := liveClaim(t, clientset, "data-payments-0")
	require.Equal(t, replacement.UID, live.UID)
	require.NotContains(t, live.Annotations, annotationClusterUID, "the replacement must not be stamped")
}

// TestScaleDownStampsUnstampedClaimsBeforeTheReplicaDrop is the upgrade case:
// claims created before the operator wrote provenance stamps (v3.0.0-rc.1)
// carry none, and the same pass also changes the pod template (as an operator
// upgrade or a spec change does). The pass stamps the claims the Ready pods
// run on before it writes the StatefulSet at all — the template update can
// roll the highest ordinal first — then lowers the replicas, then deletes the
// removed ordinals' claims.
func TestScaleDownStampsUnstampedClaimsBeforeTheReplicaDrop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, _ := scaleDownOwnershipFixture()
	f := newScaleDownReconcileFixture(t, 3, 1, nil, ownedClaims(cluster, 0, 1, 2)...)
	sts := f.statefulSet(t)
	sts.Spec.Template.Annotations[annotationSpecHash] = "previous-hash"
	require.NoError(t, f.client.Update(ctx, sts))
	f.writes = nil

	_, err := f.reconcile(t)

	require.NoError(t, err)
	templateUpdate := slices.Index(f.writes, "update statefulset replicas=3")
	replicaDrop := slices.Index(f.writes, "update statefulset replicas=1")
	require.GreaterOrEqual(t, templateUpdate, 0, "the template must be updated at the live replica count first: %v", f.writes)
	require.Greater(t, replicaDrop, templateUpdate, "the replicas must be lowered after the template update: %v", f.writes)
	firstStatefulSetWrite := slices.IndexFunc(f.writes, func(write string) bool { return strings.HasPrefix(write, "update statefulset") })
	require.Equal(t, templateUpdate, firstStatefulSetWrite)
	for _, name := range []string{"wal-payments-1", "data-payments-1", "wal-payments-2", "data-payments-2"} {
		patched, deleted := slices.Index(f.writes, "patch "+name), slices.Index(f.writes, "delete "+name)
		require.GreaterOrEqual(t, patched, 0, "%s must be stamped: %v", name, f.writes)
		require.Less(t, patched, firstStatefulSetWrite, "%s must be stamped before any StatefulSet write: %v", name, f.writes)
		require.Greater(t, deleted, replicaDrop, "%s must be deleted after the replica drop: %v", name, f.writes)
		require.False(t, f.claimExists(t, name))
	}
	for _, name := range []string{"wal-payments-0", "data-payments-0"} {
		require.Equal(t, "cluster-uid", liveClaim(t, f.clientset, name).Annotations[annotationClusterUID], "the live ordinal's claims must be stamped")
	}
	require.Equal(t, int32(1), *f.statefulSet(t).Spec.Replicas)
	require.NotContains(t, f.statefulSet(t).Annotations, annotationScaledDownFrom)
	require.Nil(t, meta.FindStatusCondition(f.cluster.Status.Conditions, conditionScaleDownPVCsRetained))
}

// TestScaleDownKeepsClaimsOfAReplicaThatNeverBecameReady proves the fail-safe
// side of the stamp: pod-1 crash-loops from its first start, so no pass ever
// saw it Ready and its claims stay unstamped. Scaling from 2 to 1 keeps them
// and reports their origin as unknown, and the record stays.
func TestScaleDownKeepsClaimsOfAReplicaThatNeverBecameReady(t *testing.T) {
	t.Parallel()
	cluster, _ := scaleDownOwnershipFixture()
	f := newScaleDownReconcileFixture(t, 2, 1, nil, ownedClaims(cluster, 0, 1)...)
	pod1 := crashLooping(readyPod(cluster, f.statefulSet(t), 1))
	require.NoError(t, f.clientset.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod1, "ns"))

	_, err := f.reconcile(t)

	require.NoError(t, err)
	require.Empty(t, deletedPVCs(f.clientset))
	for _, name := range []string{"wal-payments-1", "data-payments-1"} {
		require.True(t, f.claimExists(t, name))
		require.NotContains(t, liveClaim(t, f.clientset, name).Annotations, annotationClusterUID)
	}
	condition := meta.FindStatusCondition(f.cluster.Status.Conditions, conditionScaleDownPVCsRetained)
	require.NotNil(t, condition)
	require.Equal(t, "PVCsRetained", condition.Reason)
	require.Contains(t, condition.Message, "data-payments-1 (origin cannot be established: no ledger.formance.com/cluster-uid stamp")
	require.Equal(t, int32(1), *f.statefulSet(t).Spec.Replicas)
	require.Equal(t, "2", f.statefulSet(t).Annotations[annotationScaledDownFrom], "the record must stay while a claim is kept")
	events := f.drainEvents()
	require.Len(t, events, 1)
	require.Contains(t, events[0], "Warning ScaleDownPVCNotDeleted")
}

// TestScaleDownNeverDeletesClaimOfAnotherClusterSharingResourceName is the
// provenance case selector labels cannot catch: Cluster "orders" left retained
// ownerless claims under resourceName "payments"; Cluster "billing" uses the
// same resourceName and overrides app.kubernetes.io/instance to "orders", so
// its StatefulSet selector equals the claims' labels. Its pod-2 mounts the
// retained claims but never becomes Ready: still Pending (the volume sits in
// another zone), or started and crash-looping on the foreign persisted
// cluster ID. Scaling "billing" from 3 to 1 deletes its own ordinal-1 claims
// and keeps "orders"'s, whether "orders" stamped them or ran v3.0.0-rc.1.
func TestScaleDownNeverDeletesClaimOfAnotherClusterSharingResourceName(t *testing.T) {
	t.Parallel()

	pod2States := map[string]func(*corev1.Pod) *corev1.Pod{
		"pending": func(pod *corev1.Pod) *corev1.Pod {
			pod.Status = corev1.PodStatus{Phase: corev1.PodPending}

			return pod
		},
		"crash-looping": crashLooping,
	}
	for podState, setState := range pod2States {
		for stampName, ordersStamp := range map[string]string{"unstamped": "", "stamped by the other Cluster": "orders-uid"} {
			t.Run(podState+"/"+stampName, func(t *testing.T) {
				t.Parallel()
				orders := clusterWithResourceName("orders", "payments")
				orders.Namespace, orders.UID = "ns", "orders-uid"
				applyDefaults(orders)
				billing := clusterWithResourceName("billing", "payments")
				billing.Namespace, billing.UID = "ns", "billing-uid"
				billing.Spec.AdditionalLabels = map[string]string{labelInstance: "orders"}
				applyDefaults(billing)
				require.Equal(t, selectorLabels(orders), selectorLabels(billing), "the fixture needs identical selector labels")

				retained := ownedClaims(orders, 2)
				for _, claim := range retained {
					if ordersStamp != "" {
						claim.Annotations = map[string]string{annotationClusterUID: ordersStamp}
					}
				}
				f := newScaleDownReconcileFixtureFor(t, billing, 3, 1, nil, append(ownedClaims(billing, 0, 1), retained...)...)
				pod2 := setState(readyPod(billing, f.statefulSet(t), 2))
				require.NoError(t, f.clientset.Tracker().Update(corev1.SchemeGroupVersion.WithResource("pods"), pod2, "ns"))

				_, err := f.reconcile(t)

				require.NoError(t, err)
				require.Equal(t, map[string]types.UID{"wal-payments-1": "wal-payments-1-uid", "data-payments-1": "data-payments-1-uid"}, deletedPVCs(f.clientset),
					"only billing's own claims of the removed ordinals may be deleted")
				for _, claim := range retained {
					live := liveClaim(t, f.clientset, claim.Name)
					require.Equal(t, ordersStamp, live.Annotations[annotationClusterUID], "%s must keep its provenance", claim.Name)
				}
				condition := meta.FindStatusCondition(f.cluster.Status.Conditions, conditionScaleDownPVCsRetained)
				require.NotNil(t, condition)
				require.Equal(t, "PVCsRetained", condition.Reason)
				require.Contains(t, condition.Message, "data-payments-2 (origin cannot be established")
				require.Contains(t, condition.Message, "wal-payments-2 (origin cannot be established")
				require.Equal(t, "3", f.statefulSet(t).Annotations[annotationScaledDownFrom])
			})
		}
	}
}

// TestScaleDownRecreatedClusterRestampsAndDeletesReusedClaims proves a Cluster
// recreated under the same name (new UID) whose pods become Ready on its
// predecessor's retained claims re-stamps them with its own UID and can then
// delete them on scale-down.
func TestScaleDownRecreatedClusterRestampsAndDeletesReusedClaims(t *testing.T) {
	t.Parallel()
	cluster, _ := scaleDownOwnershipFixture()
	claims := ownedClaims(cluster, 0, 1, 2)
	for _, claim := range claims {
		claim.Annotations = map[string]string{annotationClusterUID: "previous-cluster-uid"}
	}
	f := newScaleDownReconcileFixture(t, 3, 3, nil, claims...)

	_, err := f.reconcile(t)

	require.NoError(t, err)
	for _, claim := range claims {
		require.Equal(t, "cluster-uid", liveClaim(t, f.clientset, claim.Name).Annotations[annotationClusterUID], "%s must be re-stamped", claim.Name)
	}

	f.cluster.Spec.Replicas = new(int32(1))
	_, err = f.reconcile(t)

	require.NoError(t, err)
	for _, name := range []string{"wal-payments-1", "data-payments-1", "wal-payments-2", "data-payments-2"} {
		require.False(t, f.claimExists(t, name), "%s must be deleted", name)
	}
	require.True(t, f.claimExists(t, "data-payments-0"))
	require.Nil(t, meta.FindStatusCondition(f.cluster.Status.Conditions, conditionScaleDownPVCsRetained))
}

// asAPIServerJSONPatchError returns err as the API server reports a JSON
// patch that fails to apply (a failed "test" operation): 422 Invalid. The
// controller-runtime fake client returns the raw json-patch error instead.
func asAPIServerJSONPatchError(err error) error {
	var status apierrors.APIStatus
	if err == nil || errors.As(err, &status) {
		return err
	}

	return apierrors.NewGenericServerResponse(http.StatusUnprocessableEntity, "", schema.GroupResource{}, "", err.Error(), 0, false)
}

// scaledDownFromClearFixture is a ClusterReconciler over a fake client holding
// a StatefulSet scaled down to 1 with scaled-down-from=3 and no claim left to
// clean up, whose Patch first runs beforePatch on the live object, then
// reports a failed JSON patch the way the API server does. It returns the
// reconciler, the client and the evaluated StatefulSet.
func scaledDownFromClearFixture(t *testing.T, beforePatch func(ctx context.Context, cl client.WithWatch, live *appsv1.StatefulSet)) (*ClusterReconciler, client.Client, *appsv1.StatefulSet) {
	t.Helper()
	ctx := context.Background()
	scheme := resourceNameTestScheme(t)
	_, sts := scaleDownOwnershipFixture()
	sts.Spec.Replicas = new(int32(1))
	sts.Annotations = map[string]string{annotationScaledDownFrom: "3"}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts).WithInterceptorFuncs(interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*appsv1.StatefulSet); ok && beforePatch != nil {
				live := &appsv1.StatefulSet{}
				require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(obj), live))
				beforePatch(ctx, cl, live)
			}

			return asAPIServerJSONPatchError(cl.Patch(ctx, obj, patch, opts...))
		},
	}).Build()
	inspected := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(sts), inspected))

	return &ClusterReconciler{Client: c, Scheme: scheme, Clientset: k8sfake.NewClientset()}, c, inspected
}

// TestClearScaledDownFromNeverPatchesAReplacement proves the patch removing
// the scaled-down-from record is bound to the StatefulSet that was evaluated:
// the interceptor replaces it under the same name (new UID, new
// resourceVersion, same record) between the evaluation and the patch. The
// replacement keeps its record and the failed test is returned for a retry.
func TestClearScaledDownFromNeverPatchesAReplacement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cluster, _ := scaleDownOwnershipFixture()
	r, c, inspected := scaledDownFromClearFixture(t, func(ctx context.Context, cl client.WithWatch, live *appsv1.StatefulSet) {
		require.NoError(t, cl.Delete(ctx, live))
		replacement := live.DeepCopy()
		replacement.UID, replacement.ResourceVersion = "replacement-sts-uid", ""
		require.NoError(t, cl.Create(ctx, replacement))
		// The API server gives every write a new resourceVersion; the fake
		// tracker reuses its initial one on Create, so a write models it.
		replacement.Labels = map[string]string{"replaced": "true"}
		require.NoError(t, cl.Update(ctx, replacement))
	})

	err := r.reconcileScaledDownPVCs(ctx, cluster, inspected, 1, nil)

	require.True(t, apierrors.IsInvalid(err), "the failed test must be returned for a retry: %v", err)
	require.ErrorContains(t, err, "was replaced or its ledger.formance.com/scaled-down-from record changed after it was evaluated")
	live := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(inspected), live))
	require.Equal(t, types.UID("replacement-sts-uid"), live.UID)
	require.NotEqual(t, inspected.ResourceVersion, live.ResourceVersion)
	require.Equal(t, "3", live.Annotations[annotationScaledDownFrom], "the replacement must keep its record")
}

// TestClearScaledDownFromGuardsOnlyUIDAndRecord proves the clear is not
// pinned to the resourceVersion: a status write on the same StatefulSet
// between the evaluation and the patch (what the StatefulSet controller does
// after every replica change) still clears the record, while a record raised
// by a newer scale-down is kept.
func TestClearScaledDownFromGuardsOnlyUIDAndRecord(t *testing.T) {
	t.Parallel()
	cluster, _ := scaleDownOwnershipFixture()

	for _, tc := range []struct {
		name        string
		beforePatch func(ctx context.Context, cl client.WithWatch, live *appsv1.StatefulSet)
		wantRecord  string
	}{
		{name: "unchanged StatefulSet"},
		{
			name: "status written since the evaluation",
			beforePatch: func(ctx context.Context, cl client.WithWatch, live *appsv1.StatefulSet) {
				live.Status.ReadyReplicas, live.Status.ObservedGeneration = 1, 7
				require.NoError(t, cl.Update(ctx, live))
			},
		},
		{
			name: "record changed since the evaluation",
			beforePatch: func(ctx context.Context, cl client.WithWatch, live *appsv1.StatefulSet) {
				live.Annotations[annotationScaledDownFrom] = "5"
				require.NoError(t, cl.Update(ctx, live))
			},
			wantRecord: "5",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			r, c, inspected := scaledDownFromClearFixture(t, tc.beforePatch)

			err := r.reconcileScaledDownPVCs(ctx, cluster, inspected, 1, nil)

			live := &appsv1.StatefulSet{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(inspected), live))
			require.Equal(t, inspected.UID, live.UID)
			if tc.wantRecord == "" {
				require.NoError(t, err)
				require.NotContains(t, live.Annotations, annotationScaledDownFrom)

				return
			}
			require.True(t, apierrors.IsInvalid(err), "the failed test must be returned for a retry: %v", err)
			require.Equal(t, tc.wantRecord, live.Annotations[annotationScaledDownFrom], "a newer record must be kept")
		})
	}
}
