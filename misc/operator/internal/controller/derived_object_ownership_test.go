package controller

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func ownershipTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))

	return scheme
}

// clusterNamed is a Cluster identified by name only (empty UID), matching the
// unit-test Clusters reconciled without a server-assigned UID.
func clusterNamed(name string) *ledgerv1alpha1.Cluster {
	return &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

// controlledByCluster gives obj a controller reference to cluster, as the
// operator does when it creates a derived object, and a UID so the
// UID-preconditioned deletes can target it.
func controlledByCluster[T metav1.Object](obj T, cluster *ledgerv1alpha1.Cluster) T {
	obj.SetOwnerReferences(append(obj.GetOwnerReferences(),
		*metav1.NewControllerRef(cluster, ledgerv1alpha1.GroupVersion.WithKind("Cluster"))))
	if obj.GetUID() == "" {
		obj.SetUID(types.UID(obj.GetName() + "-uid"))
	}

	return obj
}

// controlledByOtherCluster gives obj the controller reference of another
// Cluster, as a same-name object of a different owner carries.
func controlledByOtherCluster[T metav1.Object](obj T) T {
	obj.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: ledgerv1alpha1.GroupVersion.String(), Kind: "Cluster",
		Name: "other", UID: "other-uid", Controller: new(true),
	}})

	return obj
}

// ownedPod is the pod of ordinal that sts controls, as the StatefulSet
// controller creates it.
func ownedPod(sts *appsv1.StatefulSet, ordinal int) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: sts.Name + "-" + strconv.Itoa(ordinal), Namespace: sts.Namespace,
		OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(sts, appsv1.SchemeGroupVersion.WithKind("StatefulSet"))},
	}}
}

func countingExec(calls *int) ledgerctlExec {
	return func(context.Context, *rest.Config, kubernetes.Interface, string, string, string, []string) (*execResult, error) {
		*calls++

		return &execResult{}, nil
	}
}

// TestAdoptionPointRefusesUnownedObject proves a Cluster mutate function
// refuses an existing object under its derived name that it does not control,
// and leaves it untouched, while an owned object is updated normally.
func TestAdoptionPointRefusesUnownedObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := ownershipTestScheme(t)
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns", UID: "uid-app"},
		Spec:       ledgerv1alpha1.ClusterSpec{Replicas: new(int32(3))},
	}
	applyDefaults(cluster)
	for name, foreign := range map[string]*corev1.Service{
		"no controller":   {ObjectMeta: metav1.ObjectMeta{Name: resourceName("app"), Namespace: "ns", Labels: map[string]string{"app": "other"}}},
		"another Cluster": controlledByOtherCluster(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: resourceName("app"), Namespace: "ns"}}),
		"recreated Cluster": controlledByCluster(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: resourceName("app"), Namespace: "ns"}},
			&ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "app", UID: "uid-deleted-app"}}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster.DeepCopy(), foreign).Build()
			before := &corev1.Service{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(foreign), before))
			r := &ClusterReconciler{Client: c, Scheme: scheme}

			err := r.reconcileService(ctx, cluster)
			require.ErrorIs(t, err, errResourceNameConflict)
			require.ErrorContains(t, err, "Service/"+resourceName("app"))

			after := &corev1.Service{}
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(foreign), after))
			require.Equal(t, before, after, "the adoption point must not write the foreign Service")

			// An object the Cluster does control is updated normally.
			owned := controlledByCluster(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: headlessServiceName("app"), Namespace: "ns"}}, cluster)
			require.NoError(t, c.Create(ctx, owned))
			require.NoError(t, r.reconcileHeadlessService(ctx, cluster))
		})
	}
}

// TestReconcileReportsResourceNameConflict proves a conflict raised at an
// adoption point takes the loud path through a full reconcile:
// ResourceNamesAvailable=False, phase Error, a warning event, a bounded
// requeue, and no write to the foreign object. Once the foreign object is
// gone, the next pass converges and marks the condition True.
func TestReconcileReportsResourceNameConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := ownershipTestScheme(t)
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "secure", Namespace: "ns", UID: "uid-secure"},
		Spec: ledgerv1alpha1.ClusterSpec{
			ClusterID: "cluster-id",
			Replicas:  new(int32(3)),
			TLS:       &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "tls"},
		},
	}
	foreign := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: clusterSecretName("secure"), Namespace: "ns"},
		Data:       map[string][]byte{clusterSecretKey: []byte("not-ours")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, foreign).WithStatusSubresource(cluster).Build()
	recorder := record.NewFakeRecorder(16)
	r := &ClusterReconciler{Client: c, Scheme: scheme, Recorder: recorder}
	req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}

	result, err := r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.Equal(t, nameConflictRequeueInterval, result.RequeueAfter)

	stored := &ledgerv1alpha1.Cluster{}
	require.NoError(t, c.Get(ctx, req.NamespacedName, stored))
	condition := meta.FindStatusCondition(stored.Status.Conditions, conditionResourceNamesAvailable)
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, "NameConflict", condition.Reason)
	require.Contains(t, condition.Message, "Secret/"+clusterSecretName("secure")+" exists and is not owned")
	require.Equal(t, "Error", stored.Status.Phase)
	require.Contains(t, <-recorder.Events, "ResourceNameConflict")

	after := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(foreign), after))
	require.Equal(t, []byte("not-ours"), after.Data[clusterSecretKey])
	require.Empty(t, after.OwnerReferences)

	// Removing the conflicting object lets the Cluster converge.
	require.NoError(t, c.Delete(ctx, after))
	_, err = r.Reconcile(ctx, req)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, req.NamespacedName, stored))
	condition = meta.FindStatusCondition(stored.Status.Conditions, conditionResourceNamesAvailable)
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionTrue, condition.Status)
	require.NotEqual(t, "Error", stored.Status.Phase)
}

// TestReconcileReportsCollisionBeforeAnyWrite covers collisions detected by the
// reads at the start of a pass, before any write: Cluster "a-headless" derives
// the Service "ledger-a-headless", which is Cluster "a"'s headless Service, and
// a StatefulSet of another owner under the Cluster's name drives the RC upgrade
// check. Each is a NameConflict (not selector drift, not an RC error) with the
// bounded requeue, and the foreign object is not written.
func TestReconcileReportsCollisionBeforeAnyWrite(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		cluster string
		foreign client.Object
		kind    string
	}{
		{
			name:    "headless Service of another Cluster",
			cluster: "a-headless",
			foreign: controlledByCluster(&corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: headlessServiceName("a"), Namespace: "ns"},
				Spec:       corev1.ServiceSpec{Selector: map[string]string{labelName: "ledger", labelInstance: "a"}},
			}, &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "a", UID: "uid-a"}}),
			kind: "Service",
		},
		{
			name:    "StatefulSet without controller",
			cluster: "app",
			foreign: &appsv1.StatefulSet{
				ObjectMeta: metav1.ObjectMeta{Name: resourceName("app"), Namespace: "ns"},
				Spec: appsv1.StatefulSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "ledger", Image: "ghcr.io/formancehq/ledger:v3.0.0-rc.1"}},
				}}},
			},
			kind: "StatefulSet",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			scheme := ownershipTestScheme(t)
			cluster := &ledgerv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: tc.cluster, Namespace: "ns", UID: types.UID("uid-" + tc.cluster)},
				Spec:       ledgerv1alpha1.ClusterSpec{ClusterID: "cluster-id", Replicas: new(int32(3))},
			}
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, tc.foreign).WithStatusSubresource(cluster).Build()
			before := tc.foreign.DeepCopyObject().(client.Object)
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(tc.foreign), before))
			r := &ClusterReconciler{Client: c, Scheme: scheme, Recorder: record.NewFakeRecorder(16)}
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)}

			result, err := r.Reconcile(ctx, req)
			require.NoError(t, err)
			require.Equal(t, nameConflictRequeueInterval, result.RequeueAfter)

			stored := &ledgerv1alpha1.Cluster{}
			require.NoError(t, c.Get(ctx, req.NamespacedName, stored))
			condition := meta.FindStatusCondition(stored.Status.Conditions, conditionResourceNamesAvailable)
			require.NotNil(t, condition)
			require.Equal(t, "NameConflict", condition.Reason)
			require.Contains(t, condition.Message, tc.kind+"/"+tc.foreign.GetName())
			require.Nil(t, meta.FindStatusCondition(stored.Status.Conditions, "SelectorImmutable"),
				"another owner's selector must not be reported as drift")

			after := tc.foreign.DeepCopyObject().(client.Object)
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(tc.foreign), after))
			require.Equal(t, before, after, "the foreign object must not be written")
			// Nothing else was created under the Cluster's names.
			require.True(t, apierrors.IsNotFound(c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: headlessServiceName(tc.cluster)}, &corev1.Service{})))
		})
	}
}

// TestIsLedgerNotFoundIgnoresExecGateErrors proves a refusal of the exec gate
// is never read as a ledgerctl "not found" answer: a missing StatefulSet's
// lookup error reads "not found", and an index drop or ledger delete would
// otherwise be recorded as done without running.
func TestIsLedgerNotFoundIgnoresExecGateErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	c := fake.NewClientBuilder().WithScheme(ownershipTestScheme(t)).Build()
	_, absent := fetchClusterStatefulSet(ctx, c, &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns"}})
	require.ErrorIs(t, absent, errClusterRuntimeAbsent)
	require.Contains(t, absent.Error(), "not found", "the regression needs the lookup text")

	for _, err := range []error{
		absent,
		nameConflictError("StatefulSet", &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "x"}}, clusterNamed("app")),
		errPodNotOwned,
	} {
		require.False(t, isLedgerNotFound(fmt.Errorf("ledgerctl ledgers: %w", err)), "%v", err)
	}
	require.True(t, isLedgerNotFound(errors.New(`ledger "orders" not found`)))
}

// TestDeleteIfExistsSkipsObjectNotControlledByCluster proves the name-based
// prune of a disabled optional object leaves a same-name object the Cluster
// does not control in place.
func TestDeleteIfExistsSkipsObjectNotControlledByCluster(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := ownershipTestScheme(t)
	cluster := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns", UID: "uid-app"}}
	for name, foreign := range map[string]*corev1.Service{
		"no controller":   {ObjectMeta: metav1.ObjectMeta{Name: grpcServiceName("app"), Namespace: "ns"}},
		"another Cluster": controlledByOtherCluster(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: grpcServiceName("app"), Namespace: "ns"}}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(foreign).Build()
			r := &ClusterReconciler{Client: c, Scheme: scheme}

			// IngressGrpc is unset, so the gRPC Service is disabled and pruned.
			require.NoError(t, r.reconcileGrpcService(ctx, cluster))
			require.NoError(t, r.pruneDisabledOptionalServices(ctx, cluster))

			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(foreign), &corev1.Service{}),
				"a Service the Cluster does not control must never be pruned")
		})
	}
}

// TestUpdateStatusIgnoresStatefulSetNotControlledByCluster proves the status
// pass never reports the readiness of a StatefulSet the Cluster did not create.
func TestUpdateStatusIgnoresStatefulSetNotControlledByCluster(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := ownershipTestScheme(t)
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns", UID: "uid-app"},
		Spec:       ledgerv1alpha1.ClusterSpec{Replicas: new(int32(3))},
	}
	foreign := controlledByOtherCluster(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName("app"), Namespace: "ns"},
		Status:     appsv1.StatefulSetStatus{ReadyReplicas: 3},
	})
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, foreign).WithStatusSubresource(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	require.NoError(t, r.updateStatus(ctx, cluster.DeepCopy()))

	stored := &ledgerv1alpha1.Cluster{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(cluster), stored))
	require.Equal(t, "Pending", stored.Status.Phase)
	require.Zero(t, stored.Status.ReadyReplicas)
}

// ledgerOwnershipFixture is a Ledger referencing a Running Cluster read
// through the dynamic client, as the Ledger controller does.
type ledgerOwnershipFixture struct {
	scheme  *runtime.Scheme
	ledger  *ledgerv1alpha1.Ledger
	cluster *unstructured.Unstructured
}

func newLedgerOwnershipFixture(t *testing.T) ledgerOwnershipFixture {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	return ledgerOwnershipFixture{
		scheme: scheme,
		ledger: &ledgerv1alpha1.Ledger{
			ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "test", UID: "ledger-uid", Finalizers: []string{ledgerFinalizer}},
			Spec:       ledgerv1alpha1.LedgerCRDSpec{ClusterRef: "main", Name: "orders"},
		},
		cluster: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "ledger.formance.com/v1alpha1", "kind": "Cluster",
			"metadata": map[string]any{"name": "main", "namespace": "test", "uid": "cluster-uid"},
			"status":   map[string]any{"phase": "Running"},
		}},
	}
}

// ownedStatefulSet is the referenced Cluster's own StatefulSet.
func (f ledgerOwnershipFixture) ownedStatefulSet() *appsv1.StatefulSet {
	return controlledByCluster(&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: resourceName("main"), Namespace: "test", UID: "main-sts-uid",
	}}, &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "main", UID: "cluster-uid"}})
}

// reconciler builds a LedgerReconciler over objects (controller-runtime
// client) and pods (the typed clientset the exec path reads pods with).
func (f ledgerOwnershipFixture) reconciler(objects []client.Object, pods []runtime.Object, exec ledgerctlExec) (*LedgerReconciler, client.Client) {
	c := fake.NewClientBuilder().WithScheme(f.scheme).
		WithObjects(append([]client.Object{f.ledger}, objects...)...).
		WithStatusSubresource(f.ledger).Build()

	return &LedgerReconciler{
		Client: c, Scheme: f.scheme, Clientset: k8sfake.NewClientset(pods...),
		Dynamic: dynamicfake.NewSimpleDynamicClient(f.scheme, f.cluster), exec: exec,
	}, c
}

// TestLedgerExecsIntoOwnedClusterPod is the positive control of the exec
// gate: an owned StatefulSet controlling pod-0 lets the create exec run.
func TestLedgerExecsIntoOwnedClusterPod(t *testing.T) {
	t.Parallel()
	f := newLedgerOwnershipFixture(t)
	var pods []string
	exec := func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, _, pod, _ string, _ []string) (*execResult, error) {
		pods = append(pods, pod)

		return &execResult{}, nil
	}
	sts := f.ownedStatefulSet()
	r, c := f.reconciler([]client.Object{sts}, []runtime.Object{ownedPod(sts, 0)}, exec)

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.ledger)})
	require.NoError(t, err)
	require.Equal(t, []string{resourceName("main") + "-0"}, pods)

	stored := &ledgerv1alpha1.Ledger{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(f.ledger), stored))
	require.Equal(t, ledgerv1alpha1.LedgerPhaseReady, stored.Status.Phase)
}

// TestLedgerRefusesStatefulSetNotControlledByCluster proves the Ledger
// controller never execs into a StatefulSet under the referenced Cluster's
// name that the Cluster does not control (another Cluster's, or the old
// StatefulSet of a deleted Cluster recreated under the same name).
func TestLedgerRefusesStatefulSetNotControlledByCluster(t *testing.T) {
	t.Parallel()
	for name, foreign := range map[string]*appsv1.StatefulSet{
		"no controller":   {ObjectMeta: metav1.ObjectMeta{Name: resourceName("main"), Namespace: "test", UID: "foreign-uid"}},
		"another Cluster": controlledByOtherCluster(&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName("main"), Namespace: "test", UID: "foreign-uid"}}),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := newLedgerOwnershipFixture(t)
			execCalls := 0
			r, c := f.reconciler([]client.Object{foreign}, []runtime.Object{ownedPod(foreign, 0)}, countingExec(&execCalls))

			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.ledger)})
			require.NoError(t, err)
			require.Zero(t, execCalls, "must not exec into pods the Cluster does not own")

			stored := &ledgerv1alpha1.Ledger{}
			require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(f.ledger), stored))
			require.Equal(t, ledgerv1alpha1.LedgerPhasePending, stored.Status.Phase)
			condition := meta.FindStatusCondition(stored.Status.Conditions, conditionLedgerSynced)
			require.NotNil(t, condition)
			require.Contains(t, condition.Message, errResourceNameConflict.Error())
		})
	}
}

// TestLedgerNeverExecsIntoPodNotControlledByClusterStatefulSet proves the
// Ledger controller checks the target pod itself, not only the StatefulSet:
// the Cluster controls the StatefulSet, but pod-0 is still controlled by an
// older StatefulSet UID (garbage collection of a deleted Cluster's runtime),
// has no controller (orphaned during StatefulSet recreation), or does not
// exist. Neither the create nor the delete path execs; each keeps its reaction
// to a failed exec (create: Pending with the reason; delete: best-effort, the
// finalizer is released).
func TestLedgerNeverExecsIntoPodNotControlledByClusterStatefulSet(t *testing.T) {
	t.Parallel()
	oldStatefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName("main"), Namespace: "test", UID: "old-sts-uid"}}
	orphan := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: resourceName("main") + "-0", Namespace: "test"}}
	for _, tc := range []struct {
		name     string
		pods     []runtime.Object
		deleting bool
		reason   string
	}{
		{"create, pod-0 of an older StatefulSet", []runtime.Object{ownedPod(oldStatefulSet, 0)}, false, "uid old-sts-uid"},
		{"delete, pod-0 of an older StatefulSet", []runtime.Object{ownedPod(oldStatefulSet, 0)}, true, "uid old-sts-uid"},
		{"create, orphaned pod-0", []runtime.Object{orphan}, false, "has no controller"},
		{"create, pod-0 missing", nil, false, "does not exist"},
		{"delete, pod-0 missing", nil, true, "does not exist"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newLedgerOwnershipFixture(t)
			if tc.deleting {
				now := metav1.Now()
				f.ledger.DeletionTimestamp = &now
			}
			execCalls := 0
			r, c := f.reconciler([]client.Object{f.ownedStatefulSet()}, tc.pods, countingExec(&execCalls))

			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.ledger)})
			require.NoError(t, err)
			require.Zero(t, execCalls, "must not exec into a pod the Cluster's StatefulSet does not control")
			// The refusal comes from the pod check (the StatefulSet is owned).
			var podReads []string
			for _, action := range r.Clientset.(*k8sfake.Clientset).Actions() {
				if get, ok := action.(k8stesting.GetAction); ok && action.GetResource().Resource == "pods" {
					podReads = append(podReads, get.GetName())
				}
			}
			require.Equal(t, []string{resourceName("main") + "-0"}, podReads)

			stored := &ledgerv1alpha1.Ledger{}
			getErr := c.Get(context.Background(), client.ObjectKeyFromObject(f.ledger), stored)
			if tc.deleting {
				// Releasing the last finalizer of a deleting object removes it.
				if getErr == nil {
					require.NotContains(t, stored.Finalizers, ledgerFinalizer)
				} else {
					require.True(t, apierrors.IsNotFound(getErr), "reading the released Ledger: %v", getErr)
				}

				return
			}
			require.NoError(t, getErr)
			require.Equal(t, ledgerv1alpha1.LedgerPhasePending, stored.Status.Phase)
			condition := meta.FindStatusCondition(stored.Status.Conditions, conditionLedgerSynced)
			require.NotNil(t, condition)
			require.Equal(t, "CreateFailed", condition.Reason)
			require.Contains(t, condition.Message, errPodNotOwned.Error())
			require.Contains(t, condition.Message, tc.reason)
		})
	}
}

// TestLedgerDeleteNeverExecsIntoForeignOrAbsentRuntime proves a deleting Ledger
// does not exec "ledgers delete" when its Cluster has no StatefulSet (while an
// orphaned pod-0 may still run) or when the StatefulSet under its name is
// controlled by another Cluster. Like a missing Cluster, the finalizer is
// released without touching the foreign pods.
func TestLedgerDeleteNeverExecsIntoForeignOrAbsentRuntime(t *testing.T) {
	t.Parallel()
	foreign := controlledByOtherCluster(&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName("main"), Namespace: "test", UID: "foreign-uid"}})
	for _, tc := range []struct {
		name    string
		objects []client.Object
	}{
		{"StatefulSet missing", nil},
		{"StatefulSet controlled by another Cluster", []client.Object{foreign}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newLedgerOwnershipFixture(t)
			now := metav1.Now()
			f.ledger.DeletionTimestamp = &now
			execCalls := 0
			r, c := f.reconciler(tc.objects, []runtime.Object{ownedPod(foreign, 0)}, countingExec(&execCalls))

			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.ledger)})
			require.NoError(t, err)
			require.Zero(t, execCalls, "must not exec into pods the Cluster does not own")

			stored := &ledgerv1alpha1.Ledger{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(f.ledger), stored); err == nil {
				require.NotContains(t, stored.Finalizers, ledgerFinalizer)
			} else {
				require.True(t, apierrors.IsNotFound(err), "reading the released Ledger: %v", err)
			}
		})
	}
}

// TestLedgerIndexReconcileNeverExecsIntoPodOfAnotherStatefulSet proves the
// ledger-index path (a Ready Ledger reconciling spec.indexes) is gated by the
// same pod check: pod-0 is controlled by an older StatefulSet UID, so no
// ledgerctl command runs and IndexesSynced reports the refusal.
func TestLedgerIndexReconcileNeverExecsIntoPodOfAnotherStatefulSet(t *testing.T) {
	t.Parallel()
	f := newLedgerOwnershipFixture(t)
	f.ledger.Spec.Indexes = &ledgerv1alpha1.LedgerIndexesSpec{Transaction: []string{"reference"}}
	f.ledger.Status = ledgerv1alpha1.LedgerCRDStatus{Phase: ledgerv1alpha1.LedgerPhaseReady, Mode: "normal", AppliedSpecHash: computeLedgerSpecHash(&f.ledger.Spec)}
	oldStatefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName("main"), Namespace: "test", UID: "old-sts-uid"}}
	execCalls := 0
	r, c := f.reconciler([]client.Object{f.ownedStatefulSet()}, []runtime.Object{ownedPod(oldStatefulSet, 0)}, countingExec(&execCalls))

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.ledger)})
	require.NoError(t, err)
	require.Zero(t, execCalls, "must not exec into a pod the Cluster's StatefulSet does not control")

	stored := &ledgerv1alpha1.Ledger{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(f.ledger), stored))
	condition := meta.FindStatusCondition(stored.Status.Conditions, conditionIndexesSynced)
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Contains(t, condition.Message, errPodNotOwned.Error())
	require.Contains(t, condition.Message, "uid old-sts-uid")
}

// scaleDownOwnershipFixture is a Cluster whose owned StatefulSet is scaled
// from 3 to 1 replicas.
func scaleDownOwnershipFixture() (*ledgerv1alpha1.Cluster, *appsv1.StatefulSet) {
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "ns", UID: "cluster-uid"},
		Spec:       ledgerv1alpha1.ClusterSpec{Replicas: new(int32(1))},
	}
	applyDefaults(cluster)
	sts := controlledByCluster(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName("orders"), Namespace: "ns", UID: "sts-uid"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: new(int32(3)),
			Selector: &metav1.LabelSelector{MatchLabels: selectorLabels(cluster)},
		},
	}, cluster)

	return cluster, sts
}

// TestScaleDownNeverExecsIntoPodOfAnotherStatefulSet proves the Raft
// scale-down checks pod-0 itself: the Cluster controls the StatefulSet, but
// pod-0 is still controlled by an older StatefulSet UID, so no ledgerctl
// command runs and the replicas are not reduced.
func TestScaleDownNeverExecsIntoPodOfAnotherStatefulSet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := ownershipTestScheme(t)
	cluster, sts := scaleDownOwnershipFixture()
	oldStatefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: sts.Name, Namespace: "ns", UID: "old-sts-uid"}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, sts).Build()
	clientset := k8sfake.NewClientset(ownedPod(oldStatefulSet, 0))
	r := &ClusterReconciler{Client: c, Scheme: scheme, Config: &rest.Config{}, Clientset: clientset}
	execCalls := 0

	_, err := r.reconcileStatefulSetWithExec(ctx, cluster, "hash", nil, countingExec(&execCalls))

	require.ErrorIs(t, err, errPodNotOwned)
	require.ErrorContains(t, err, "pod ns/"+sts.Name+"-0 is controlled by StatefulSet/"+sts.Name+" (uid old-sts-uid), not StatefulSet "+sts.Name+" (uid sts-uid)")
	require.Zero(t, execCalls, "must not exec into a pod the Cluster's StatefulSet does not control")
	live := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(sts), live))
	require.Equal(t, int32(3), *live.Spec.Replicas, "replicas must not drop before the Raft removal")
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
	oldStatefulSet := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: sts.Name, Namespace: "ns", UID: "old-sts-uid"}}
	clientset := k8sfake.NewClientset(ownedPod(oldStatefulSet, 0))

	t.Run("volume expansion disk usage", func(t *testing.T) {
		t.Parallel()
		r := &VolumeExpansionReconciler{Clientset: clientset}

		results := r.collectDiskUsage(ctx, cluster, sts, tlsModeDisabled, 1)

		require.Len(t, results, 1)
		require.Equal(t, sts.Name+"-0", results[0].Pod)
		require.ErrorIs(t, results[0].Err, errPodNotOwned)
		require.ErrorContains(t, results[0].Err, "uid old-sts-uid")
	})

	t.Run("EventSink ledgerctl", func(t *testing.T) {
		t.Parallel()
		scheme := ownershipTestScheme(t)
		c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sts.DeepCopy()).Build()
		r := &ClusterReconciler{Client: c, Scheme: scheme, Clientset: clientset}

		_, err := r.ledgerctlExecOutput(ctx, cluster, cluster.Spec.GrpcPort, "events", "list", "--json")

		require.ErrorIs(t, err, errPodNotOwned)
		require.ErrorContains(t, err, "uid old-sts-uid")
	})
}

// TestRequirePodControlledByRejectsPodWithoutController proves an orphaned pod
// (no controller reference, e.g. after the VolumeClaimTemplate
// orphan-and-recreate path, before the new StatefulSet adopts it) is refused.
func TestRequirePodControlledByRejectsPodWithoutController(t *testing.T) {
	t.Parallel()
	_, sts := scaleDownOwnershipFixture()
	orphan := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: sts.Name + "-0", Namespace: "ns", UID: "pod-uid"}}

	err := requirePodControlledBy(context.Background(), k8sfake.NewClientset(orphan), sts, "ns", sts.Name+"-0")

	require.ErrorIs(t, err, errPodNotOwned)
	require.EqualError(t, err, "pod not controlled by the Cluster StatefulSet: pod ns/"+sts.Name+"-0 has no controller, expected StatefulSet "+sts.Name+" (uid sts-uid)")
}

// TestVolumeExpansionRefusesStatefulSetNotControlledByCluster proves the volume
// expansion controller never measures or expands the PVCs of a StatefulSet
// under the Cluster's name that another Cluster controls.
func TestVolumeExpansionRefusesStatefulSetNotControlledByCluster(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := ownershipTestScheme(t)
	maximum := resource.MustParse("200Gi")
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "intruder", Namespace: "test", UID: "intruder-uid"},
		Spec: ledgerv1alpha1.ClusterSpec{
			Persistence: ledgerv1alpha1.PersistenceSpec{Data: ledgerv1alpha1.VolumeSpec{
				AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: &maximum},
			}},
		},
	}
	foreign := controlledByOtherCluster(&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName("intruder"), Namespace: "test"}})
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, foreign).WithStatusSubresource(cluster).Build()
	measured := 0
	r := &VolumeExpansionReconciler{
		Client: kube, APIReader: kube, Recorder: record.NewFakeRecorder(10),
		ReadDiskUsage: func(context.Context, *ledgerv1alpha1.Cluster, string, string) (podDiskUsage, error) {
			measured++

			return podDiskUsage{}, nil
		},
	}

	result, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(cluster)})
	require.NoError(t, err)
	require.Equal(t, volumeExpansionRetryInterval, result.RequeueAfter)
	require.Zero(t, measured, "another Cluster's pods must not be measured")

	stored := &ledgerv1alpha1.Cluster{}
	require.NoError(t, kube.Get(ctx, client.ObjectKeyFromObject(cluster), stored))
	condition := meta.FindStatusCondition(stored.Status.Conditions, dataVolumeExpansionCondition)
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, "ResourceNameConflict", condition.Reason)
}

// TestEventSinkRefusesStatefulSetNotControlledByCluster proves the EventSink
// controller refuses a StatefulSet under the Cluster's name that another
// Cluster controls instead of configuring sinks on its runtime, and that a
// deleting sink releases its finalizer rather than looping on it.
func TestEventSinkRefusesStatefulSetNotControlledByCluster(t *testing.T) {
	t.Parallel()
	for _, deleting := range []bool{false, true} {
		t.Run("deleting="+strconv.FormatBool(deleting), func(t *testing.T) {
			t.Parallel()
			scheme := ownershipTestScheme(t)
			sink := testEventSink()
			sink.Namespace = "test"
			sink.Finalizers = []string{eventSinkFinalizer}
			if deleting {
				now := metav1.Now()
				sink.DeletionTimestamp = &now
			}
			cluster := &ledgerv1alpha1.Cluster{ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "test", UID: "cluster-uid"}}
			foreign := controlledByOtherCluster(&appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: resourceName("cluster"), Namespace: "test"}})
			kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sink, cluster, foreign).WithStatusSubresource(sink).Build()
			r := &EventSinkReconciler{Client: kube, Scheme: scheme}
			key := client.ObjectKeyFromObject(sink)

			result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
			require.NoError(t, err)

			var stored ledgerv1alpha1.EventSink
			getErr := kube.Get(context.Background(), key, &stored)
			if deleting {
				if getErr == nil {
					require.NotContains(t, stored.Finalizers, eventSinkFinalizer)
				} else {
					require.True(t, apierrors.IsNotFound(getErr), "reading the released EventSink: %v", getErr)
				}

				return
			}
			require.NoError(t, getErr)
			require.Equal(t, sinkDriftCheckInterval, result.RequeueAfter)
			condition := meta.FindStatusCondition(stored.Status.Conditions, "Synced")
			require.NotNil(t, condition)
			require.Equal(t, metav1.ConditionFalse, condition.Status)
			require.Equal(t, "ResourceNameConflict", condition.Reason)
		})
	}
}

// TestBackupRunRequiresClusterService proves a backup Job is created only
// when the ClusterIP Service it dials is controlled by the Cluster: a missing
// Service, or one the Cluster does not control, creates no Job and is retried.
func TestBackupRunRequiresClusterService(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		service      func(*ledgerv1alpha1.Cluster) *corev1.Service
		wantConflict bool
	}{
		{"missing", nil, false},
		{"no controller", func(c *ledgerv1alpha1.Cluster) *corev1.Service {
			return &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: resourceName(c.Name), Namespace: c.Namespace}}
		}, true},
		{"another Cluster", func(c *ledgerv1alpha1.Cluster) *corev1.Service {
			return controlledByOtherCluster(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: resourceName(c.Name), Namespace: c.Namespace}})
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, run := backupRunRecoveryFixture(t)
			ctx := t.Context()
			cluster := &ledgerv1alpha1.Cluster{}
			backup := &ledgerv1alpha1.Backup{}
			require.NoError(t, c.Get(ctx, types.NamespacedName{Name: "cluster", Namespace: run.Namespace}, cluster))
			require.NoError(t, c.Get(ctx, types.NamespacedName{Name: run.Spec.BackupRef, Namespace: run.Namespace}, backup))
			require.NoError(t, c.Delete(ctx, backupClusterService(cluster)))
			if tc.service != nil {
				require.NoError(t, c.Create(ctx, tc.service(cluster)))
			}
			r := &BackupRunReconciler{Client: c, APIReader: c, Scheme: c.Scheme()}

			job, err := r.ensureBackupJob(ctx, run, backup, cluster)
			if tc.wantConflict {
				require.ErrorIs(t, err, errResourceNameConflict)
			} else {
				require.ErrorIs(t, err, errClusterServiceAbsent)
			}
			require.Nil(t, job)
			var jobs batchv1.JobList
			require.NoError(t, c.List(ctx, &jobs))
			require.Empty(t, jobs.Items)
		})
	}
}

// TestBackupRunReadsClusterServiceUncached proves the Service check reads the
// API server, not the informer cache: when the cache still holds the owned
// Service but another Cluster's Service has already taken its name, no Job is
// created.
func TestBackupRunReadsClusterServiceUncached(t *testing.T) {
	t.Parallel()
	cache, run := backupRunRecoveryFixture(t)
	live, _ := backupRunRecoveryFixture(t)
	ctx := t.Context()
	cluster := &ledgerv1alpha1.Cluster{}
	backup := &ledgerv1alpha1.Backup{}
	require.NoError(t, cache.Get(ctx, types.NamespacedName{Name: "cluster", Namespace: run.Namespace}, cluster))
	require.NoError(t, cache.Get(ctx, types.NamespacedName{Name: run.Spec.BackupRef, Namespace: run.Namespace}, backup))
	require.NoError(t, live.Delete(ctx, backupClusterService(cluster)))
	require.NoError(t, live.Create(ctx, controlledByOtherCluster(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: resourceName(cluster.Name), Namespace: cluster.Namespace}})))
	r := &BackupRunReconciler{Client: cache, APIReader: live, Scheme: cache.Scheme()}

	job, err := r.ensureBackupJob(ctx, run, backup, cluster)
	require.ErrorIs(t, err, errResourceNameConflict)
	require.Nil(t, job)
	var jobs batchv1.JobList
	require.NoError(t, cache.List(ctx, &jobs))
	require.Empty(t, jobs.Items)
}
