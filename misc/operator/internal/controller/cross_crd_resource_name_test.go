package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// resourceNameCrossCRDFixture is a Ledger referencing a Running Cluster whose
// base name is overridden by spec.resourceName.
type resourceNameCrossCRDFixture struct {
	scheme  *runtime.Scheme
	ledger  *ledgerv1alpha1.Ledger
	cluster *unstructured.Unstructured
}

func newResourceNameCrossCRDFixture(t *testing.T) resourceNameCrossCRDFixture {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))

	return resourceNameCrossCRDFixture{
		scheme: scheme,
		ledger: &ledgerv1alpha1.Ledger{
			ObjectMeta: metav1.ObjectMeta{Name: "orders", Namespace: "test", UID: "ledger-uid", Finalizers: []string{ledgerFinalizer}},
			Spec:       ledgerv1alpha1.LedgerCRDSpec{ClusterRef: "jdxmvkvwlyiy-cukg", Name: "orders"},
		},
		cluster: &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "ledger.formance.com/v1alpha1", "kind": "Cluster",
			"metadata": map[string]any{"name": "jdxmvkvwlyiy-cukg", "namespace": "test", "uid": "cluster-uid"},
			"spec":     map[string]any{"resourceName": "payments"},
			"status":   map[string]any{"phase": "Running"},
		}},
	}
}

// ownedStatefulSet is the referenced Cluster's own StatefulSet "payments".
func (f resourceNameCrossCRDFixture) ownedStatefulSet() *appsv1.StatefulSet {
	return &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: "payments", Namespace: "test",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: ledgerv1alpha1.GroupVersion.String(), Kind: "Cluster",
			Name: "jdxmvkvwlyiy-cukg", UID: types.UID("cluster-uid"), Controller: new(true),
		}},
	}}
}

func (f resourceNameCrossCRDFixture) reconciler(objects []client.Object, exec ledgerctlExec) (*LedgerReconciler, client.Client) {
	c := fake.NewClientBuilder().WithScheme(f.scheme).
		WithObjects(append([]client.Object{f.ledger}, objects...)...).
		WithStatusSubresource(f.ledger).Build()

	return &LedgerReconciler{Client: c, Scheme: f.scheme, Dynamic: dynamicfake.NewSimpleDynamicClient(f.scheme, f.cluster), exec: exec}, c
}

// TestLedgerDialsOverriddenClusterPod proves the Ledger controller, which only
// has a Cluster reference name, resolves the referenced Cluster and execs into
// "<resourceName>-0" dialing "<resourceName>-headless" — never "ledger-<name>".
func TestLedgerDialsOverriddenClusterPod(t *testing.T) {
	t.Parallel()
	f := newResourceNameCrossCRDFixture(t)

	type call struct{ namespace, pod, script string }
	var calls []call
	exec := func(_ context.Context, _ *rest.Config, _ kubernetes.Interface, namespace, pod, _ string, command []string) (*execResult, error) {
		calls = append(calls, call{namespace, pod, command[len(command)-1]})

		return &execResult{}, nil
	}
	r, c := f.reconciler([]client.Object{f.ownedStatefulSet()}, exec)

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.ledger)})
	require.NoError(t, err)

	require.Len(t, calls, 1)
	require.Equal(t, "test", calls[0].namespace)
	require.Equal(t, "payments-0", calls[0].pod)
	require.Contains(t, calls[0].script, "$POD_NAME.payments-headless.$POD_NAMESPACE.svc.cluster.local:8888")
	require.NotContains(t, calls[0].script, "ledger-jdxmvkvwlyiy-cukg")

	stored := &ledgerv1alpha1.Ledger{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(f.ledger), stored))
	require.Equal(t, ledgerv1alpha1.LedgerPhaseReady, stored.Status.Phase)
}

// TestLedgerRefusesConflictingClusterStatefulSet proves the Ledger controller
// never execs into a StatefulSet that sits under the referenced Cluster's base
// name but is controlled by another Cluster (spec.resourceName collision).
func TestLedgerRefusesConflictingClusterStatefulSet(t *testing.T) {
	t.Parallel()
	f := newResourceNameCrossCRDFixture(t)
	foreign := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: "payments", Namespace: "test",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: ledgerv1alpha1.GroupVersion.String(), Kind: "Cluster",
			Name: "other", UID: types.UID("other-uid"), Controller: new(true),
		}},
	}}
	execCalls := 0
	exec := func(context.Context, *rest.Config, kubernetes.Interface, string, string, string, []string) (*execResult, error) {
		execCalls++

		return &execResult{}, nil
	}
	r, c := f.reconciler([]client.Object{foreign}, exec)

	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.ledger)})
	require.NoError(t, err)
	require.Zero(t, execCalls, "must not exec into another Cluster's pods")

	stored := &ledgerv1alpha1.Ledger{}
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(f.ledger), stored))
	require.Equal(t, ledgerv1alpha1.LedgerPhasePending, stored.Status.Phase)
	condition := meta.FindStatusCondition(stored.Status.Conditions, conditionLedgerSynced)
	require.NotNil(t, condition)
	require.Contains(t, condition.Message, errResourceNameConflict.Error())
}

// TestLedgerDeleteNeverExecsIntoForeignOrAbsentRuntime proves a deleting Ledger
// does not exec "ledgers delete" when its Cluster has no StatefulSet (e.g. a
// same-base-name Cluster in NameConflict while a deleted Cluster's orphaned
// "<base>-0" pod still runs) or when the StatefulSet under the base name is
// controlled by another Cluster. Like a missing Cluster, the finalizer is
// released without touching the foreign pods.
func TestLedgerDeleteNeverExecsIntoForeignOrAbsentRuntime(t *testing.T) {
	t.Parallel()
	foreign := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: "payments", Namespace: "test",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: ledgerv1alpha1.GroupVersion.String(), Kind: "Cluster",
			Name: "other", UID: types.UID("other-uid"), Controller: new(true),
		}},
	}}
	for _, tc := range []struct {
		name    string
		objects []client.Object
	}{
		{"StatefulSet missing", nil},
		{"StatefulSet controlled by another Cluster", []client.Object{foreign}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f := newResourceNameCrossCRDFixture(t)
			now := metav1.Now()
			f.ledger.DeletionTimestamp = &now
			execCalls := 0
			exec := func(context.Context, *rest.Config, kubernetes.Interface, string, string, string, []string) (*execResult, error) {
				execCalls++

				return &execResult{}, nil
			}
			r, c := f.reconciler(tc.objects, exec)

			_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.ledger)})
			require.NoError(t, err)
			require.Zero(t, execCalls, "must not exec into pods the Cluster does not own")

			stored := &ledgerv1alpha1.Ledger{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(f.ledger), stored); err == nil {
				require.NotContains(t, stored.Finalizers, ledgerFinalizer)
			}
		})
	}
}

// TestVolumeExpansionRefusesConflictingClusterStatefulSet proves the volume
// expansion controller never measures or expands the PVCs of a StatefulSet that
// sits under the Cluster base name but is controlled by another Cluster.
func TestVolumeExpansionRefusesConflictingClusterStatefulSet(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	maximum := resource.MustParse("200Gi")
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "intruder", Namespace: "test", UID: "intruder-uid"},
		Spec: ledgerv1alpha1.ClusterSpec{
			ResourceName: "payments",
			Persistence: ledgerv1alpha1.PersistenceSpec{Data: ledgerv1alpha1.VolumeSpec{
				AutoExpansion: &ledgerv1alpha1.VolumeAutoExpansionSpec{Enabled: true, MaximumSize: &maximum},
			}},
		},
	}
	foreign := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: "payments", Namespace: "test",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: ledgerv1alpha1.GroupVersion.String(), Kind: "Cluster",
			Name: "other", UID: types.UID("other-uid"), Controller: new(true),
		}},
	}}
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

	stored := getCluster(t, kube, cluster.Namespace, cluster.Name)
	condition := meta.FindStatusCondition(stored.Status.Conditions, dataVolumeExpansionCondition)
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, "ResourceNameConflict", condition.Reason)
}

// TestEventSinkRefusesConflictingClusterStatefulSet proves the EventSink
// controller resolves the overridden base name and refuses a StatefulSet
// controlled by another Cluster instead of configuring sinks on its runtime.
func TestEventSinkRefusesConflictingClusterStatefulSet(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	sink := testEventSink()
	sink.Namespace = "test"
	sink.Finalizers = []string{eventSinkFinalizer}
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "cluster", Namespace: "test", UID: "cluster-uid"},
		Spec:       ledgerv1alpha1.ClusterSpec{ResourceName: "payments"},
	}
	foreign := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{
		Name: "payments", Namespace: "test",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: ledgerv1alpha1.GroupVersion.String(), Kind: "Cluster",
			Name: "other", UID: types.UID("other-uid"), Controller: new(true),
		}},
	}}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(sink, cluster, foreign).WithStatusSubresource(sink).Build()
	r := &EventSinkReconciler{Client: kube, Scheme: scheme}
	key := types.NamespacedName{Name: sink.Name, Namespace: sink.Namespace}

	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: key})
	require.NoError(t, err)
	require.Equal(t, sinkDriftCheckInterval, result.RequeueAfter)

	var stored ledgerv1alpha1.EventSink
	require.NoError(t, kube.Get(context.Background(), key, &stored))
	condition := meta.FindStatusCondition(stored.Status.Conditions, "Synced")
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, "ResourceNameConflict", condition.Reason)
}
