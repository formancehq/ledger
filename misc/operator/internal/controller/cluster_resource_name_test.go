package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func resourceNameTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, clientgoscheme.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))

	return scheme
}

// reconcileCluster runs one Reconcile pass for the named Cluster.
func reconcileCluster(t *testing.T, r *ClusterReconciler, namespace, name string) reconcile.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	require.NoError(t, err)

	return result
}

func getCluster(t *testing.T, c client.Client, namespace, name string) *ledgerv1alpha1.Cluster {
	t.Helper()
	cluster := &ledgerv1alpha1.Cluster{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, cluster))

	return cluster
}

// TestReconcileUsesResourceNameOverride drives a full Cluster reconcile and
// proves spec.resourceName replaces the whole "ledger-<name>" base for every
// derived object and every in-cluster address the pods and status carry.
func TestReconcileUsesResourceNameOverride(t *testing.T) {
	t.Parallel()
	const (
		namespace = "ns"
		crName    = "jdxmvkvwlyiy-cukg"
	)
	scheme := resourceNameTestScheme(t)
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: namespace, UID: "uid-override"},
		Spec:       ledgerv1alpha1.ClusterSpec{ResourceName: "payments", Replicas: new(int32(3))},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).WithStatusSubresource(cluster).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	reconcileCluster(t, r, namespace, crName)

	ctx := context.Background()
	sts := &appsv1.StatefulSet{}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "payments"}, sts))
	require.Equal(t, "payments-headless", sts.Spec.ServiceName)
	require.Equal(t, "payments", sts.Spec.Template.Spec.ServiceAccountName)
	for _, name := range []string{"payments", "payments-headless"} {
		require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &corev1.Service{}), "Service %s", name)
	}
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "payments"}, &corev1.ServiceAccount{}))

	// Nothing may be created under the default base name.
	require.Error(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "ledger-" + crName}, &appsv1.StatefulSet{}))
	require.Error(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "ledger-" + crName + "-headless"}, &corev1.Service{}))

	// Raft bootstrap/join, advertise and ledgerctl addresses use the override.
	var container *corev1.Container
	for i := range sts.Spec.Template.Spec.Containers {
		if sts.Spec.Template.Spec.Containers[i].Name == ledgerContainer {
			container = &sts.Spec.Template.Spec.Containers[i]
		}
	}
	require.NotNil(t, container)
	command := strings.Join(container.Command, " ")
	require.Contains(t, command, "payments-0.payments-headless.${POD_NAMESPACE}.svc.cluster.local")
	require.NotContains(t, command, "ledger-"+crName)
	env := map[string]string{}
	for _, e := range container.Env {
		env[e.Name] = e.Value
	}
	require.Contains(t, env["ADVERTISE_ADDR"], ".payments-headless.")

	stored := getCluster(t, c, namespace, crName)
	require.NotNil(t, stored.Status.Endpoints)
	require.Equal(t, "payments.ns.svc.cluster.local:8888", stored.Status.Endpoints.GRPC)
	require.Equal(t, "http://payments.ns.svc.cluster.local:9000", stored.Status.Endpoints.HTTP)
	condition := meta.FindStatusCondition(stored.Status.Conditions, conditionResourceNamesAvailable)
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionTrue, condition.Status)
}

// TestReconcileRefusesUnownedObjectUnderBaseName proves an existing object
// without any controller reference under a derived name (e.g. a Ledger v2
// "ledger" Service in the same namespace) is never adopted: the Cluster fails
// loudly and the object is left byte-for-byte unchanged, with no owner
// reference added and no other object created.
func TestReconcileRefusesUnownedObjectUnderBaseName(t *testing.T) {
	t.Parallel()
	const namespace = "ns"
	ctx := context.Background()
	scheme := resourceNameTestScheme(t)
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "v3", Namespace: namespace, UID: "uid-v3"},
		Spec:       ledgerv1alpha1.ClusterSpec{ResourceName: "ledger", Replicas: new(int32(3))},
	}
	v2Service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "ledger", Namespace: namespace, Labels: map[string]string{"app": "ledger-v2"}},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": "ledger-v2"},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 8080}},
		},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, v2Service).WithStatusSubresource(cluster).Build()
	before := &corev1.Service{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(v2Service), before))
	recorder := record.NewFakeRecorder(16)
	r := &ClusterReconciler{Client: c, Scheme: scheme, Recorder: recorder}

	result := reconcileCluster(t, r, namespace, cluster.Name)
	require.Equal(t, nameConflictRequeueInterval, result.RequeueAfter)

	after := &corev1.Service{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(v2Service), after))
	require.Equal(t, before, after, "the unowned Service must be left byte-for-byte unchanged")
	require.Empty(t, after.OwnerReferences)

	stored := getCluster(t, c, namespace, cluster.Name)
	condition := meta.FindStatusCondition(stored.Status.Conditions, conditionResourceNamesAvailable)
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, "NameConflict", condition.Reason)
	require.Contains(t, condition.Message, `Service/ledger exists and is not owned by Cluster "v3"`)
	require.Equal(t, "Error", stored.Status.Phase)
	require.Contains(t, <-recorder.Events, "ResourceNameConflict")

	// No derived object was created alongside the conflicting one.
	require.Error(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "ledger"}, &appsv1.StatefulSet{}))
	require.Error(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: "ledger-headless"}, &corev1.Service{}))
}

// TestReconcileRefusesResourceNameCollision proves that a Cluster whose base
// name resolves to objects controlled by another Cluster fails loudly
// (condition, Error phase, warning event) and never adopts, mutates or deletes
// the other Cluster's objects, nor reports its readiness.
func TestReconcileRefusesResourceNameCollision(t *testing.T) {
	t.Parallel()

	disabled := false
	for _, tc := range []struct {
		name              string
		ownerResourceName string
		intruderResource  string
		base              string
	}{
		{"override equals another Cluster's default name", "", "ledger-owner", "ledger-owner"},
		{"two equal overrides", "shared", "shared", "shared"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			const namespace = "ns"
			ctx := context.Background()
			scheme := resourceNameTestScheme(t)
			owner := &ledgerv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "owner", Namespace: namespace, UID: "uid-owner"},
				Spec:       ledgerv1alpha1.ClusterSpec{ResourceName: tc.ownerResourceName, Replicas: new(int32(3))},
			}
			intruder := &ledgerv1alpha1.Cluster{
				ObjectMeta: metav1.ObjectMeta{Name: "intruder", Namespace: namespace, UID: "uid-intruder"},
				Spec: ledgerv1alpha1.ClusterSpec{
					ResourceName: tc.intruderResource,
					Replicas:     new(int32(1)),
					// A disabled optional Service exercises the name-based
					// delete path that must not reach the owner's object.
					HeadlessService: ledgerv1alpha1.HeadlessServiceSpec{Enabled: &disabled},
				},
			}
			c := fake.NewClientBuilder().WithScheme(scheme).
				WithObjects(owner, intruder).WithStatusSubresource(owner, intruder).Build()
			recorder := record.NewFakeRecorder(16)
			r := &ClusterReconciler{Client: c, Scheme: scheme, Recorder: recorder}

			reconcileCluster(t, r, namespace, "owner")
			ownedSTS := &appsv1.StatefulSet{}
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: tc.base}, ownedSTS))
			ownedSTS.Status.ReadyReplicas = 3
			require.NoError(t, c.Status().Update(ctx, ownedSTS))
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: tc.base}, ownedSTS))
			ownedHeadless := &corev1.Service{}
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: tc.base + "-headless"}, ownedHeadless))

			result := reconcileCluster(t, r, namespace, "intruder")
			require.Equal(t, nameConflictRequeueInterval, result.RequeueAfter)

			stored := getCluster(t, c, namespace, "intruder")
			condition := meta.FindStatusCondition(stored.Status.Conditions, conditionResourceNamesAvailable)
			require.NotNil(t, condition)
			require.Equal(t, metav1.ConditionFalse, condition.Status)
			require.Equal(t, "NameConflict", condition.Reason)
			require.Contains(t, condition.Message, "StatefulSet/"+tc.base+" is controlled by Cluster/owner")
			require.Equal(t, "Error", stored.Status.Phase)
			require.Zero(t, stored.Status.ReadyReplicas, "the owner's readiness must not be reported")
			require.Contains(t, <-recorder.Events, "ResourceNameConflict")

			// The owner's objects are untouched.
			afterSTS := &appsv1.StatefulSet{}
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: tc.base}, afterSTS))
			require.Equal(t, ownedSTS.ResourceVersion, afterSTS.ResourceVersion)
			require.True(t, metav1.IsControlledBy(afterSTS, owner))
			afterHeadless := &corev1.Service{}
			require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: tc.base + "-headless"}, afterHeadless))
			require.Equal(t, ownedHeadless.ResourceVersion, afterHeadless.ResourceVersion)

			// The owner keeps reconciling normally.
			reconcileCluster(t, r, namespace, "owner")
			ownerStatus := getCluster(t, c, namespace, "owner")
			ownerCondition := meta.FindStatusCondition(ownerStatus.Status.Conditions, conditionResourceNamesAvailable)
			require.NotNil(t, ownerCondition)
			require.Equal(t, metav1.ConditionTrue, ownerCondition.Status)
		})
	}
}
