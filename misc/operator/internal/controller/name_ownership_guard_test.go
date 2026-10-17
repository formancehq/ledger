package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// TestAdoptionPointRefusesUnownedObject bypasses the up-front guard and calls a
// reconcile function directly: the mutate-level check alone must refuse an
// existing object the Cluster does not control and leave it untouched.
func TestAdoptionPointRefusesUnownedObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := resourceNameTestScheme(t)
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "v3", Namespace: "ns", UID: "uid-v3"},
		Spec:       ledgerv1alpha1.ClusterSpec{ResourceName: "ledger", Replicas: new(int32(3))},
	}
	applyDefaults(cluster)
	foreign := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "ledger", Namespace: "ns", Labels: map[string]string{"app": "ledger-v2"}},
		Spec:       corev1.ServiceSpec{Selector: map[string]string{"app": "ledger-v2"}, Ports: []corev1.ServicePort{{Name: "http", Port: 8080}}},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, foreign).Build()
	before := &corev1.Service{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(foreign), before))
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	err := r.reconcileService(ctx, cluster)
	require.ErrorIs(t, err, errResourceNameConflict)
	require.Contains(t, err.Error(), `Service/ledger exists and is not owned by Cluster "v3"`)

	after := &corev1.Service{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(foreign), after))
	require.Equal(t, before, after, "the adoption point must not write the foreign Service")

	// An object the Cluster does control is updated normally.
	owned := controlledByCluster(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "ledger-headless", Namespace: "ns"}}, cluster)
	require.NoError(t, c.Create(ctx, owned))
	require.NoError(t, r.reconcileHeadlessService(ctx, cluster))
}

// TestAdoptionPointConflictReportsCondition proves a conflict raised at an
// adoption point the up-front guard does not cover (the cluster Secret, which
// only exists while TLS is active) takes the same loud path:
// ResourceNamesAvailable=False, phase Error, warning event, no write.
func TestAdoptionPointConflictReportsCondition(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := resourceNameTestScheme(t)
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "secure", Namespace: "ns", UID: "uid-secure"},
		Spec: ledgerv1alpha1.ClusterSpec{
			Replicas: new(int32(3)),
			TLS:      &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "tls"},
		},
	}
	foreign := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ledger-secure-cluster-secret", Namespace: "ns"},
		Data:       map[string][]byte{"cluster-secret": []byte("not-ours")},
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, foreign).WithStatusSubresource(cluster).Build()
	recorder := record.NewFakeRecorder(16)
	r := &ClusterReconciler{Client: c, Scheme: scheme, Recorder: recorder}

	result := reconcileCluster(t, r, "ns", "secure")
	require.Equal(t, nameConflictRequeueInterval, result.RequeueAfter)

	stored := getCluster(t, c, "ns", "secure")
	condition := meta.FindStatusCondition(stored.Status.Conditions, conditionResourceNamesAvailable)
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionFalse, condition.Status)
	require.Equal(t, "NameConflict", condition.Reason)
	require.Contains(t, condition.Message, "Secret/ledger-secure-cluster-secret exists and is not owned")
	require.Equal(t, "Error", stored.Status.Phase)
	require.Contains(t, <-recorder.Events, "ResourceNameConflict")

	after := &corev1.Secret{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(foreign), after))
	require.Equal(t, []byte("not-ours"), after.Data["cluster-secret"])
	require.Empty(t, after.OwnerReferences)
}

// TestUnusedDerivedNamesDoNotBlock proves a foreign object under a derived name
// the Cluster will not use (disabled gRPC Service/Ingress, a user-managed
// ServiceAccount with create=false) neither blocks reconcile nor gets pruned.
func TestUnusedDerivedNamesDoNotBlock(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := resourceNameTestScheme(t)
	disabled := false
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns", UID: "uid-app"},
		Spec: ledgerv1alpha1.ClusterSpec{
			Replicas:       new(int32(3)),
			ServiceAccount: ledgerv1alpha1.ServiceAccountSpec{Create: &disabled, Name: "default"},
		},
	}
	foreignGrpc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "ledger-app-grpc", Namespace: "ns"}}
	foreignSA := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default", Namespace: "ns"}}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(cluster, foreignGrpc, foreignSA).WithStatusSubresource(cluster).Build()
	beforeSA := &corev1.ServiceAccount{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(foreignSA), beforeSA))
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	reconcileCluster(t, r, "ns", "app")

	stored := getCluster(t, c, "ns", "app")
	condition := meta.FindStatusCondition(stored.Status.Conditions, conditionResourceNamesAvailable)
	require.NotNil(t, condition)
	require.Equal(t, metav1.ConditionTrue, condition.Status)
	require.NoError(t, c.Get(ctx, types.NamespacedName{Namespace: "ns", Name: "ledger-app"}, &appsv1.StatefulSet{}))

	// The disabled-gRPC prune path must not delete the foreign Service.
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(foreignGrpc), &corev1.Service{}))
	afterSA := &corev1.ServiceAccount{}
	require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(foreignSA), afterSA))
	require.Equal(t, beforeSA, afterSA, "a user-managed ServiceAccount is never touched")
}

// TestGuardReportsDNSEndpointConflict proves an enabled DNSEndpoint entry whose
// derived object exists without this Cluster's controller reference is a
// conflict, while an entry with no endpoints (never created) is not checked.
func TestGuardReportsDNSEndpointConflict(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := resourceNameTestScheme(t)
	scheme.AddKnownTypeWithName(dnsEndpointGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(dnsEndpointGVK.GroupVersion().WithKind("DNSEndpointList"), &unstructured.UnstructuredList{})
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "dns", Namespace: "ns", UID: "uid-dns"},
		Spec: ledgerv1alpha1.ClusterSpec{DNSEndpoints: []ledgerv1alpha1.DNSEndpointSpec{
			{Name: "public", Enabled: true, Endpoints: []ledgerv1alpha1.DNSEndpointEntry{{DNSName: "a.example.com", Targets: []string{"lb"}}}},
			{Name: "empty", Enabled: true},
		}},
	}
	foreignDNS := func(name string) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(dnsEndpointGVK)
		obj.SetName(name)
		obj.SetNamespace("ns")

		return obj
	}
	c := fake.NewClientBuilder().WithScheme(scheme).
		WithObjects(foreignDNS("ledger-dns-public"), foreignDNS("ledger-dns-empty")).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	err := r.validateResourceNameOwnership(ctx, cluster)
	require.ErrorIs(t, err, errResourceNameConflict)
	require.Contains(t, err.Error(), "DNSEndpoint/ledger-dns-public exists and is not owned")
	require.NotContains(t, err.Error(), "ledger-dns-empty", "an entry without endpoints is never created, so it cannot conflict")
}
