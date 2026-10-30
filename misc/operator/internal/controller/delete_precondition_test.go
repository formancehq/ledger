package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// replacedBeforeDelete emulates an owned derived object being replaced by a
// foreign one under the same name between the reconciler's ownership read and
// its delete (or a stale cached read): on the first Delete of the replacement's
// name, the interceptor deletes the live object, creates the replacement (new
// UID, no controller reference to the Cluster), records it as the API server
// stores it, then performs the requested delete.
//
// controller-runtime v0.20.4's fake client enforces only the ResourceVersion
// delete precondition, not the UID one (see fakeClient.Delete), so the
// interceptor enforces a UID precondition the way the API server does, with a
// Conflict when the live UID differs. A delete without a UID precondition is
// passed through unchanged and deletes the replacement, which the test detects.
type replacedBeforeDelete struct {
	replacement client.Object
	stored      client.Object
}

func (r *replacedBeforeDelete) funcs(t *testing.T) interceptor.Funcs {
	t.Helper()

	return interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
		if r.stored == nil && obj.GetName() == r.replacement.GetName() && obj.GetNamespace() == r.replacement.GetNamespace() {
			current, ok := obj.DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, c.Delete(ctx, current))
			replacement, ok := r.replacement.DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, c.Create(ctx, replacement))
			stored, ok := r.replacement.DeepCopyObject().(client.Object)
			require.True(t, ok)
			require.NoError(t, c.Get(ctx, client.ObjectKeyFromObject(replacement), stored))
			require.NotEmpty(t, stored.GetUID())
			r.stored = stored
		}

		var options client.DeleteOptions
		options.ApplyOptions(opts)
		if options.Preconditions != nil && options.Preconditions.UID != nil {
			live, ok := obj.DeepCopyObject().(client.Object)
			require.True(t, ok)
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), live); err == nil && live.GetUID() != *options.Preconditions.UID {
				return apierrors.NewConflict(schema.GroupResource{}, obj.GetName(), fmt.Errorf(
					"Precondition failed: UID in precondition: %s, UID in object meta: %s", *options.Preconditions.UID, live.GetUID()))
			}
		}

		return c.Delete(ctx, obj, opts...)
	}}
}

// requireReplacementIntact asserts the replacement was installed and still
// exists exactly as the API server stored it, and that err reports the retry.
func (r *replacedBeforeDelete) requireReplacementIntact(t *testing.T, c client.Client, err error) {
	t.Helper()
	require.NotNil(t, r.stored, "the guarded delete path never reached Delete")
	require.Error(t, err)
	require.True(t, apierrors.IsConflict(err), "want a precondition Conflict surfaced for retry, got %v", err)
	live, ok := r.stored.DeepCopyObject().(client.Object)
	require.True(t, ok)
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(r.stored), live), "the foreign replacement was deleted")
	require.Equal(t, r.stored, live)
}

func deletePreconditionCluster() *ledgerv1alpha1.Cluster {
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "pinned", Namespace: "ns", UID: "uid-pinned"},
		Spec:       ledgerv1alpha1.ClusterSpec{Replicas: new(int32(3))},
	}
	applyDefaults(cluster)

	return cluster
}

// foreignMeta is the replacement's metadata: same name, new UID, controlled by
// another Cluster.
func foreignMeta(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name: name, Namespace: "ns", UID: "uid-foreign",
		Labels: map[string]string{"owner": "foreign"},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: ledgerv1alpha1.GroupVersion.String(), Kind: "Cluster",
			Name: "foreign", UID: "uid-foreign-cluster", Controller: new(true),
		}},
	}
}

func newDeletePreconditionClient(t *testing.T, scheme *runtime.Scheme, guard *replacedBeforeDelete, objs ...client.Object) client.Client {
	t.Helper()

	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
		WithInterceptorFuncs(guard.funcs(t)).Build()
}

func TestDeleteIfExistsNeverDeletesAReplacement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := ownershipTestScheme(t)
	cluster := deletePreconditionCluster()
	name := headlessServiceName(cluster.Name)
	owned := controlledByCluster(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: "uid-owned"}}, cluster)
	guard := &replacedBeforeDelete{replacement: &corev1.Service{ObjectMeta: foreignMeta(name)}}
	c := newDeletePreconditionClient(t, scheme, guard, cluster, owned)
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	err := r.deleteIfExists(ctx, cluster, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}})

	guard.requireReplacementIntact(t, c, err)
}

func TestDeleteIfExistsDeletesTheInspectedObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := ownershipTestScheme(t)
	cluster := deletePreconditionCluster()
	name := headlessServiceName(cluster.Name)
	owned := controlledByCluster(&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: "uid-owned"}}, cluster)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, owned).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	require.NoError(t, r.deleteIfExists(ctx, cluster, &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}}))
	err := c.Get(ctx, client.ObjectKeyFromObject(owned), &corev1.Service{})
	require.True(t, apierrors.IsNotFound(err), "the owned Service must be deleted, got %v", err)
}

func TestPruneDNSEndpointsNeverDeletesAReplacement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := ownershipTestScheme(t)
	scheme.AddKnownTypeWithName(dnsEndpointGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(dnsEndpointGVK.GroupVersion().WithKind("DNSEndpointList"), &unstructured.UnstructuredList{})
	cluster := deletePreconditionCluster()
	name := dnsEndpointName(cluster.Name, "stale")
	dnsEndpoint := func(meta metav1.ObjectMeta) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{}
		obj.SetGroupVersionKind(dnsEndpointGVK)
		obj.SetName(meta.Name)
		obj.SetNamespace(meta.Namespace)
		obj.SetUID(meta.UID)
		obj.SetOwnerReferences(meta.OwnerReferences)
		obj.SetLabels(map[string]string{labelManagedBy: "ledger-operator"})

		return obj
	}
	owned := controlledByCluster(dnsEndpoint(metav1.ObjectMeta{Name: name, Namespace: "ns", UID: "uid-owned"}), cluster)
	guard := &replacedBeforeDelete{replacement: dnsEndpoint(foreignMeta(name))}
	c := newDeletePreconditionClient(t, scheme, guard, cluster, owned)
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	err := r.pruneDNSEndpoints(ctx, cluster, map[string]struct{}{})

	guard.requireReplacementIntact(t, c, err)
}

func TestDeleteClusterSecretNeverDeletesAReplacement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := ownershipTestScheme(t)
	cluster := deletePreconditionCluster()
	name := clusterSecretName(cluster.Name)
	owned := controlledByCluster(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: "uid-owned"}}, cluster)
	guard := &replacedBeforeDelete{replacement: &corev1.Secret{
		ObjectMeta: foreignMeta(name), Data: map[string][]byte{clusterSecretKey: []byte("foreign")},
	}}
	c := newDeletePreconditionClient(t, scheme, guard, cluster, owned)
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	err := r.deleteClusterSecret(ctx, cluster)

	guard.requireReplacementIntact(t, c, err)
}

func TestAuthKeysCleanupNeverDeletesAReplacement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := ownershipTestScheme(t)
	cluster := deletePreconditionCluster()
	name := authKeysConfigMapName(cluster.Name)
	owned := controlledByCluster(&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: "uid-owned"}}, cluster)
	guard := &replacedBeforeDelete{replacement: &corev1.ConfigMap{
		ObjectMeta: foreignMeta(name), Data: map[string]string{"auth-keys.json": `{"keys":[]}`},
	}}
	c := newDeletePreconditionClient(t, scheme, guard, cluster, owned)
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	// No Credentials match the Cluster: the ConfigMap is a legitimate removal.
	_, _, err := r.reconcileAuthKeys(ctx, cluster)

	guard.requireReplacementIntact(t, c, err)
}

func TestStatefulSetRecreateNeverDeletesAReplacement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := ownershipTestScheme(t)
	cluster := deletePreconditionCluster()
	name := resourceName(cluster.Name)
	// A VolumeClaimTemplate the desired spec does not have forces the
	// orphan-and-recreate path.
	staleVCT := []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "legacy"}}}
	owned := controlledByCluster(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: "uid-owned"},
		Spec:       appsv1.StatefulSetSpec{Replicas: new(int32(3)), VolumeClaimTemplates: staleVCT},
	}, cluster)
	guard := &replacedBeforeDelete{replacement: &appsv1.StatefulSet{
		ObjectMeta: foreignMeta(name),
		Spec:       appsv1.StatefulSetSpec{Replicas: new(int32(1)), ServiceName: "foreign"},
	}}
	c := newDeletePreconditionClient(t, scheme, guard, cluster, owned)
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	noExec := func(context.Context, *rest.Config, kubernetes.Interface, string, string, string, []string) (*execResult, error) {
		t.Fatal("the recreate path must not exec")

		return nil, nil
	}

	_, err := r.reconcileStatefulSetWithExec(ctx, cluster, "hash", nil, noExec)

	guard.requireReplacementIntact(t, c, err)
}

func TestStatefulSetRecreateOrphansTheInspectedObject(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	scheme := ownershipTestScheme(t)
	cluster := deletePreconditionCluster()
	owned := controlledByCluster(&appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: resourceName(cluster.Name), Namespace: "ns", UID: "uid-owned"},
		Spec: appsv1.StatefulSetSpec{
			Replicas:             new(int32(3)),
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "legacy"}}},
		},
	}, cluster)
	var sent client.DeleteOptions
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster, owned).
		WithInterceptorFuncs(interceptor.Funcs{Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			sent.ApplyOptions(opts)

			return c.Delete(ctx, obj, opts...)
		}}).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	_, err := r.reconcileStatefulSetWithExec(ctx, cluster, "hash", nil, nil)

	require.NoError(t, err)
	require.NotNil(t, sent.PropagationPolicy)
	require.Equal(t, metav1.DeletePropagationOrphan, *sent.PropagationPolicy)
	require.NotNil(t, sent.Preconditions)
	require.NotNil(t, sent.Preconditions.UID)
	require.Equal(t, owned.UID, *sent.Preconditions.UID)
	getErr := c.Get(ctx, client.ObjectKeyFromObject(owned), &appsv1.StatefulSet{})
	require.True(t, apierrors.IsNotFound(getErr), "the owned StatefulSet must be deleted, got %v", getErr)
}
