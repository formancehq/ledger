package controller

import (
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

// A same-named Pebble Cluster may already own services, pods and credentials in
// this namespace. Reconciliation must create a separate RocksDB deployment.
func TestClusterCoexistsWithSameNamedPebbleWorkload(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	legacySelector := map[string]string{"app.kubernetes.io/name": "ledger", "app.kubernetes.io/instance": "shared"}
	legacyService := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: "ledger-shared", Namespace: "test"},
		Spec:       corev1.ServiceSpec{Selector: legacySelector},
	}
	replicas := int32(3)
	legacyStatefulSet := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "ledger-shared", Namespace: "test"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "ledger", Image: "ledger:pebble", Env: []corev1.EnvVar{{Name: "PEBBLE_CACHE_SIZE", Value: "1Gi"}}}}}},
		},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithObjects(legacyService, legacyStatefulSet).Build()
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "test", UID: types.UID("rocksdb-cluster")},
		Spec:       ledgerv1alpha1.ClusterSpec{Replicas: &replicas, Image: ledgerv1alpha1.ImageSpec{Repository: "ledger", Tag: "rocksdb"}},
	}
	r := &ClusterReconciler{Client: kube, Scheme: scheme}
	require.NoError(t, r.reconcileService(t.Context(), cluster))
	_, err := r.reconcileStatefulSet(t.Context(), cluster, "rocksdb-hash", nil)
	require.NoError(t, err)
	var nextService corev1.Service
	var nextStatefulSet appsv1.StatefulSet
	key := types.NamespacedName{Name: "ledger-next-shared", Namespace: "test"}
	require.NoError(t, kube.Get(t.Context(), key, &nextService))
	require.NoError(t, kube.Get(t.Context(), key, &nextStatefulSet))
	require.Equal(t, "ledger:rocksdb", nextStatefulSet.Spec.Template.Spec.Containers[0].Image)
	require.Equal(t, "ledger-next.formance.com/v1alpha1", nextStatefulSet.OwnerReferences[0].APIVersion)
	require.False(t, labels.SelectorFromSet(legacySelector).Matches(labels.Set(nextStatefulSet.Spec.Template.Labels)), "the Pebble service must not route to RocksDB pods")
	require.False(t, labels.SelectorFromSet(nextService.Spec.Selector).Matches(labels.Set(legacySelector)), "the RocksDB service must not route to Pebble pods")
	var storedService corev1.Service
	var storedStatefulSet appsv1.StatefulSet
	legacyKey := types.NamespacedName{Name: "ledger-shared", Namespace: "test"}
	require.NoError(t, kube.Get(t.Context(), legacyKey, &storedService))
	require.NoError(t, kube.Get(t.Context(), legacyKey, &storedStatefulSet))
	require.Equal(t, legacyService.Spec, storedService.Spec)
	require.Equal(t, legacyStatefulSet.Spec, storedStatefulSet.Spec)
}

func TestCredentialsCoexistencePreservesPebbleSecretsDuringCreateAndDelete(t *testing.T) {
	t.Parallel()
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	legacy := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ledger-shared-credentials-keys", Namespace: "test", Labels: map[string]string{"ledger.formance.com/credentials-name": "shared"}},
		Data:       map[string][]byte{"seed-hex": []byte("legacy-material")},
	}
	credentials := &ledgerv1alpha1.Credentials{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", UID: types.UID("rocksdb-credentials")},
		Spec:       ledgerv1alpha1.CredentialsSpec{AdditionalNamespaces: []string{"test"}},
	}
	kube := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(credentials).WithObjects(legacy, credentials).Build()
	r := &CredentialsReconciler{Client: kube, Scheme: scheme, APIReader: kube, OperatorNamespace: "operators"}
	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: credentials.Name}})
	require.NoError(t, err)
	var nextSecret corev1.Secret
	require.NoError(t, kube.Get(t.Context(), types.NamespacedName{Name: "ledger-next-shared-credentials-keys", Namespace: "test"}, &nextSecret))
	require.NotEqual(t, legacy.Data, nextSecret.Data, "new credentials must not adopt Pebble credential material")
	var storedCredentials ledgerv1alpha1.Credentials
	require.NoError(t, kube.Get(t.Context(), types.NamespacedName{Name: "shared"}, &storedCredentials))
	require.NoError(t, kube.Delete(t.Context(), &storedCredentials))
	_, err = r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: credentials.Name}})
	require.NoError(t, err)
	var storedLegacy corev1.Secret
	require.NoError(t, kube.Get(t.Context(), types.NamespacedName{Name: legacy.Name, Namespace: legacy.Namespace}, &storedLegacy))
	require.Equal(t, legacy.Data, storedLegacy.Data)
	require.Equal(t, legacy.Labels, storedLegacy.Labels)
}
