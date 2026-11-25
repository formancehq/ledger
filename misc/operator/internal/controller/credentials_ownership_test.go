package controller

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestCredentialsReconcileRejectsUnverifiedReplicaSeed(t *testing.T) {
	t.Parallel()
	credentials := &ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: "identity", UID: "current"}, Spec: ledgerv1alpha1.CredentialsSpec{AdditionalNamespaces: []string{"target"}}}
	planted := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialsSecretName(credentials), Namespace: "target", Labels: map[string]string{credentialsNameLabel: credentials.Name}, UID: "planted"}, Data: map[string][]byte{keySeedHex: []byte("planted-seed"), keyKeyID: []byte("planted-key")}}
	scheme := ownershipTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(credentials).WithObjects(credentials, planted).Build()
	r := &CredentialsReconciler{Client: c, APIReader: c, Scheme: scheme, OperatorNamespace: "operator"}

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: credentials.Name}})
	require.ErrorContains(t, err, "unverified credentials Secret")
	actual := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(planted), actual))
	require.Equal(t, planted.Data, actual.Data)
	require.Empty(t, actual.OwnerReferences)
	canonical := &corev1.Secret{}
	require.Error(t, c.Get(t.Context(), client.ObjectKey{Namespace: "operator", Name: credentialsCanonicalSecretName(credentials.Name)}, canonical))
}

func TestCredentialsReconcileLeavesUnownedLabeledSecretOutsideTargets(t *testing.T) {
	t.Parallel()
	credentials := &ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: "identity", UID: "current"}}
	foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialsSecretName(credentials), Namespace: "other", Labels: map[string]string{credentialsNameLabel: credentials.Name}, UID: "foreign"}, Data: map[string][]byte{keySeedHex: []byte("foreign")}}
	scheme := ownershipTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(credentials).WithObjects(credentials, foreign).Build()
	r := &CredentialsReconciler{Client: c, APIReader: c, Scheme: scheme, OperatorNamespace: "operator"}

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: credentials.Name}})
	require.NoError(t, err)
	actual := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(foreign), actual))
	require.Equal(t, foreign.Data, actual.Data)
}

func TestCredentialsReconcileDoesNotAdoptPlantedSeedOutsideTargets(t *testing.T) {
	t.Parallel()
	credentials := &ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: "identity", UID: "current"}, Spec: ledgerv1alpha1.CredentialsSpec{AdditionalNamespaces: []string{"target"}}}
	planted := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialsSecretName(credentials), Namespace: "other", UID: "planted", Labels: map[string]string{credentialsNameLabel: credentials.Name}}, Data: map[string][]byte{keySeedHex: []byte("planted-seed")}}
	scheme := ownershipTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(credentials).WithObjects(credentials, planted).Build()
	r := &CredentialsReconciler{Client: c, APIReader: c, Scheme: scheme, OperatorNamespace: "operator"}

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: credentials.Name}})
	require.NoError(t, err)
	canonical := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKey{Namespace: "operator", Name: credentialsCanonicalSecretName(credentials.Name)}, canonical))
	require.NotEqual(t, planted.Data[keySeedHex], canonical.Data[keySeedHex])
	require.True(t, metav1.IsControlledBy(canonical, credentials))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(planted), &corev1.Secret{}))
}

func TestCredentialsReconcileRejectsUnownedSameNameSecret(t *testing.T) {
	t.Parallel()
	credentials := &ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: "identity", UID: "current"}, Spec: ledgerv1alpha1.CredentialsSpec{AdditionalNamespaces: []string{"target"}}}
	foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialsSecretName(credentials), Namespace: "target", UID: "foreign"}, Data: map[string][]byte{keySeedHex: []byte("foreign")}}
	scheme := ownershipTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(credentials).WithObjects(credentials, foreign).Build()
	r := &CredentialsReconciler{Client: c, APIReader: c, Scheme: scheme, OperatorNamespace: "operator"}

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: credentials.Name}})
	require.ErrorContains(t, err, "unverified credentials Secret")
	actual := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(foreign), actual))
	require.Equal(t, foreign.Data, actual.Data)
	require.Empty(t, actual.Labels)
}

func TestCredentialsDeletionPreservesForeignSecretsAndRecreatedOwner(t *testing.T) {
	t.Parallel()
	deletionTime := metav1.NewTime(time.Now())
	credentials := &ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: "identity", UID: "current", Finalizers: []string{credentialsFinalizer}, DeletionTimestamp: &deletionTime}}
	foreign := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialsSecretName(credentials), Namespace: "other", UID: "foreign", Labels: map[string]string{credentialsNameLabel: credentials.Name}}, Data: map[string][]byte{keySeedHex: []byte("foreign")}}
	prior := &ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: credentials.Name, UID: "prior"}}
	stale := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialsSecretName(credentials), Namespace: "stale", UID: "stale", Labels: map[string]string{credentialsNameLabel: credentials.Name}, OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(prior, ledgerv1alpha1.GroupVersion.WithKind("Credentials"))}}}
	scheme := ownershipTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(credentials, foreign, stale).Build()
	r := &CredentialsReconciler{Client: c, APIReader: c, Scheme: scheme, OperatorNamespace: "operator"}

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: credentials.Name}})
	require.NoError(t, err)
	for _, secret := range []*corev1.Secret{foreign, stale} {
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(secret), &corev1.Secret{}))
	}
}

func TestCredentialsReconcileDoesNotRotateMissingCanonicalSeed(t *testing.T) {
	t.Parallel()
	credentials := &ledgerv1alpha1.Credentials{
		ObjectMeta: metav1.ObjectMeta{Name: "identity", UID: "current"},
		Status:     ledgerv1alpha1.CredentialsStatus{KeyID: "previous-key"},
	}
	scheme := ownershipTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(credentials).WithObjects(credentials).Build()
	r := &CredentialsReconciler{Client: c, APIReader: c, Scheme: scheme, OperatorNamespace: "operator"}

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: credentials.Name}})
	require.ErrorContains(t, err, "canonical seed for Credentials")
	require.ErrorContains(t, err, "no verified legacy replica")
	require.Error(t, c.Get(t.Context(), client.ObjectKey{Namespace: "operator", Name: credentialsCanonicalSecretName(credentials.Name)}, &corev1.Secret{}))
}

func TestCredentialsReconcileRepairsLabelsOnOwnedSecrets(t *testing.T) {
	t.Parallel()
	credentials := &ledgerv1alpha1.Credentials{ObjectMeta: metav1.ObjectMeta{Name: "identity", UID: "current"}, Spec: ledgerv1alpha1.CredentialsSpec{AdditionalNamespaces: []string{"target"}}}
	owner := []metav1.OwnerReference{*metav1.NewControllerRef(credentials, ledgerv1alpha1.GroupVersion.WithKind("Credentials"))}
	canonical := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialsCanonicalSecretName(credentials.Name), Namespace: "operator", UID: "canonical", OwnerReferences: owner}, Data: map[string][]byte{keySeedHex: []byte("owned-seed"), keyKeyID: []byte("owned-key")}}
	replica := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialsSecretName(credentials), Namespace: "target", UID: "replica", OwnerReferences: owner}, Data: map[string][]byte{keySeedHex: []byte("old-seed")}}
	scheme := ownershipTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(credentials).WithObjects(credentials, canonical, replica).Build()
	r := &CredentialsReconciler{Client: c, APIReader: c, Scheme: scheme, OperatorNamespace: "operator"}

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: credentials.Name}})
	require.NoError(t, err)
	for _, secret := range []*corev1.Secret{canonical, replica} {
		actual := &corev1.Secret{}
		require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(secret), actual))
		require.Equal(t, credentials.Name, actual.Labels[credentialsNameLabel])
		require.True(t, metav1.IsControlledBy(actual, credentials))
	}
	actual := &corev1.Secret{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(replica), actual))
	require.Equal(t, canonical.Data, actual.Data)
}

func TestCredentialsReconcileDeletesStatusReferencedOwnedReplicaAfterLabelDrift(t *testing.T) {
	t.Parallel()
	credentials := &ledgerv1alpha1.Credentials{
		ObjectMeta: metav1.ObjectMeta{Name: "identity", UID: "current"},
		Status: ledgerv1alpha1.CredentialsStatus{
			KeyID:                 "owned-key",
			DistributedSecretRefs: []ledgerv1alpha1.SecretReference{{Namespace: "old", Name: "ledger-identity-credentials-keys"}},
		},
	}
	owner := []metav1.OwnerReference{*metav1.NewControllerRef(credentials, ledgerv1alpha1.GroupVersion.WithKind("Credentials"))}
	canonical := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialsCanonicalSecretName(credentials.Name), Namespace: "operator", UID: "canonical", Labels: map[string]string{credentialsNameLabel: credentials.Name}, OwnerReferences: owner}, Data: map[string][]byte{keySeedHex: []byte("owned-seed"), keyKeyID: []byte("owned-key")}}
	replica := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: credentialsSecretName(credentials), Namespace: "old", UID: "replica", OwnerReferences: owner}}
	scheme := ownershipTestScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(credentials).WithObjects(credentials, canonical, replica).Build()
	r := &CredentialsReconciler{Client: c, APIReader: c, Scheme: scheme, OperatorNamespace: "operator"}

	_, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: types.NamespacedName{Name: credentials.Name}})
	require.NoError(t, err)
	require.Error(t, c.Get(t.Context(), client.ObjectKeyFromObject(replica), &corev1.Secret{}))
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(canonical), &corev1.Secret{}))
}
