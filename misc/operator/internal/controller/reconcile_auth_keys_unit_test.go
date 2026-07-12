package controller

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

// authKeysScheme builds a scheme with everything reconcileAuthKeys touches.
func authKeysScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, ledgerv1alpha1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))

	return scheme
}

// authEnabledCluster returns a minimal auth-enabled Cluster carrying the given
// labels (used by Credentials selectors).
func authEnabledCluster(name, namespace string, labels map[string]string) *ledgerv1alpha1.Cluster {
	enabled := true

	return &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec: ledgerv1alpha1.ClusterSpec{
			Auth: &ledgerv1alpha1.AuthorizationConfig{Enabled: &enabled},
		},
	}
}

// issuerBackedCluster returns an auth-enabled Cluster that authenticates via an
// OIDC issuer (no Ed25519 keys required). Such a cluster boots and authenticates
// fine with an empty/absent Ed25519 key set, so the auth-keys fail-safe must
// never freeze it.
func issuerBackedCluster(name, namespace string, labels map[string]string) *ledgerv1alpha1.Cluster {
	enabled := true

	return &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace, Labels: labels},
		Spec: ledgerv1alpha1.ClusterSpec{
			Auth: &ledgerv1alpha1.AuthorizationConfig{
				Enabled: &enabled,
				Issuer:  "https://issuer.example.com",
			},
		},
	}
}

// matchingCredentials returns a cluster-scoped Credentials selecting the given
// labels. If distributed is true its status carries a DistributedSecretRefs
// entry pointing at secretNS/secretName.
func matchingCredentials(name string, selector map[string]string, distributed bool, secretNS, secretName string) *ledgerv1alpha1.Credentials {
	cred := &ledgerv1alpha1.Credentials{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: ledgerv1alpha1.CredentialsSpec{
			Scopes:   []string{"read"},
			Selector: metav1.LabelSelector{MatchLabels: selector},
		},
	}
	if distributed {
		cred.Status.DistributedSecretRefs = []ledgerv1alpha1.SecretReference{
			{Namespace: secretNS, Name: secretName},
		}
	}

	return cred
}

// existingAuthKeysConfigMap returns a ConfigMap standing in for a previously
// reconciled auth-keys ConfigMap for the given Cluster.
func existingAuthKeysConfigMap(clusterName, namespace string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      authKeysConfigMapName(clusterName),
			Namespace: namespace,
		},
		Data: map[string]string{
			"auth-keys.json": `{"keys":[{"keyId":"stale","publicKeyFile":"/auth-keys/stale.hex","scopes":["read"]}]}`,
		},
	}
}

// authKeysConfigMapWithEntries builds an auth-keys ConfigMap whose auth-keys.json
// and per-credential pubkey blobs are keyed by the stable pubKeyFileName identity,
// mirroring what reconcileAuthKeys itself writes. entries maps a credential name
// (with the default "credentials" prefix) to its (keyID, hex pubkey). This is the
// carry-forward source for the partial-resolution cases.
func authKeysConfigMapWithEntries(clusterName, namespace string, entries map[string]struct {
	keyID  string
	pubKey string
}) *corev1.ConfigMap {
	aks := authKeysJSON{}
	data := map[string]string{}
	for credName, e := range entries {
		fileName := pubKeyFileName("credentials", credName)
		aks.Keys = append(aks.Keys, authKeyEntry{
			KeyID:         e.keyID,
			PublicKeyFile: "/auth-keys/" + fileName,
			Scopes:        []string{"read"},
		})
		data[fileName] = e.pubKey
	}
	raw, err := json.Marshal(aks)
	if err != nil {
		panic(err)
	}
	data["auth-keys.json"] = string(raw)

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      authKeysConfigMapName(clusterName),
			Namespace: namespace,
		},
		Data: data,
	}
}

// TestReconcileAuthKeys_TransientNonDistribution_PreservesConfigMap covers the
// core EN-1487 fix: matching Credentials exist but none is distributed yet, and
// a ConfigMap already exists. reconcileAuthKeys must NOT delete the ConfigMap,
// must return no credentials, and must signal pending so the caller preserves
// the StatefulSet wiring, sets AuthKeysPending, and requeues.
func TestReconcileAuthKeys_TransientNonDistribution_PreservesConfigMap(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
	)
	selector := map[string]string{"tier": "gold"}

	scheme := authKeysScheme(t)
	existingCM := existingAuthKeysConfigMap(clusterName, namespace)
	cred := matchingCredentials("thierry-cred", selector, false, "", "")

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingCM, cred).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	cluster := authEnabledCluster(clusterName, namespace, selector)

	credentials, pending, err := r.reconcileAuthKeys(context.Background(), cluster)
	require.NoError(t, err)
	assert.True(t, pending, "transient non-distribution must report pending")
	assert.Nil(t, credentials, "no key must be returned while credentials are undistributed")

	// The existing ConfigMap must survive untouched — deleting it is exactly the
	// bug that crash-loops auth-enabled clusters.
	cm := &corev1.ConfigMap{}
	err = c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: authKeysConfigMapName(clusterName)}, cm)
	require.NoError(t, err, "existing auth-keys ConfigMap must be preserved during transient non-distribution")
	assert.Equal(t, existingCM.Data["auth-keys.json"], cm.Data["auth-keys.json"],
		"ConfigMap content must not be mutated during transient non-distribution")
}

// TestReconcileAuthKeys_TransientNonDistribution_RefreshesAuthorization covers
// the EN-1491 follow-up: when EVERY matching credential is still unresolved but
// a prior ConfigMap holds its key, reconcileAuthKeys must keep the pending
// StatefulSet fail-safe (do not roll a possibly-keyless template) yet rebuild
// the ConfigMap so the stored authorization metadata tracks the live spec —
// carrying only the key material forward. A narrowed / god-cleared Credentials
// must not preserve stale privileges indefinitely while its Secret stays
// undistributed.
func TestReconcileAuthKeys_TransientNonDistribution_RefreshesAuthorization(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
		credName    = "thierry-cred"
	)
	selector := map[string]string{"tier": "gold"}
	scheme := authKeysScheme(t)

	// Prior ConfigMap: the credential's key with STALE broad authorization
	// (god + read/write), keyed by the same stable identity reconcileAuthKeys
	// writes.
	fileName := pubKeyFileName("credentials", credName)
	prior := authKeysJSON{Keys: []authKeyEntry{{
		KeyID:         "k1",
		PublicKeyFile: "/auth-keys/" + fileName,
		Scopes:        []string{"read", "write"},
		God:           true,
	}}}
	priorRaw, err := json.Marshal(prior)
	require.NoError(t, err)
	existingCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: authKeysConfigMapName(clusterName), Namespace: namespace},
		Data: map[string]string{
			"auth-keys.json": string(priorRaw),
			fileName:         "deadbeef",
		},
	}

	// Live Credentials: still undistributed, but authorization has been narrowed
	// to read-only with god cleared (matchingCredentials sets Scopes=["read"],
	// God=false).
	cred := matchingCredentials(credName, selector, false, "", "")

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existingCM, cred).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	cluster := authEnabledCluster(clusterName, namespace, selector)

	credentials, pending, err := r.reconcileAuthKeys(context.Background(), cluster)
	require.NoError(t, err)
	assert.True(t, pending, "every credential still unresolved must keep the StatefulSet fail-safe (pending)")
	require.Len(t, credentials, 1, "the carried key must be returned")

	// ConfigMap rebuilt: key material carried, authorization refreshed from the
	// live spec.
	cm := &corev1.ConfigMap{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: authKeysConfigMapName(clusterName)}, cm))
	var got authKeysJSON
	require.NoError(t, json.Unmarshal([]byte(cm.Data["auth-keys.json"]), &got))
	require.Len(t, got.Keys, 1)
	assert.Equal(t, "k1", got.Keys[0].KeyID, "key material must be carried forward")
	assert.Equal(t, "deadbeef", cm.Data[fileName], "public key blob must be carried forward")
	assert.Equal(t, []string{"read"}, got.Keys[0].Scopes, "scopes must be refreshed from the live spec")
	assert.False(t, got.Keys[0].God, "god must be refreshed (cleared) from the live spec")
}

// TestReconcileAuthKeys_TransientNonDistribution_AuthDisabled_NotPending covers
// the auth-disabled carve-out: a Cluster with spec.auth.enabled=false has
// matching-but-undistributed Credentials. Because buildEnvVars never emits
// AUTH_ED25519_KEYS for an auth-disabled cluster, there is no crash-loop to
// guard against, so reconcileAuthKeys must NOT report pending (which would stall
// the StatefulSet pass during Credentials churn for a cluster that never needed
// auth keys). It falls through to the removal path and deletes the ConfigMap.
func TestReconcileAuthKeys_TransientNonDistribution_AuthDisabled_NotPending(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
	)
	selector := map[string]string{"tier": "gold"}

	scheme := authKeysScheme(t)
	existingCM := existingAuthKeysConfigMap(clusterName, namespace)
	cred := matchingCredentials("thierry-cred", selector, false, "", "")

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingCM, cred).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	// Auth explicitly disabled.
	disabled := false
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace, Labels: selector},
		Spec: ledgerv1alpha1.ClusterSpec{
			Auth: &ledgerv1alpha1.AuthorizationConfig{Enabled: &disabled},
		},
	}

	credentials, pending, err := r.reconcileAuthKeys(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, pending, "auth-disabled cluster must not report pending on transient non-distribution")
	assert.Nil(t, credentials)

	// With auth disabled there is no wiring to preserve; the ConfigMap is removed
	// like any other no-effective-keys case.
	cm := &corev1.ConfigMap{}
	err = c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: authKeysConfigMapName(clusterName)}, cm)
	assert.True(t, apierrors.IsNotFound(err),
		"auth-keys ConfigMap must be deleted for an auth-disabled cluster")
}

// TestReconcileAuthKeys_IssuerBacked_AllUnresolved_NotPending is the regression
// for the EN-1487 P1 finding: an OIDC-issuer-backed Cluster (auth enabled, issuer
// set, no Ed25519 keys required) whose matching Credentials are ALL unresolved must
// NOT be frozen. The server's validateAuthConfig accepts AUTH_ENABLED=true backed
// by an issuer alone, and the operator emits no AUTH_ED25519_KEYS when no key
// resolves, so there is no keyless crash-loop to guard against. Freezing here would
// block image/replica/TLS updates indefinitely. reconcileAuthKeys must report
// pending=false so the StatefulSet pass proceeds; with no prior key and nothing to
// carry forward the (empty) ConfigMap is removed like any no-effective-keys case.
func TestReconcileAuthKeys_IssuerBacked_AllUnresolved_NotPending(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
	)
	selector := map[string]string{"tier": "gold"}

	scheme := authKeysScheme(t)
	// A never-distributed matching Credentials, exactly the case the finding
	// describes ("adding a never-distributed Credentials to an OIDC cluster").
	cred := matchingCredentials("thierry-cred", selector, false, "", "")

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cred).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	cluster := issuerBackedCluster(clusterName, namespace, selector)

	credentials, pending, err := r.reconcileAuthKeys(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, pending,
		"an issuer-backed cluster must never be frozen by an unresolved Credentials — it authenticates on the issuer")
	assert.Nil(t, credentials, "no key resolves, so no key info is returned")

	// No prior ConfigMap existed; none must be created (no effective keys).
	cm := &corev1.ConfigMap{}
	err = c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: authKeysConfigMapName(clusterName)}, cm)
	assert.True(t, apierrors.IsNotFound(err),
		"no auth-keys ConfigMap should be created for an issuer-backed cluster with no resolvable keys")
}

// TestReconcileAuthKeys_IssuerBacked_AllUnresolved_PreservesPriorKeyNotPending
// covers the issuer-backed variant where a prior ConfigMap already holds the now-
// unresolved credential's key. The cluster still must NOT freeze (issuer keeps auth
// valid), yet the carried-forward key material is preserved and the ConfigMap is
// rebuilt so a later StatefulSet pass keeps mounting it. This proves the P1 fix
// lifts only the freeze, without stripping any existing key.
func TestReconcileAuthKeys_IssuerBacked_AllUnresolved_PreservesPriorKeyNotPending(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
		credName    = "thierry-cred"
	)
	selector := map[string]string{"tier": "gold"}
	scheme := authKeysScheme(t)

	existingCM := authKeysConfigMapWithEntries(clusterName, namespace, map[string]struct {
		keyID  string
		pubKey string
	}{
		credName: {keyID: "kid-old", pubKey: "deadbeef"},
	})
	cred := matchingCredentials(credName, selector, false, "", "")

	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(existingCM, cred).Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}
	cluster := issuerBackedCluster(clusterName, namespace, selector)

	credentials, pending, err := r.reconcileAuthKeys(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, pending, "issuer-backed cluster must not freeze even with a carried-forward key")
	require.Len(t, credentials, 1, "the prior key must be carried forward, not dropped")

	// The ConfigMap must still carry the prior key material so the StatefulSet keeps
	// mounting a valid key set.
	cm := &corev1.ConfigMap{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: authKeysConfigMapName(clusterName)}, cm))
	ids := keyIDsInConfigMap(t, cm)
	assert.Contains(t, ids, "kid-old", "the carried-forward key must be preserved")
	assert.Equal(t, "deadbeef", cm.Data[pubKeyFileName("credentials", credName)],
		"the carried-forward pubkey blob must be preserved")
}

// TestReconcileAuthKeys_NoMatch_DeletesConfigMap covers the legitimate removal
// path: zero Credentials match the selector, so the ConfigMap is deleted and no
// pending signal is raised.
func TestReconcileAuthKeys_NoMatch_DeletesConfigMap(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
	)

	scheme := authKeysScheme(t)
	existingCM := existingAuthKeysConfigMap(clusterName, namespace)
	// A Credentials that does NOT match the cluster labels.
	cred := matchingCredentials("other-cred", map[string]string{"tier": "silver"}, true, namespace, "some-secret")

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingCM, cred).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	// Cluster carries labels matched by no Credentials.
	cluster := authEnabledCluster(clusterName, namespace, map[string]string{"tier": "gold"})

	credentials, pending, err := r.reconcileAuthKeys(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, pending, "no matching credentials is a legitimate removal, not pending")
	assert.Nil(t, credentials)

	cm := &corev1.ConfigMap{}
	err = c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: authKeysConfigMapName(clusterName)}, cm)
	assert.True(t, apierrors.IsNotFound(err),
		"auth-keys ConfigMap must be deleted when no Credentials match the selector")
}

// TestReconcileAuthKeys_Distributed_CreatesConfigMap covers convergence: once a
// matching Credentials becomes distributed and its Secret is readable, the
// ConfigMap is (re)created with the aggregated keys and pending is cleared.
func TestReconcileAuthKeys_Distributed_CreatesConfigMap(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
		secretName  = "thierry-cred-secret"
	)
	selector := map[string]string{"tier": "gold"}

	scheme := authKeysScheme(t)
	cred := matchingCredentials("thierry-cred", selector, true, namespace, secretName)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace},
		Data: map[string][]byte{
			"pubkey.hex": []byte("deadbeef"),
			"key-id":     []byte("kid-123"),
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(cred, secret).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	cluster := authEnabledCluster(clusterName, namespace, selector)

	credentials, pending, err := r.reconcileAuthKeys(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, pending, "a distributed credentials must not be pending")
	require.Len(t, credentials, 1, "the distributed credentials must be resolved into a key")
	assert.Equal(t, "kid-123", credentials[0].KeyID)

	cm := &corev1.ConfigMap{}
	err = c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: authKeysConfigMapName(clusterName)}, cm)
	require.NoError(t, err, "auth-keys ConfigMap must be created once credentials are distributed")
	assert.Contains(t, cm.Data, "auth-keys.json")
	assert.Equal(t, "deadbeef", cm.Data["credentials-thierry-cred-pubkey.hex"])
}

// TestReconcileAuthKeys_FullyResolved_MalformedConfigMap_SelfHeals is the
// regression for the delta finding: the fully-resolved path (no unresolved
// credential) must stay self-healing. A pre-existing auth-keys ConfigMap whose
// auth-keys.json is malformed must NOT wedge reconciliation — since every matched
// credential is resolved there is nothing to carry forward, so reconcileAuthKeys
// must skip reading the corrupt ConfigMap and rebuild it unconditionally from the
// freshly-resolved set. Before the fix the unconditional read returned a parse
// error that failed the whole reconcile, freezing a cluster on a bad ConfigMap.
func TestReconcileAuthKeys_FullyResolved_MalformedConfigMap_SelfHeals(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
		secretName  = "thierry-cred-secret"
	)
	selector := map[string]string{"tier": "gold"}

	scheme := authKeysScheme(t)
	// Malformed existing ConfigMap: auth-keys.json is not valid JSON.
	malformedCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      authKeysConfigMapName(clusterName),
			Namespace: namespace,
		},
		Data: map[string]string{
			"auth-keys.json": `{"keys":[{"keyId":"corrupt",`, // truncated / invalid JSON
		},
	}
	cred := matchingCredentials("thierry-cred", selector, true, namespace, secretName)
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace},
		Data: map[string][]byte{
			"pubkey.hex": []byte("deadbeef"),
			"key-id":     []byte("kid-123"),
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(malformedCM, cred, secret).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	cluster := authEnabledCluster(clusterName, namespace, selector)

	credentials, pending, err := r.reconcileAuthKeys(context.Background(), cluster)
	require.NoError(t, err, "a malformed existing ConfigMap must not wedge the fully-resolved path")
	assert.False(t, pending, "everything resolved must not be pending")
	require.Len(t, credentials, 1, "the resolved credential must be returned")

	// The ConfigMap must be rebuilt (self-healed) from the resolved set, dropping
	// the corrupt content entirely.
	cm := &corev1.ConfigMap{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: authKeysConfigMapName(clusterName)}, cm))
	ids := keyIDsInConfigMap(t, cm)
	assert.Contains(t, ids, "kid-123", "the resolved key must be written")
	assert.NotContains(t, ids, "corrupt", "the corrupt prior content must be gone")
	assert.Len(t, ids, 1)
	assert.Equal(t, "deadbeef", cm.Data["credentials-thierry-cred-pubkey.hex"])
}

// TestReconcileAuthKeys_Partial_MalformedConfigMap_SelfHeals covers the
// carry-forward path when the existing ConfigMap read SUCCEEDS but its content is
// corrupt: the malformed prior key for the still-unresolved credential is
// genuinely unrecoverable, so it degrades to "no prior key" (contributes
// nothing), while the freshly-resolved credential is still propagated and the
// cluster is not frozen. This is the self-heal case — distinct from a transient
// API read error, which must requeue (next test).
func TestReconcileAuthKeys_Partial_MalformedConfigMap_SelfHeals(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
		secretName  = "thierry-cred-a-secret"
	)
	selector := map[string]string{"tier": "gold"}

	scheme := authKeysScheme(t)
	// Existing ConfigMap present but its auth-keys.json is corrupt.
	malformedCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      authKeysConfigMapName(clusterName),
			Namespace: namespace,
		},
		Data: map[string]string{
			"auth-keys.json": `{"keys":[{"keyId":"corrupt",`, // invalid JSON
		},
	}
	credA := matchingCredentials("thierry-cred-a", selector, true, namespace, secretName)
	credB := matchingCredentials("thierry-cred-b", selector, false, "", "")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace},
		Data: map[string][]byte{
			"pubkey.hex": []byte("aaaa"),
			"key-id":     []byte("kid-a-new"),
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(malformedCM, credA, credB, secret).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	cluster := authEnabledCluster(clusterName, namespace, selector)

	credentials, pending, err := r.reconcileAuthKeys(context.Background(), cluster)
	require.NoError(t, err, "malformed prior content must self-heal, not wedge the partial path")
	assert.False(t, pending, "the resolved credential is non-empty, so not pending")
	require.Len(t, credentials, 1, "only the resolved credential contributes; the corrupt prior key is unrecoverable")

	cm := &corev1.ConfigMap{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: authKeysConfigMapName(clusterName)}, cm))
	ids := keyIDsInConfigMap(t, cm)
	assert.Contains(t, ids, "kid-a-new", "the resolved key must be propagated")
	assert.NotContains(t, ids, "corrupt", "the corrupt content must be dropped")
	assert.Len(t, ids, 1)
}

// TestReconcileAuthKeys_Partial_TransientReadError_DoesNotDropKey is the
// regression for the fail-safe finding: during partial resolution, if the
// existing-ConfigMap read (the API Get) fails TRANSIENTLY, reconcileAuthKeys must
// NOT proceed treating prior keys as absent. Doing so would drop the
// still-unresolved credential's still-authorized key — and because another
// credential resolved, the merged set is non-empty so the crash-loop guard would
// not catch it. Instead the reconcile must return an error so the controller
// requeues and retries once the API recovers, leaving the existing ConfigMap
// (with the prior key) untouched.
func TestReconcileAuthKeys_Partial_TransientReadError_DoesNotDropKey(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
		secretName  = "thierry-cred-a-secret"
	)
	selector := map[string]string{"tier": "gold"}
	cmName := authKeysConfigMapName(clusterName)

	scheme := authKeysScheme(t)
	// Valid existing ConfigMap holding the still-unresolved credential's prior key.
	existingCM := authKeysConfigMapWithEntries(clusterName, namespace, map[string]struct {
		keyID  string
		pubKey string
	}{
		"thierry-cred-b": {keyID: "kid-b-old", pubKey: "bbbb"},
	})
	credA := matchingCredentials("thierry-cred-a", selector, true, namespace, secretName)
	credB := matchingCredentials("thierry-cred-b", selector, false, "", "")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace},
		Data: map[string][]byte{
			"pubkey.hex": []byte("aaaa"),
			"key-id":     []byte("kid-a-new"),
		},
	}

	// Fail ONLY the FIRST auth-keys ConfigMap Get transiently (a server-timeout API
	// error, not NotFound); let every subsequent Get succeed. This models the exact
	// race the finding describes: the carry-forward read fails on a blip, but a
	// later CreateOrUpdate Get on the same ConfigMap would succeed — so under the
	// buggy swallow-all behavior the reconcile would drop cred-b's still-authorized
	// key and rewrite the ConfigMap without it (the crash-loop guard cannot catch
	// it because cred-a resolved, keeping the merged set non-empty). With the fix
	// the first failure propagates and the reconcile bails before any write.
	var cmGets int
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingCM, credA, credB, secret).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.ConfigMap); ok && key.Name == cmName && key.Namespace == namespace {
					cmGets++
					if cmGets == 1 {
						return apierrors.NewServerTimeout(
							corev1.Resource("configmaps"), "get", 1)
					}
				}

				return cl.Get(ctx, key, obj, opts...)
			},
		}).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	cluster := authEnabledCluster(clusterName, namespace, selector)

	credentials, pending, err := r.reconcileAuthKeys(context.Background(), cluster)
	require.Error(t, err, "a transient carry-forward read failure must not be swallowed")
	assert.False(t, apierrors.IsNotFound(err), "the error must be the transient API error, not a NotFound")
	assert.Nil(t, credentials, "no key set must be returned when the reconcile errors out")
	assert.False(t, pending)
	assert.Equal(t, 1, cmGets, "the reconcile must bail on the first failed carry-forward read, before any later Get")

	// Read the ConfigMap back through the SAME store (subsequent Gets succeed): the
	// existing ConfigMap must be preserved verbatim — the still-authorized prior key
	// must NOT have been dropped, because the reconcile bailed before CreateOrUpdate.
	cm := &corev1.ConfigMap{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: cmName}, cm))
	ids := keyIDsInConfigMap(t, cm)
	assert.Contains(t, ids, "kid-b-old", "the still-authorized prior key must be preserved, not dropped")
	assert.Equal(t, "bbbb", cm.Data[pubKeyFileName("credentials", "thierry-cred-b")],
		"the still-authorized prior pubkey blob must be preserved")
}

// keyIDsInConfigMap parses the auth-keys.json in a ConfigMap and returns the set
// of keyIDs it advertises, for asserting union membership.
func keyIDsInConfigMap(t *testing.T, cm *corev1.ConfigMap) map[string]struct{} {
	t.Helper()
	var parsed authKeysJSON
	require.NoError(t, json.Unmarshal([]byte(cm.Data["auth-keys.json"]), &parsed))
	ids := map[string]struct{}{}
	for _, e := range parsed.Keys {
		ids[e.KeyID] = struct{}{}
	}

	return ids
}

// TestReconcileAuthKeys_PartialNonDistribution_PreservesConfigMap covers the
// PROPER partial-resolution behavior (EN-1491, superseding the naive freeze):
// two Credentials match the Cluster, one is freshly resolved (new/rotated key)
// and one is transiently unresolved but has a prior key in the existing
// ConfigMap. reconcileAuthKeys must NOT freeze — it must build the ConfigMap from
// the UNION of the newly-resolved key (propagated) and the pending credential's
// carried-forward prior key (preserved), and must NOT report pending so the
// StatefulSet pass proceeds normally.
func TestReconcileAuthKeys_PartialNonDistribution_PreservesConfigMap(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
		secretName  = "thierry-cred-a-secret"
	)
	selector := map[string]string{"tier": "gold"}

	scheme := authKeysScheme(t)
	// Prior ConfigMap holds a key for the still-pending credential (cred-b) so it
	// can be carried forward.
	existingCM := authKeysConfigMapWithEntries(clusterName, namespace, map[string]struct {
		keyID  string
		pubKey string
	}{
		"thierry-cred-b": {keyID: "kid-b-old", pubKey: "bbbb"},
	})
	// cred-a freshly resolved (new/rotated key), cred-b still undistributed.
	credA := matchingCredentials("thierry-cred-a", selector, true, namespace, secretName)
	credB := matchingCredentials("thierry-cred-b", selector, false, "", "")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace},
		Data: map[string][]byte{
			"pubkey.hex": []byte("aaaa"),
			"key-id":     []byte("kid-a-new"),
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingCM, credA, credB, secret).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	cluster := authEnabledCluster(clusterName, namespace, selector)

	credentials, pending, err := r.reconcileAuthKeys(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, pending, "partial resolution must NOT freeze the cluster")
	require.Len(t, credentials, 2, "the merged set must carry both the resolved and the carried-forward key")

	cm := &corev1.ConfigMap{}
	err = c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: authKeysConfigMapName(clusterName)}, cm)
	require.NoError(t, err, "auth-keys ConfigMap must exist")

	ids := keyIDsInConfigMap(t, cm)
	assert.Contains(t, ids, "kid-a-new", "the freshly-resolved key must be propagated")
	assert.Contains(t, ids, "kid-b-old", "the pending credential's prior key must be preserved")
	assert.Len(t, ids, 2, "the merged set must be exactly the union")

	// The carried-forward pubkey blob must be preserved verbatim, and the new one
	// propagated.
	assert.Equal(t, "bbbb", cm.Data[pubKeyFileName("credentials", "thierry-cred-b")],
		"pending credential's prior pubkey must be carried forward")
	assert.Equal(t, "aaaa", cm.Data[pubKeyFileName("credentials", "thierry-cred-a")],
		"freshly-resolved pubkey must be written")
}

// TestReconcileAuthKeys_PartialResolution_BrokenCredentialDoesNotBlockRotation
// proves a permanently-broken Credentials never blocks rotation/propagation of
// the healthy ones. cred-b stays undistributed across two reconciles while cred-a
// rotates twice; each reconcile must propagate cred-a's latest key while keeping
// cred-b's carried-forward prior key.
func TestReconcileAuthKeys_PartialResolution_BrokenCredentialDoesNotBlockRotation(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
		secretName  = "thierry-cred-a-secret"
	)
	selector := map[string]string{"tier": "gold"}

	scheme := authKeysScheme(t)
	existingCM := authKeysConfigMapWithEntries(clusterName, namespace, map[string]struct {
		keyID  string
		pubKey string
	}{
		"thierry-cred-b": {keyID: "kid-b-old", pubKey: "bbbb"},
	})
	credA := matchingCredentials("thierry-cred-a", selector, true, namespace, secretName)
	credB := matchingCredentials("thierry-cred-b", selector, false, "", "")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace},
		Data: map[string][]byte{
			"pubkey.hex": []byte("a1"),
			"key-id":     []byte("kid-a-v1"),
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingCM, credA, credB, secret).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	cluster := authEnabledCluster(clusterName, namespace, selector)

	// First reconcile: cred-a v1 propagated, cred-b carried forward.
	_, pending, err := r.reconcileAuthKeys(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, pending)

	cm := &corev1.ConfigMap{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: authKeysConfigMapName(clusterName)}, cm))
	ids := keyIDsInConfigMap(t, cm)
	assert.Contains(t, ids, "kid-a-v1")
	assert.Contains(t, ids, "kid-b-old")

	// Rotate cred-a while cred-b stays broken.
	sec := &corev1.Secret{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: secretName}, sec))
	sec.Data["pubkey.hex"] = []byte("a2")
	sec.Data["key-id"] = []byte("kid-a-v2")
	require.NoError(t, c.Update(context.Background(), sec))

	// Second reconcile: cred-a's further rotation must still propagate.
	_, pending, err = r.reconcileAuthKeys(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, pending, "a permanently-broken credential must not block further rotation of the healthy one")

	cm = &corev1.ConfigMap{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: authKeysConfigMapName(clusterName)}, cm))
	ids = keyIDsInConfigMap(t, cm)
	assert.Contains(t, ids, "kid-a-v2", "the healthy credential's rotation must propagate despite the broken one")
	assert.Contains(t, ids, "kid-b-old", "the broken credential still keeps its own last-known key")
	assert.NotContains(t, ids, "kid-a-v1", "the superseded key must be gone")
}

// TestReconcileAuthKeys_PartialResolution_NoPriorKey_Skips covers a partial state
// where the still-unresolved credential was NEVER distributed (no prior entry in
// the existing ConfigMap). It simply contributes no key; the resolved credential
// is propagated and the cluster is not frozen.
func TestReconcileAuthKeys_PartialResolution_NoPriorKey_Skips(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
		secretName  = "thierry-cred-a-secret"
	)
	selector := map[string]string{"tier": "gold"}

	scheme := authKeysScheme(t)
	// Existing ConfigMap holds only cred-a's prior key; cred-b was never distributed.
	existingCM := authKeysConfigMapWithEntries(clusterName, namespace, map[string]struct {
		keyID  string
		pubKey string
	}{
		"thierry-cred-a": {keyID: "kid-a-old", pubKey: "aaaa"},
	})
	credA := matchingCredentials("thierry-cred-a", selector, true, namespace, secretName)
	credB := matchingCredentials("thierry-cred-b", selector, false, "", "")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: namespace},
		Data: map[string][]byte{
			"pubkey.hex": []byte("aaaa2"),
			"key-id":     []byte("kid-a-new"),
		},
	}

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(existingCM, credA, credB, secret).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	cluster := authEnabledCluster(clusterName, namespace, selector)

	credentials, pending, err := r.reconcileAuthKeys(context.Background(), cluster)
	require.NoError(t, err)
	assert.False(t, pending, "a never-distributed pending credential must not freeze the cluster")
	require.Len(t, credentials, 1, "only the resolved credential contributes a key")

	cm := &corev1.ConfigMap{}
	require.NoError(t, c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: authKeysConfigMapName(clusterName)}, cm))
	ids := keyIDsInConfigMap(t, cm)
	assert.Contains(t, ids, "kid-a-new", "the resolved credential's key must be propagated")
	assert.Len(t, ids, 1, "the never-distributed credential contributes nothing")
}

// TestCredentialsToClusters_EnqueuesMatchingCluster verifies the watch mapping
// that drives convergence: a Credentials change must enqueue every Cluster its
// selector matches, so the transition non-distributed -> distributed triggers a
// re-reconcile without waiting for the requeue safety net.
func TestCredentialsToClusters_EnqueuesMatchingCluster(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
	)
	selector := map[string]string{"tier": "gold"}

	scheme := authKeysScheme(t)
	matching := authEnabledCluster(clusterName, namespace, selector)
	nonMatching := authEnabledCluster("other", namespace, map[string]string{"tier": "silver"})

	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(matching, nonMatching).
		Build()
	r := &ClusterReconciler{Client: c, Scheme: scheme}

	cred := matchingCredentials("thierry-cred", selector, true, namespace, "secret")

	requests := r.credentialsToClusters(context.Background(), client.Object(cred))
	require.Len(t, requests, 1, "only the Cluster matched by the selector must be enqueued")
	assert.Equal(t, types.NamespacedName{Name: clusterName, Namespace: namespace}, requests[0].NamespacedName)
}

// TestReconcileVolumeProtectionPass_RunsIndependentlyOfAuthKeys is the core
// EN-1487 Option A guarantee at the seam the AuthKeysPending branch now calls:
// reconcileVolumeProtectionPass must maintain the deletion-protection labels
// using nothing but the Cluster spec and the volumes already present in the
// cluster. It takes no auth-keys credentials, no specHash and no TLS mode, so it
// is safe to invoke while the rest of the StatefulSet pass is deferred during
// Credentials churn. Here a single-replica cluster with a bound data PVC/PV must
// have both stamped, and the pass must report no requeue.
func TestReconcileVolumeProtectionPass_RunsIndependentlyOfAuthKeys(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
	)

	// resourceName(clusterName) == "ledger-thierry"; the data PVC for ordinal 0 is
	// "data-ledger-thierry-0".
	stsName := resourceName(clusterName)
	boundPVC, boundPV := boundPVCAndPV("data-"+stsName+"-0", "pv-thierry-0", namespace)
	cs := k8sfake.NewClientset(boundPVC, boundPV)

	r := &ClusterReconciler{Clientset: cs}

	// Single replica, only the data volume PVC-backed — keep the fixture minimal
	// while still exercising both the PVC and the bound PV stamping path.
	replicas := int32(1)
	hostPath := &ledgerv1alpha1.HostPathVolumeSpec{Path: "/mnt/wal"}
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
		Spec: ledgerv1alpha1.ClusterSpec{
			Replicas: &replicas,
			Persistence: ledgerv1alpha1.PersistenceSpec{
				// hostPath volumes are not PVC-backed, so only "data" is reconciled.
				WAL:       ledgerv1alpha1.VolumeSpec{HostPath: hostPath},
				ColdCache: ledgerv1alpha1.VolumeSpec{HostPath: hostPath},
			},
		},
	}

	result, err := r.reconcileVolumeProtectionPass(context.Background(), cluster)
	require.NoError(t, err)
	require.True(t, result.IsZero(), "a bound PVC leaves nothing to requeue")

	gotPVC, err := cs.CoreV1().PersistentVolumeClaims(namespace).Get(context.Background(), "data-"+stsName+"-0", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, labelDeletionProtectionValue, gotPVC.Labels[labelDeletionProtection],
		"deletion-protection label must be stamped on the PVC by the pass")

	gotPV, err := cs.CoreV1().PersistentVolumes().Get(context.Background(), "pv-thierry-0", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, labelDeletionProtectionValue, gotPV.Labels[labelDeletionProtection],
		"deletion-protection label must be stamped on the bound PV by the pass")
}

// TestReconcileVolumeProtectionPass_RequeuesWhilePreservingStatefulSet proves
// the two halves of Option A live side by side: while AuthKeysPending, running
// volume protection (a) requeues when a desired PVC does not exist yet — the
// signal the pending branch folds into its own requeue — and (b) does NOT touch
// the existing StatefulSet template, which the deferred StatefulSet pass is
// responsible for preserving. The pass is given a pre-existing StatefulSet and
// must leave it byte-for-byte unchanged.
func TestReconcileVolumeProtectionPass_RequeuesWhilePreservingStatefulSet(t *testing.T) {
	t.Parallel()

	const (
		clusterName = "thierry"
		namespace   = "ledger-v3"
	)
	stsName := resourceName(clusterName)

	// A StatefulSet standing in for the auth-wired template that must survive the
	// pending window untouched. The typed clientset holds it so we can assert the
	// volume-protection pass never mutates it.
	existingReplicas := int32(3)
	existingSTS := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: stsName, Namespace: namespace, ResourceVersion: "424242"},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &existingReplicas,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{"preserve-me": "auth-wired"},
				},
				Spec: corev1.PodSpec{
					Volumes: []corev1.Volume{{Name: "auth-keys"}},
				},
			},
		},
	}
	// No PVCs exist yet -> the pass must report pending (requeue).
	cs := k8sfake.NewClientset(existingSTS)

	r := &ClusterReconciler{Clientset: cs}

	replicas := int32(1)
	hostPath := &ledgerv1alpha1.HostPathVolumeSpec{Path: "/mnt/wal"}
	cluster := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: clusterName, Namespace: namespace},
		Spec: ledgerv1alpha1.ClusterSpec{
			Replicas: &replicas,
			Persistence: ledgerv1alpha1.PersistenceSpec{
				WAL:       ledgerv1alpha1.VolumeSpec{HostPath: hostPath},
				ColdCache: ledgerv1alpha1.VolumeSpec{HostPath: hostPath},
			},
		},
	}

	result, err := r.reconcileVolumeProtectionPass(context.Background(), cluster)
	require.NoError(t, err)
	require.False(t, result.IsZero(), "an absent PVC under protection must requeue")
	require.Equal(t, volumeBindRequeueInterval, result.RequeueAfter,
		"the pass must requeue on the volume-bind interval, which the pending branch reuses")

	// The StatefulSet template must be preserved verbatim: the pending path must
	// never roll it while auth keys are undistributed.
	gotSTS, err := cs.AppsV1().StatefulSets(namespace).Get(context.Background(), stsName, metav1.GetOptions{})
	require.NoError(t, err, "the existing StatefulSet must still be present")
	require.Equal(t, "424242", gotSTS.ResourceVersion,
		"the StatefulSet must not be rewritten by the volume-protection pass")
	require.Equal(t, "auth-wired", gotSTS.Spec.Template.Annotations["preserve-me"],
		"the auth-wired pod template must be preserved untouched")
	require.Equal(t, existingSTS.Spec.Template.Spec.Volumes, gotSTS.Spec.Template.Spec.Volumes,
		"the auth-keys volume wiring must be preserved untouched")
}
