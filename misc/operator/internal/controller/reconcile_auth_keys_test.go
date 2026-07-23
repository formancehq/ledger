//go:build integration

package controller

import (
	"encoding/json"
	"testing"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

func TestReconcile_AuthKeysConfigMap(t *testing.T) {
	ns := createTestNamespace(t)

	// Create Cluster first so the credentials has a namespace to distribute into.
	ls := newCluster("authcm-cluster", ns)
	ls.Labels = map[string]string{"tier": "authcm"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Create credentials with matching selector.
	credentials := newCredentials("authcm-credentials", []string{"read", "write"}, map[string]string{"tier": "authcm"})
	require.NoError(t, k8sClient.Create(ctx, credentials))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, credentials) //nolint:errcheck // best-effort cleanup
	})

	// Wait for the credentials to be ready (secret created in the service's namespace).
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "authcm-credentials"}, credentials); err != nil {
			return false
		}
		return credentials.Status.Phase == "Ready" && len(credentials.Status.DistributedSecretRefs) > 0
	}, "credentials should be Ready with a distributed secret ref")

	// Wait for the auth-keys ConfigMap to appear.
	cm := &corev1.ConfigMap{}
	cmKey := types.NamespacedName{Namespace: ns, Name: "ledger-authcm-cluster-auth-keys"}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, cmKey, cm) == nil
	}, "auth-keys ConfigMap should be created")

	// Verify auth-keys.json exists and is valid.
	rawJSON, ok := cm.Data["auth-keys.json"]
	require.True(t, ok, "ConfigMap must contain auth-keys.json")

	var authKeys authKeysJSON
	require.NoError(t, json.Unmarshal([]byte(rawJSON), &authKeys))
	require.Len(t, authKeys.Keys, 1)
	assert.Equal(t, credentials.Status.KeyID, authKeys.Keys[0].KeyID)
	assert.Equal(t, "/auth-keys/credentials-authcm-credentials-pubkey.hex", authKeys.Keys[0].PublicKeyFile)
	assert.Equal(t, []string{"read", "write"}, authKeys.Keys[0].Scopes)

	// Verify the individual pubkey file exists.
	pubKeyData, ok := cm.Data["credentials-authcm-credentials-pubkey.hex"]
	assert.True(t, ok, "ConfigMap must contain the credentials pubkey file")
	assert.NotEmpty(t, pubKeyData)
}

func TestReconcile_AuthKeysStatefulSet(t *testing.T) {
	ns := createTestNamespace(t)

	// Create Cluster first so the credentials has a namespace to distribute into.
	ls := newCluster("authsts-cluster", ns)
	ls.Labels = map[string]string{"tier": "authsts"}
	// Auth env (incl. AUTH_ED25519_KEYS) is only emitted once TLS has converged
	// to `required` (see buildEnvVars' auth deferral). A fresh cluster with no
	// prior StatefulSet targets the desired mode directly (computeTargetTLSMode
	// bootstrap branch), so enabling TLS here makes the reconcile reach
	// `required` immediately and the AUTH_ED25519_KEYS assertion below holds.
	ls.Spec.TLS = &ledgerv1alpha1.TLSConfig{Enabled: true, SecretName: "authsts-tls"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Create credentials with matching selector.
	credentials := newCredentials("authsts-credentials", []string{"read"}, map[string]string{"tier": "authsts"})
	require.NoError(t, k8sClient.Create(ctx, credentials))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, credentials) //nolint:errcheck // best-effort cleanup
	})

	// Wait for credentials to be ready.
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "authsts-credentials"}, credentials); err != nil {
			return false
		}
		return credentials.Status.Phase == "Ready" && len(credentials.Status.DistributedSecretRefs) > 0
	}, "credentials should be Ready")

	// Wait for the StatefulSet to appear.
	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-authsts-cluster", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")

	// Wait for the auth-keys volume to be present (may need a re-reconciliation).
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-authsts-cluster", Namespace: ns}, sts); err != nil {
			return false
		}
		for _, v := range sts.Spec.Template.Spec.Volumes {
			if v.Name == "auth-keys" {
				return true
			}
		}
		return false
	}, "StatefulSet should have auth-keys volume")

	// Verify auth-keys volume mount.
	container := sts.Spec.Template.Spec.Containers[0]
	requireVolumeMount(t, container.VolumeMounts, "auth-keys", "/auth-keys")

	// Verify the mount is read-only.
	for _, m := range container.VolumeMounts {
		if m.Name == "auth-keys" {
			assert.True(t, m.ReadOnly, "auth-keys volume mount should be read-only")
		}
	}

	// Verify the AUTH_ED25519_KEYS env var points at the mounted file.
	authEd25519 := findEnv(container.Env, "AUTH_ED25519_KEYS")
	if assert.NotNil(t, authEd25519, "AUTH_ED25519_KEYS env var should be set when credentials exist") {
		assert.Equal(t, "/auth-keys/auth-keys.json", authEd25519.Value)
	}
}

func TestReconcile_NoAgentsNoConfigMap(t *testing.T) {
	ns := createTestNamespace(t)

	// Create Cluster without matching credentials.
	ls := newCluster("no-credentials-cluster", ns)
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Wait for StatefulSet to confirm reconciliation ran.
	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-no-credentials-cluster", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")

	// Verify no auth-keys ConfigMap.
	cm := &corev1.ConfigMap{}
	err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "ledger-no-credentials-cluster-auth-keys"}, cm)
	assert.True(t, apierrors.IsNotFound(err), "auth-keys ConfigMap should not exist when no credentials match")

	// Verify no auth-keys volume in the StatefulSet.
	for _, v := range sts.Spec.Template.Spec.Volumes {
		assert.NotEqual(t, "auth-keys", v.Name, "auth-keys volume should not exist without credentials")
	}

	// Verify the AUTH_ED25519_KEYS env var is not set.
	container := sts.Spec.Template.Spec.Containers[0]
	assert.Nil(t, findEnv(container.Env, "AUTH_ED25519_KEYS"), "AUTH_ED25519_KEYS env var should not be set without credentials")
}

func TestReconcile_AuthKeysHashAnnotation(t *testing.T) {
	ns := createTestNamespace(t)

	// Create Cluster first so the credentials has a namespace to distribute into.
	ls := newCluster("authhash-cluster", ns)
	ls.Labels = map[string]string{"tier": "authhash"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Create credentials with matching selector.
	credentials := newCredentials("authhash-credentials", []string{"read"}, map[string]string{"tier": "authhash"})
	require.NoError(t, k8sClient.Create(ctx, credentials))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, credentials) //nolint:errcheck // best-effort cleanup
	})

	// Wait for credentials to be ready.
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "authhash-credentials"}, credentials); err != nil {
			return false
		}
		return credentials.Status.Phase == "Ready" && len(credentials.Status.DistributedSecretRefs) > 0
	}, "credentials should be Ready")

	// Wait for the StatefulSet with auth-keys hash annotation.
	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "ledger-authhash-cluster", Namespace: ns}, sts); err != nil {
			return false
		}
		_, ok := sts.Spec.Template.Annotations[annotationAuthKeysHash]
		return ok
	}, "StatefulSet pod template should have auth-keys-hash annotation")

	hashValue := sts.Spec.Template.Annotations[annotationAuthKeysHash]
	assert.Len(t, hashValue, 64, "auth-keys-hash annotation should be a 64-char SHA-256 hex string")
}
