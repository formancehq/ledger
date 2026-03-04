//go:build integration

package controller

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func TestReconcile_NsAgentCreatesSecret(t *testing.T) {
	ns := createTestNamespace(t)

	agent := newLedgerAgent("ns-creates-secret", ns, []string{"read"}, map[string]string{"app": "ledger"})
	require.NoError(t, k8sClient.Create(ctx, agent))

	// Wait for the Secret to appear in the same namespace.
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: ns,
		Name:      "ns-creates-secret-agent-keys",
	}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, secretKey, secret) == nil
	}, "Secret should be created in same namespace")

	// Verify secret data keys.
	assert.Contains(t, secret.Data, "seed.hex")
	assert.Contains(t, secret.Data, "pubkey.hex")
	assert.Contains(t, secret.Data, "key-id")

	// Verify non-empty values.
	assert.NotEmpty(t, secret.Data["seed.hex"])
	assert.NotEmpty(t, secret.Data["pubkey.hex"])
	assert.NotEmpty(t, secret.Data["key-id"])

	// Verify ownerReference is set (no finalizer needed).
	require.Len(t, secret.OwnerReferences, 1)
	assert.Equal(t, "ns-creates-secret", secret.OwnerReferences[0].Name)
}

func TestReconcile_NsAgentStatus(t *testing.T) {
	ns := createTestNamespace(t)

	agent := newLedgerAgent("ns-status-check", ns, []string{"read"}, map[string]string{"app": "ledger"})
	require.NoError(t, k8sClient.Create(ctx, agent))

	// Wait for status to be populated.
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "ns-status-check", Namespace: ns}, agent); err != nil {
			return false
		}
		return agent.Status.Phase == "Ready"
	}, "agent phase should be Ready")

	assert.NotEmpty(t, agent.Status.KeyID, "keyID must be set")
	assert.Equal(t, ns, agent.Status.SecretRef.Namespace)
	assert.Equal(t, "ns-status-check-agent-keys", agent.Status.SecretRef.Name)
}

func TestReconcile_NsAgentMatchesServices(t *testing.T) {
	ns := createTestNamespace(t)

	// Create a LedgerService with matching labels.
	ls := newLedgerService("ns-matched-svc", ns)
	ls.Labels = map[string]string{"app": "ns-matched"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Create namespace agent with matching selector.
	agent := newLedgerAgent("ns-matcher", ns, []string{"read"}, map[string]string{"app": "ns-matched"})
	require.NoError(t, k8sClient.Create(ctx, agent))

	// Wait for status to list the matched service.
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "ns-matcher", Namespace: ns}, agent); err != nil {
			return false
		}
		for _, ms := range agent.Status.MatchedServices {
			if ms.Name == "ns-matched-svc" && ms.Namespace == ns {
				return true
			}
		}
		return false
	}, "namespace agent should match the LedgerService")
}

func TestReconcile_NsAgentDoesNotMatchOtherNamespace(t *testing.T) {
	ns1 := createTestNamespace(t)
	ns2 := createTestNamespace(t)

	// Create a LedgerService in ns1.
	ls := newLedgerService("cross-ns-svc", ns1)
	ls.Labels = map[string]string{"app": "cross-ns"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Create namespace agent in ns2 with matching labels.
	agent := newLedgerAgent("cross-ns-agent", ns2, []string{"read"}, map[string]string{"app": "cross-ns"})
	require.NoError(t, k8sClient.Create(ctx, agent))

	// Wait for agent to be ready.
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "cross-ns-agent", Namespace: ns2}, agent); err != nil {
			return false
		}
		return agent.Status.Phase == "Ready"
	}, "agent should be Ready")

	// Verify no matched services (different namespace).
	assert.Empty(t, agent.Status.MatchedServices, "namespace agent should not match services in other namespaces")
}

func TestReconcile_NsAgentDeletion(t *testing.T) {
	ns := createTestNamespace(t)

	agent := newLedgerAgent("ns-to-delete", ns, []string{"read"}, map[string]string{"app": "ledger"})
	require.NoError(t, k8sClient.Create(ctx, agent))

	// Wait for the Secret to appear.
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: ns,
		Name:      "ns-to-delete-agent-keys",
	}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, secretKey, secret) == nil
	}, "Secret should be created")

	// Delete the agent.
	require.NoError(t, k8sClient.Delete(ctx, agent))

	// Wait for the Secret to be cleaned up by ownerReference cascade.
	requireEventually(t, func() bool {
		err := k8sClient.Get(ctx, secretKey, secret)
		return apierrors.IsNotFound(err)
	}, "Secret should be deleted after agent deletion")

	// Verify the agent itself is gone.
	requireEventually(t, func() bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: "ns-to-delete", Namespace: ns}, &ledgerv1alpha1.LedgerAgent{})
		return apierrors.IsNotFound(err)
	}, "agent should be deleted")
}

func TestReconcile_NsAgentAuthKeysConfigMap(t *testing.T) {
	ns := createTestNamespace(t)

	// Create namespace agent with matching selector.
	agent := newLedgerAgent("nsauthcm-agent", ns, []string{"read", "write"}, map[string]string{"tier": "nsauthcm"})
	require.NoError(t, k8sClient.Create(ctx, agent))

	// Wait for agent to be ready.
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "nsauthcm-agent", Namespace: ns}, agent); err != nil {
			return false
		}
		return agent.Status.Phase == "Ready" && agent.Status.SecretRef.Name != ""
	}, "namespace agent should be Ready with SecretRef")

	// Create LedgerService with matching labels.
	ls := newLedgerService("nsauthcm-svc", ns)
	ls.Labels = map[string]string{"tier": "nsauthcm"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Wait for the auth-keys ConfigMap to appear.
	cm := &corev1.ConfigMap{}
	cmKey := types.NamespacedName{Namespace: ns, Name: "nsauthcm-svc-auth-keys"}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, cmKey, cm) == nil
	}, "auth-keys ConfigMap should be created")

	// Verify auth-keys.json exists and is valid.
	rawJSON, ok := cm.Data["auth-keys.json"]
	require.True(t, ok, "ConfigMap must contain auth-keys.json")

	var authKeys authKeysJSON
	require.NoError(t, json.Unmarshal([]byte(rawJSON), &authKeys))
	require.Len(t, authKeys.Keys, 1)
	assert.Equal(t, agent.Status.KeyID, authKeys.Keys[0].KeyID)
	assert.Equal(t, "/auth-keys/nsagent-nsauthcm-agent-pubkey.hex", authKeys.Keys[0].PublicKeyFile)
	assert.Equal(t, []string{"read", "write"}, authKeys.Keys[0].Scopes)

	// Verify the individual pubkey file exists with nsagent prefix.
	pubKeyData, ok := cm.Data["nsagent-nsauthcm-agent-pubkey.hex"]
	assert.True(t, ok, "ConfigMap must contain the namespace agent pubkey file")
	assert.NotEmpty(t, pubKeyData)
}
