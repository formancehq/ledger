//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"

	ledgerv1alpha1 "github.com/formancehq/ledger-v3-poc/operator/api/v1alpha1"
)

func TestReconcile_AgentCreatesSecret(t *testing.T) {
	agent := newLedgerClusterAgent("creates-secret", []string{"read"}, map[string]string{"app": "ledger"})
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	// Wait for the Secret to appear.
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: agentSecretsNamespace,
		Name:      "creates-secret-agent-keys",
	}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, secretKey, secret) == nil
	}, "Secret should be created")

	// Verify secret data keys.
	assert.Contains(t, secret.Data, "seed.hex")
	assert.Contains(t, secret.Data, "pubkey.hex")
	assert.Contains(t, secret.Data, "key-id")

	// Verify non-empty values.
	assert.NotEmpty(t, secret.Data["seed.hex"])
	assert.NotEmpty(t, secret.Data["pubkey.hex"])
	assert.NotEmpty(t, secret.Data["key-id"])

	// Verify tracking annotation.
	assert.Equal(t, "creates-secret", secret.Annotations["ledger.formance.com/agent-name"])
}

func TestReconcile_AgentSecretIdempotent(t *testing.T) {
	agent := newLedgerClusterAgent("idempotent", []string{"read"}, map[string]string{"app": "ledger"})
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	// Wait for the Secret to appear and capture initial keys.
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: agentSecretsNamespace,
		Name:      "idempotent-agent-keys",
	}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, secretKey, secret) == nil
	}, "Secret should be created")

	initialSeed := string(secret.Data["seed.hex"])
	initialPubKey := string(secret.Data["pubkey.hex"])
	initialKeyID := string(secret.Data["key-id"])

	// Force a re-reconciliation by updating the agent spec.
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "idempotent"}, agent))
	agent.Spec.Scopes = []string{"read", "write"}
	require.NoError(t, k8sClient.Update(ctx, agent))

	// Wait for the status to reflect the updated generation.
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "idempotent"}, agent); err != nil {
			return false
		}
		return agent.Status.ObservedGeneration == agent.Generation
	}, "agent status should reflect updated generation")

	// Re-read the secret and verify keys haven't changed.
	require.NoError(t, k8sClient.Get(ctx, secretKey, secret))
	assert.Equal(t, initialSeed, string(secret.Data["seed.hex"]), "seed must not change on re-reconciliation")
	assert.Equal(t, initialPubKey, string(secret.Data["pubkey.hex"]), "pubkey must not change on re-reconciliation")
	assert.Equal(t, initialKeyID, string(secret.Data["key-id"]), "keyID must not change on re-reconciliation")
}

func TestReconcile_AgentStatus(t *testing.T) {
	agent := newLedgerClusterAgent("status-check", []string{"read"}, map[string]string{"app": "ledger"})
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	// Wait for status to be populated.
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "status-check"}, agent); err != nil {
			return false
		}
		return agent.Status.Phase == "Ready"
	}, "agent phase should be Ready")

	assert.NotEmpty(t, agent.Status.KeyID, "keyID must be set")
	assert.Equal(t, agentSecretsNamespace, agent.Status.SecretRef.Namespace)
	assert.Equal(t, "status-check-agent-keys", agent.Status.SecretRef.Name)
}

func TestReconcile_AgentMatchesServices(t *testing.T) {
	ns := createTestNamespace(t)

	// Create a LedgerService with matching labels.
	ls := newLedgerService("matched-svc", ns)
	ls.Labels = map[string]string{"app": "matched"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Create agent with matching selector.
	agent := newLedgerClusterAgent("matcher", []string{"read"}, map[string]string{"app": "matched"})
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	// Wait for status to list the matched service.
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "matcher"}, agent); err != nil {
			return false
		}
		for _, ms := range agent.Status.MatchedServices {
			if ms.Name == "matched-svc" && ms.Namespace == ns {
				return true
			}
		}
		return false
	}, "agent should match the LedgerService")
}

func TestReconcile_AgentDeletion(t *testing.T) {
	agent := newLedgerClusterAgent("to-delete", []string{"read"}, map[string]string{"app": "ledger"})
	require.NoError(t, k8sClient.Create(ctx, agent))

	// Wait for the Secret to appear.
	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: agentSecretsNamespace,
		Name:      "to-delete-agent-keys",
	}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, secretKey, secret) == nil
	}, "Secret should be created")

	// Delete the agent.
	require.NoError(t, k8sClient.Delete(ctx, agent))

	// Wait for the Secret to be cleaned up by the finalizer.
	requireEventually(t, func() bool {
		err := k8sClient.Get(ctx, secretKey, secret)
		return apierrors.IsNotFound(err)
	}, "Secret should be deleted after agent deletion")

	// Verify the agent itself is gone.
	requireEventually(t, func() bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: "to-delete"}, &ledgerv1alpha1.LedgerClusterAgent{})
		return apierrors.IsNotFound(err)
	}, "agent should be deleted")
}
