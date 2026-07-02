//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestReconcile_AgentDistributesToAdditionalNamespace(t *testing.T) {
	ns := createTestNamespace(t)

	agent := newLedgerClusterAgentWithAdditional("creates-secret", []string{"read"}, map[string]string{"app": "ledger"}, ns)
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: ns,
		Name:      "ledger-creates-secret-agent-keys",
	}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, secretKey, secret) == nil
	}, "Secret should be created in the additional namespace")

	assert.Contains(t, secret.Data, "seed.hex")
	assert.Contains(t, secret.Data, "pubkey.hex")
	assert.Contains(t, secret.Data, "key-id")

	assert.NotEmpty(t, secret.Data["seed.hex"])
	assert.NotEmpty(t, secret.Data["pubkey.hex"])
	assert.NotEmpty(t, secret.Data["key-id"])

	assert.Equal(t, "creates-secret", secret.Labels[agentNameLabel])
}

func TestReconcile_AgentDistributesToMatchedServiceNamespaces(t *testing.T) {
	nsA := createTestNamespace(t)
	nsB := createTestNamespace(t)

	for _, ns := range []string{nsA, nsB} {
		ls := newCluster("matched-svc", ns)
		ls.Labels = map[string]string{"tier": "multi"}
		require.NoError(t, k8sClient.Create(ctx, ls))
	}

	agent := newLedgerClusterAgent("multi-distrib", []string{"read"}, map[string]string{"tier": "multi"})
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	secretA := &corev1.Secret{}
	secretB := &corev1.Secret{}
	keyA := types.NamespacedName{Namespace: nsA, Name: "ledger-multi-distrib-agent-keys"}
	keyB := types.NamespacedName{Namespace: nsB, Name: "ledger-multi-distrib-agent-keys"}

	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, keyA, secretA) == nil && k8sClient.Get(ctx, keyB, secretB) == nil
	}, "Secret should be present in both matched namespaces")

	assert.Equal(t, string(secretA.Data["seed.hex"]), string(secretB.Data["seed.hex"]), "replicas must share the same seed")
	assert.Equal(t, string(secretA.Data["pubkey.hex"]), string(secretB.Data["pubkey.hex"]))
	assert.Equal(t, string(secretA.Data["key-id"]), string(secretB.Data["key-id"]))
}

func TestReconcile_AgentSecretIdempotent(t *testing.T) {
	ns := createTestNamespace(t)

	agent := newLedgerClusterAgentWithAdditional("idempotent", []string{"read"}, map[string]string{"app": "ledger"}, ns)
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	secret := &corev1.Secret{}
	secretKey := types.NamespacedName{
		Namespace: ns,
		Name:      "ledger-idempotent-agent-keys",
	}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, secretKey, secret) == nil
	}, "Secret should be created")

	initialSeed := string(secret.Data["seed.hex"])
	initialPubKey := string(secret.Data["pubkey.hex"])
	initialKeyID := string(secret.Data["key-id"])

	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Name: "idempotent"}, agent))
	agent.Spec.Scopes = []string{"read", "write"}
	require.NoError(t, k8sClient.Update(ctx, agent))

	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "idempotent"}, agent); err != nil {
			return false
		}

		return agent.Status.ObservedGeneration == agent.Generation
	}, "agent status should reflect updated generation")

	require.NoError(t, k8sClient.Get(ctx, secretKey, secret))
	assert.Equal(t, initialSeed, string(secret.Data["seed.hex"]), "seed must not change on re-reconciliation")
	assert.Equal(t, initialPubKey, string(secret.Data["pubkey.hex"]), "pubkey must not change on re-reconciliation")
	assert.Equal(t, initialKeyID, string(secret.Data["key-id"]), "keyID must not change on re-reconciliation")
}

func TestReconcile_AgentStatus(t *testing.T) {
	ns := createTestNamespace(t)

	agent := newLedgerClusterAgentWithAdditional("status-check", []string{"read"}, map[string]string{"app": "ledger"}, ns)
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "status-check"}, agent); err != nil {
			return false
		}

		return agent.Status.Phase == "Ready" && len(agent.Status.DistributedSecretRefs) > 0
	}, "agent phase should be Ready with a distributed secret ref")

	assert.NotEmpty(t, agent.Status.KeyID, "keyID must be set")
	require.Len(t, agent.Status.DistributedSecretRefs, 1)
	assert.Equal(t, ns, agent.Status.DistributedSecretRefs[0].Namespace)
	assert.Equal(t, "ledger-status-check-agent-keys", agent.Status.DistributedSecretRefs[0].Name)
}

func TestReconcile_AgentNoTargets(t *testing.T) {
	agent := newLedgerClusterAgent("no-targets", []string{"read"}, map[string]string{"app": "nothing-matches-this"})
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "no-targets"}, agent); err != nil {
			return false
		}

		return agent.Status.Phase == "Pending" && agent.Status.ObservedGeneration == agent.Generation
	}, "agent with no targets should report Pending phase")

	assert.Empty(t, agent.Status.DistributedSecretRefs, "no replicas should be tracked when no targets exist")
	assert.Empty(t, agent.Status.KeyID, "key material should not be generated when no targets exist")

	var secrets corev1.SecretList
	require.NoError(t, k8sClient.List(ctx, &secrets, client.MatchingLabels{agentNameLabel: "no-targets"}))
	assert.Empty(t, secrets.Items, "no Secret should exist when there are no targets")
}

func TestReconcile_AgentMatchesServices(t *testing.T) {
	ns := createTestNamespace(t)

	ls := newCluster("matched-svc", ns)
	ls.Labels = map[string]string{"app": "matched"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	agent := newLedgerClusterAgent("matcher", []string{"read"}, map[string]string{"app": "matched"})
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

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
	}, "agent should match the Cluster")
}

func TestReconcile_AgentOrphanCleanup(t *testing.T) {
	nsA := createTestNamespace(t)
	nsB := createTestNamespace(t)

	for _, ns := range []string{nsA, nsB} {
		ls := newCluster("cleanup-svc", ns)
		ls.Labels = map[string]string{"tier": "cleanup"}
		require.NoError(t, k8sClient.Create(ctx, ls))
	}

	agent := newLedgerClusterAgent("orphan-cleanup", []string{"read"}, map[string]string{"tier": "cleanup"})
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	keyA := types.NamespacedName{Namespace: nsA, Name: "ledger-orphan-cleanup-agent-keys"}
	keyB := types.NamespacedName{Namespace: nsB, Name: "ledger-orphan-cleanup-agent-keys"}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, keyA, &corev1.Secret{}) == nil && k8sClient.Get(ctx, keyB, &corev1.Secret{}) == nil
	}, "both replicas should be created initially")

	// Remove the matching label from the service in nsB so it leaves the selector scope.
	lsB := &ledgerv1alpha1.Cluster{}
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: nsB, Name: "cleanup-svc"}, lsB))
	lsB.Labels = map[string]string{"tier": "elsewhere"}
	require.NoError(t, k8sClient.Update(ctx, lsB))

	// The replica in nsB must disappear; the one in nsA must remain.
	requireEventually(t, func() bool {
		err := k8sClient.Get(ctx, keyB, &corev1.Secret{})

		return apierrors.IsNotFound(err)
	}, "replica in unmatched namespace should be deleted")

	require.NoError(t, k8sClient.Get(ctx, keyA, &corev1.Secret{}), "matched-namespace replica must remain")
}

func TestReconcile_AgentDeletion(t *testing.T) {
	nsA := createTestNamespace(t)
	nsB := createTestNamespace(t)

	agent := newLedgerClusterAgentWithAdditional("to-delete", []string{"read"}, map[string]string{"app": "ledger"}, nsA, nsB)
	require.NoError(t, k8sClient.Create(ctx, agent))

	keyA := types.NamespacedName{Namespace: nsA, Name: "ledger-to-delete-agent-keys"}
	keyB := types.NamespacedName{Namespace: nsB, Name: "ledger-to-delete-agent-keys"}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, keyA, &corev1.Secret{}) == nil && k8sClient.Get(ctx, keyB, &corev1.Secret{}) == nil
	}, "replicas should be created in both additional namespaces")

	require.NoError(t, k8sClient.Delete(ctx, agent))

	requireEventually(t, func() bool {
		errA := k8sClient.Get(ctx, keyA, &corev1.Secret{})
		errB := k8sClient.Get(ctx, keyB, &corev1.Secret{})

		return apierrors.IsNotFound(errA) && apierrors.IsNotFound(errB)
	}, "all replicas should be deleted after agent deletion")

	requireEventually(t, func() bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Name: "to-delete"}, &ledgerv1alpha1.LedgerClusterAgent{})

		return apierrors.IsNotFound(err)
	}, "agent should be deleted")
}
