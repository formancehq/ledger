//go:build integration

package controller

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestReconcile_AgentDistributesToAdditionalNamespace(t *testing.T) {
	ns := createTestNamespace(t)

	agent := newCredentialsWithAdditional("creates-secret", []string{"read"}, map[string]string{"app": "ledger"}, ns)
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

	agent := newCredentials("multi-distrib", []string{"read"}, map[string]string{"tier": "multi"})
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

	agent := newCredentialsWithAdditional("idempotent", []string{"read"}, map[string]string{"app": "ledger"}, ns)
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

	agent := newCredentialsWithAdditional("status-check", []string{"read"}, map[string]string{"app": "ledger"}, ns)
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
	agent := newCredentials("no-targets", []string{"read"}, map[string]string{"app": "nothing-matches-this"})
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

	agent := newCredentials("matcher", []string{"read"}, map[string]string{"app": "matched"})
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

	agent := newCredentials("orphan-cleanup", []string{"read"}, map[string]string{"tier": "cleanup"})
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

func TestReconcile_AgentSeedSurvivesClusterRecreation(t *testing.T) {
	ns := createTestNamespace(t)

	ls := newCluster("survive-svc", ns)
	ls.Labels = map[string]string{"tier": "survive"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	agent := newCredentials("survive-agent", []string{"read"}, map[string]string{"tier": "survive"})
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	replicaKey := types.NamespacedName{Namespace: ns, Name: "ledger-survive-agent-agent-keys"}
	canonicalKey := types.NamespacedName{Namespace: testOperatorNamespace, Name: "ledger-survive-agent-agent-canonical"}
	replica := &corev1.Secret{}
	canonical := &corev1.Secret{}

	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, replicaKey, replica) == nil && k8sClient.Get(ctx, canonicalKey, canonical) == nil
	}, "both canonical and replica secrets should be created")

	initialSeed := string(canonical.Data["seed.hex"])
	initialPubKey := string(canonical.Data["pubkey.hex"])
	initialKeyID := string(canonical.Data["key-id"])
	require.NotEmpty(t, initialSeed)
	assert.Equal(t, initialSeed, string(replica.Data["seed.hex"]), "replica must project canonical seed")

	require.NoError(t, k8sClient.Delete(ctx, ls))
	requireEventually(t, func() bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "survive-svc"}, &ledgerv1alpha1.Cluster{})

		return apierrors.IsNotFound(err)
	}, "Cluster should be deleted")

	// Replica must be aggressively GC'd; canonical must survive to preserve seed identity.
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "survive-agent"}, agent); err != nil {
			return false
		}

		return agent.Status.Phase == "Pending" && agent.Status.ObservedGeneration == agent.Generation
	}, "agent should report Pending once no service matches")

	requireEventually(t, func() bool {
		err := k8sClient.Get(ctx, replicaKey, &corev1.Secret{})

		return apierrors.IsNotFound(err)
	}, "replica in unmatched namespace should be deleted")

	require.NoError(t, k8sClient.Get(ctx, canonicalKey, canonical), "canonical seed must survive Cluster deletion")
	assert.Equal(t, initialSeed, string(canonical.Data["seed.hex"]), "canonical seed must not be regenerated")

	lsAgain := newCluster("survive-svc", ns)
	lsAgain.Labels = map[string]string{"tier": "survive"}
	require.NoError(t, k8sClient.Create(ctx, lsAgain))

	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "survive-agent"}, agent); err != nil {
			return false
		}

		return agent.Status.Phase == "Ready" && agent.Status.KeyID == initialKeyID
	}, "agent should return to Ready with the original keyID")

	require.NoError(t, k8sClient.Get(ctx, replicaKey, replica))
	assert.Equal(t, initialSeed, string(replica.Data["seed.hex"]), "seed must be identical after Cluster recreation")
	assert.Equal(t, initialPubKey, string(replica.Data["pubkey.hex"]))
	assert.Equal(t, initialKeyID, string(replica.Data["key-id"]))
}

func TestReconcile_AgentUpgradeAdoptsLegacyReplicaSeed(t *testing.T) {
	ns := createTestNamespace(t)

	// Simulate a replica Secret produced by a pre-canonical version of the
	// operator: it carries only the legacy agentNameLabel and holds seed
	// material at the same well-known key set. The agent name matches what
	// the reconciler will look up.
	legacyReplica := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ledger-legacy-agent-agent-keys",
			Namespace: ns,
			Labels: map[string]string{
				agentNameLabel: "legacy-agent",
			},
		},
		Data: map[string][]byte{
			"seed.hex":   []byte("aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"),
			"pubkey.hex": []byte("11223344556677889900aabbccddeeff11223344556677889900aabbccddeeff"),
			"key-id":     []byte("legacyseed12345"),
		},
	}
	require.NoError(t, k8sClient.Create(ctx, legacyReplica))

	ls := newCluster("legacy-svc", ns)
	ls.Labels = map[string]string{"tier": "legacy"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	agent := newCredentials("legacy-agent", []string{"read"}, map[string]string{"tier": "legacy"})
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	canonicalKey := types.NamespacedName{Namespace: testOperatorNamespace, Name: "ledger-legacy-agent-agent-canonical"}
	canonical := &corev1.Secret{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, canonicalKey, canonical); err != nil {
			return false
		}

		return len(canonical.Data["seed.hex"]) > 0
	}, "canonical secret should be created and seeded from the legacy replica")

	assert.Equal(t, string(legacyReplica.Data["seed.hex"]), string(canonical.Data["seed.hex"]), "canonical must adopt the legacy replica seed")
	assert.Equal(t, string(legacyReplica.Data["pubkey.hex"]), string(canonical.Data["pubkey.hex"]))
	assert.Equal(t, string(legacyReplica.Data["key-id"]), string(canonical.Data["key-id"]))

	// The legacy replica must keep the same seed content (no rotation).
	require.NoError(t, k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: legacyReplica.Name}, legacyReplica))
	assert.Equal(t, string(canonical.Data["seed.hex"]), string(legacyReplica.Data["seed.hex"]), "legacy replica seed must not be rotated on upgrade")
}

func TestReconcile_AgentUpgradeAdoptsLegacySeedWithoutTargets(t *testing.T) {
	ns := createTestNamespace(t)

	// Simulate an upgrade scenario where the old operator was stopped, the
	// Cluster was deleted while it was down, and now we upgrade. The
	// only survivor is a legacy replica Secret sitting alone in a namespace
	// no longer referenced by any Cluster.
	legacyReplica := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ledger-orphan-agent-agent-keys",
			Namespace: ns,
			Labels: map[string]string{
				agentNameLabel: "orphan-agent",
			},
		},
		Data: map[string][]byte{
			"seed.hex":   []byte("cafebabecafebabecafebabecafebabecafebabecafebabecafebabecafebabe"),
			"pubkey.hex": []byte("f00dfeedf00dfeedf00dfeedf00dfeedf00dfeedf00dfeedf00dfeedf00dfeed"),
			"key-id":     []byte("orphanseedxxxxx"),
		},
	}
	require.NoError(t, k8sClient.Create(ctx, legacyReplica))

	agent := newCredentials("orphan-agent", []string{"read"}, map[string]string{"tier": "never-matches"})
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	canonicalKey := types.NamespacedName{Namespace: testOperatorNamespace, Name: "ledger-orphan-agent-agent-canonical"}
	canonical := &corev1.Secret{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, canonicalKey, canonical); err != nil {
			return false
		}

		return len(canonical.Data["seed.hex"]) > 0
	}, "canonical must be bootstrapped from the orphan legacy replica even with no matching services")

	assert.Equal(t, string(legacyReplica.Data["seed.hex"]), string(canonical.Data["seed.hex"]), "canonical must adopt the legacy seed on no-target upgrade")
	assert.Equal(t, string(legacyReplica.Data["key-id"]), string(canonical.Data["key-id"]))

	// The orphan legacy replica is then aggressively GC'd because no target
	// covers its namespace — but the seed identity is preserved on the canonical.
	requireEventually(t, func() bool {
		err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: legacyReplica.Name}, &corev1.Secret{})

		return apierrors.IsNotFound(err)
	}, "orphan legacy replica should be GC'd after its seed is adopted")
}

func TestReconcile_AgentCanonicalDeletedOnAgentRemoval(t *testing.T) {
	ns := createTestNamespace(t)

	agent := newCredentialsWithAdditional("canonical-cleanup", []string{"read"}, map[string]string{"app": "ledger"}, ns)
	require.NoError(t, k8sClient.Create(ctx, agent))

	replicaKey := types.NamespacedName{Namespace: ns, Name: "ledger-canonical-cleanup-agent-keys"}
	canonicalKey := types.NamespacedName{Namespace: testOperatorNamespace, Name: "ledger-canonical-cleanup-agent-canonical"}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, replicaKey, &corev1.Secret{}) == nil && k8sClient.Get(ctx, canonicalKey, &corev1.Secret{}) == nil
	}, "canonical and replica should be created")

	require.NoError(t, k8sClient.Delete(ctx, agent))

	requireEventually(t, func() bool {
		errReplica := k8sClient.Get(ctx, replicaKey, &corev1.Secret{})
		errCanonical := k8sClient.Get(ctx, canonicalKey, &corev1.Secret{})

		return apierrors.IsNotFound(errReplica) && apierrors.IsNotFound(errCanonical)
	}, "canonical and replica should both be deleted after agent deletion")
}

func TestReconcile_AgentDeletion(t *testing.T) {
	nsA := createTestNamespace(t)
	nsB := createTestNamespace(t)

	agent := newCredentialsWithAdditional("to-delete", []string{"read"}, map[string]string{"app": "ledger"}, nsA, nsB)
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
		err := k8sClient.Get(ctx, types.NamespacedName{Name: "to-delete"}, &ledgerv1alpha1.Credentials{})

		return apierrors.IsNotFound(err)
	}, "agent should be deleted")
}
