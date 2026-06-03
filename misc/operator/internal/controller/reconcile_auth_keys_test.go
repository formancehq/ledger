//go:build integration

package controller

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

func TestReconcile_AuthKeysConfigMap(t *testing.T) {
	ns := createTestNamespace(t)

	// Create LedgerService first so the agent has a namespace to distribute into.
	ls := newLedgerService("authcm-svc", ns)
	ls.Labels = map[string]string{"tier": "authcm"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Create agent with matching selector.
	agent := newLedgerClusterAgent("authcm-agent", []string{"read", "write"}, map[string]string{"tier": "authcm"})
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	// Wait for the agent to be ready (secret created in the service's namespace).
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "authcm-agent"}, agent); err != nil {
			return false
		}
		return agent.Status.Phase == "Ready" && len(agent.Status.DistributedSecretRefs) > 0
	}, "agent should be Ready with a distributed secret ref")

	// Wait for the auth-keys ConfigMap to appear.
	cm := &corev1.ConfigMap{}
	cmKey := types.NamespacedName{Namespace: ns, Name: "authcm-svc-auth-keys"}
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
	assert.Equal(t, "/auth-keys/agent-authcm-agent-pubkey.hex", authKeys.Keys[0].PublicKeyFile)
	assert.Equal(t, []string{"read", "write"}, authKeys.Keys[0].Scopes)

	// Verify the individual pubkey file exists.
	pubKeyData, ok := cm.Data["agent-authcm-agent-pubkey.hex"]
	assert.True(t, ok, "ConfigMap must contain the agent pubkey file")
	assert.NotEmpty(t, pubKeyData)
}

func TestReconcile_AuthKeysStatefulSet(t *testing.T) {
	ns := createTestNamespace(t)

	// Create LedgerService first so the agent has a namespace to distribute into.
	ls := newLedgerService("authsts-svc", ns)
	ls.Labels = map[string]string{"tier": "authsts"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Create agent with matching selector.
	agent := newLedgerClusterAgent("authsts-agent", []string{"read"}, map[string]string{"tier": "authsts"})
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	// Wait for agent to be ready.
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "authsts-agent"}, agent); err != nil {
			return false
		}
		return agent.Status.Phase == "Ready" && len(agent.Status.DistributedSecretRefs) > 0
	}, "agent should be Ready")

	// Wait for the StatefulSet to appear.
	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "authsts-svc", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")

	// Wait for the auth-keys volume to be present (may need a re-reconciliation).
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "authsts-svc", Namespace: ns}, sts); err != nil {
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

	// Verify the command contains --auth-ed25519-keys flag.
	command := strings.Join(container.Command, " ")
	assert.Contains(t, command, `--auth-ed25519-keys "/auth-keys/auth-keys.json"`)
}

func TestReconcile_NoAgentsNoConfigMap(t *testing.T) {
	ns := createTestNamespace(t)

	// Create LedgerService without matching agents.
	ls := newLedgerService("no-agents-svc", ns)
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Wait for StatefulSet to confirm reconciliation ran.
	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		return k8sClient.Get(ctx, types.NamespacedName{Name: "no-agents-svc", Namespace: ns}, sts) == nil
	}, "StatefulSet should be created")

	// Verify no auth-keys ConfigMap.
	cm := &corev1.ConfigMap{}
	err := k8sClient.Get(ctx, types.NamespacedName{Namespace: ns, Name: "no-agents-svc-auth-keys"}, cm)
	assert.True(t, apierrors.IsNotFound(err), "auth-keys ConfigMap should not exist when no agents match")

	// Verify no auth-keys volume in the StatefulSet.
	for _, v := range sts.Spec.Template.Spec.Volumes {
		assert.NotEqual(t, "auth-keys", v.Name, "auth-keys volume should not exist without agents")
	}

	// Verify the command does not contain --auth-ed25519-keys flag.
	container := sts.Spec.Template.Spec.Containers[0]
	command := strings.Join(container.Command, " ")
	assert.NotContains(t, command, "--auth-ed25519-keys")
}

func TestReconcile_AuthKeysHashAnnotation(t *testing.T) {
	ns := createTestNamespace(t)

	// Create LedgerService first so the agent has a namespace to distribute into.
	ls := newLedgerService("authhash-svc", ns)
	ls.Labels = map[string]string{"tier": "authhash"}
	require.NoError(t, k8sClient.Create(ctx, ls))

	// Create agent with matching selector.
	agent := newLedgerClusterAgent("authhash-agent", []string{"read"}, map[string]string{"tier": "authhash"})
	require.NoError(t, k8sClient.Create(ctx, agent))
	t.Cleanup(func() {
		_ = k8sClient.Delete(ctx, agent) //nolint:errcheck // best-effort cleanup
	})

	// Wait for agent to be ready.
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "authhash-agent"}, agent); err != nil {
			return false
		}
		return agent.Status.Phase == "Ready" && len(agent.Status.DistributedSecretRefs) > 0
	}, "agent should be Ready")

	// Wait for the StatefulSet with auth-keys hash annotation.
	sts := &appsv1.StatefulSet{}
	requireEventually(t, func() bool {
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: "authhash-svc", Namespace: ns}, sts); err != nil {
			return false
		}
		_, ok := sts.Spec.Template.Annotations[annotationAuthKeysHash]
		return ok
	}, "StatefulSet pod template should have auth-keys-hash annotation")

	hashValue := sts.Spec.Template.Annotations[annotationAuthKeysHash]
	assert.Len(t, hashValue, 64, "auth-keys-hash annotation should be a 64-char SHA-256 hex string")
}
