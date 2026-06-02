package controller

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formance/ledger/operator/api/v1alpha1"
)

func TestBuildCommand_AgentsWithAuthDisabled(t *testing.T) {
	t.Parallel()

	falseBool := false
	trueBool := true

	tests := []struct {
		name          string
		authEnabled   *bool
		agents        []agentKeyInfo
		expectEd25519 bool
	}{
		{
			name:          "agents present, auth not set (nil) → includes ed25519 flag",
			authEnabled:   nil,
			agents:        []agentKeyInfo{{KeyID: "k1", PublicKey: "deadbeef", ConfigMapPrefix: "agent", AgentName: "a1", Scopes: []string{"read"}}},
			expectEd25519: true,
		},
		{
			name:          "agents present, auth explicitly true → includes ed25519 flag",
			authEnabled:   &trueBool,
			agents:        []agentKeyInfo{{KeyID: "k1", PublicKey: "deadbeef", ConfigMapPrefix: "agent", AgentName: "a1", Scopes: []string{"read"}}},
			expectEd25519: true,
		},
		{
			name:          "agents present, auth explicitly false → no ed25519 flag",
			authEnabled:   &falseBool,
			agents:        []agentKeyInfo{{KeyID: "k1", PublicKey: "deadbeef", ConfigMapPrefix: "agent", AgentName: "a1", Scopes: []string{"read"}}},
			expectEd25519: false,
		},
		{
			name:          "no agents → no ed25519 flag",
			authEnabled:   nil,
			agents:        nil,
			expectEd25519: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			replicas := int32(1)
			ls := &ledgerv1alpha1.LedgerService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-svc",
					Namespace: "default",
				},
				Spec: ledgerv1alpha1.LedgerServiceSpec{
					Replicas: &replicas,
					Auth: &ledgerv1alpha1.AuthorizationConfig{
						Enabled: tt.authEnabled,
					},
				},
			}

			cmd := buildCommand(ls, tt.agents)
			script := strings.Join(cmd, " ")

			if tt.expectEd25519 {
				assert.Contains(t, script, "--auth-ed25519-keys")
			} else {
				assert.NotContains(t, script, "--auth-ed25519-keys")
			}
		})
	}
}
