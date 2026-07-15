package controller

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ledgerv1alpha1 "github.com/formancehq/ledger/misc/operator/api/v1alpha1"
)

func TestBuildCommand_KeepsOnlyPodIndexLogic(t *testing.T) {
	t.Parallel()

	ls := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			DataDir: "/data/app",
		},
	}

	cmd := buildCommand(ls)
	script := strings.Join(cmd, " ")

	// The shell entrypoint must contain only the POD_INDEX-derived bits:
	// NODE_ID arithmetic and the cluster-startup flag selection.
	assert.Contains(t, script, "NODE_ID=$((POD_INDEX + 1))")
	assert.Contains(t, script, `--node-id $NODE_ID`)
	assert.Contains(t, script, `$CLUSTER_FLAG`)

	// All other flags must have moved to env vars — none should appear in
	// the script anymore.
	forbidden := []string{
		"--advertise-addr",
		"--response-signing-key",
		"--cluster-secret",
		"--auth-ed25519-keys",
		"--learner-promotion-threshold",
		"--tls-mode",
		"--tls-cert-file",
		"--tls-key-file",
		"--tls-ca-cert-file",
		"OTEL_RESOURCE_ATTRIBUTES",
		"RAFT_PORT",
		"ADVERTISE_ADDR=",
	}
	for _, f := range forbidden {
		assert.NotContainsf(t, script, f, "script should not contain %q (moved to env var)", f)
	}
}

func TestBuildCommand_RestoreFlag(t *testing.T) {
	t.Parallel()

	ls := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			Restore: true,
			DataDir: "/data/app",
		},
	}

	script := strings.Join(buildCommand(ls), " ")
	assert.Contains(t, script, `CLUSTER_FLAG="--restore"`)
	assert.NotContains(t, script, "--bootstrap")
	assert.NotContains(t, script, "--join")
}

func TestBuildCommand_BootstrapVsJoin(t *testing.T) {
	t.Parallel()

	ls := &ledgerv1alpha1.Cluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test-cluster", Namespace: "default"},
		Spec: ledgerv1alpha1.ClusterSpec{
			DataDir: "/data/app",
			WalDir:  "/data/raft",
		},
	}

	script := strings.Join(buildCommand(ls), " ")
	// Restart detection comes first: any pod that owns the CLUSTER_JOINED
	// marker restarts from its persisted ConfState with no flag (neither
	// --bootstrap nor --join), regardless of POD_INDEX. Otherwise pod-0
	// bootstraps and non-zero pods join pod-0 via the RaftServer port
	// (extracted from BindAddr), not the external gRPC service port — see
	// ClusterBootstrapService.
	assert.Contains(t, script, `if [ -f "/data/raft/CLUSTER_JOINED" ]`)
	assert.Contains(t, script, `elif [ "$POD_INDEX" = "0" ]; then`)
	assert.Contains(t, script, `CLUSTER_FLAG="--bootstrap"`)
	assert.Contains(t, script, `CLUSTER_FLAG="--join ledger-test-cluster-0.ledger-test-cluster-headless.`)
	assert.Contains(t, script, `:7777"`)
	assert.NotContains(t, script, `:${GRPC_PORT}"`)

	// The restart-from-marker branch must run BEFORE the POD_INDEX
	// dispatch: passing --join on a restart blocks indefinitely on
	// GetPeers when peer pods are themselves Candidates without a
	// leader (cold-start deadlock — EN-1328). CLUSTER_JOINED is the
	// right signal because it is only written by the server AFTER the
	// cluster has accepted this node (initial snapshot persist for
	// pod-0, JoinAsLearner success for the others); a snapshot-file
	// presence check would mis-fire during the pre-registration window
	// between snapshot write and learner acceptance, orphaning the pod.
	markerBranch := strings.Index(script, `if [ -f "/data/raft/CLUSTER_JOINED" ]`)
	podIndexBranch := strings.Index(script, `elif [ "$POD_INDEX" = "0" ]`)
	assert.Less(t, markerBranch, podIndexBranch,
		"CLUSTER_JOINED marker check must precede POD_INDEX dispatch")
}

func TestBuildEnvVars_AuthEd25519Keys(t *testing.T) {
	t.Parallel()

	falseBool := false
	trueBool := true
	credentials := credentialsKeyInfo{KeyID: "k1", PublicKey: "deadbeef", ConfigMapPrefix: "credentials", CredentialsName: "a1", Scopes: []string{"read"}}

	tests := []struct {
		name        string
		authEnabled *bool
		credentials []credentialsKeyInfo
		wantEnv     bool
	}{
		{
			name:        "credentials present, auth nil → env var set",
			authEnabled: nil,
			credentials: []credentialsKeyInfo{credentials},
			wantEnv:     true,
		},
		{
			name:        "credentials present, auth explicitly true → env var set",
			authEnabled: &trueBool,
			credentials: []credentialsKeyInfo{credentials},
			wantEnv:     true,
		},
		{
			name:        "credentials present, auth explicitly false → env var absent",
			authEnabled: &falseBool,
			credentials: []credentialsKeyInfo{credentials},
			wantEnv:     false,
		},
		{
			name:        "no credentials → env var absent",
			credentials: nil,
			wantEnv:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ls := newMinimalCluster()
			if tt.authEnabled != nil {
				ls.Spec.Auth = &ledgerv1alpha1.AuthorizationConfig{Enabled: tt.authEnabled}
			}
			envs := buildEnvVars(ls, "disabled", tt.credentials)
			if tt.wantEnv {
				assertEnv(t, envs, "AUTH_ED25519_KEYS", "/auth-keys/auth-keys.json")
			} else {
				assertNoEnv(t, envs, "AUTH_ED25519_KEYS")
			}
		})
	}
}

func TestBuildEnvVars_AdvertiseAddr(t *testing.T) {
	t.Parallel()

	ls := newMinimalCluster()
	envs := buildEnvVars(ls, "disabled", nil)
	// $(POD_NAME) / $(POD_NAMESPACE) are resolved by the kubelet — the
	// operator just emits the template. The port is the Raft port (BindAddr),
	// NOT the service gRPC port — see TestBuildEnvVars_AdvertiseAddr_UsesRaftPort.
	assertEnv(t, envs, "ADVERTISE_ADDR", "$(POD_NAME).ledger-test-headless.$(POD_NAMESPACE).svc.cluster.local:7777")
}

func TestBuildEnvVars_OtelResourceAttributes(t *testing.T) {
	t.Parallel()

	t.Run("operator attrs only when monitoring nil", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "OTEL_RESOURCE_ATTRIBUTES", "service.cluster=test,service.node_id=$(POD_NAME)")
	})

	t.Run("user attrs prepended when set", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Monitoring = &ledgerv1alpha1.MonitoringConfig{Attributes: "env=prod,region=us"}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "OTEL_RESOURCE_ATTRIBUTES", "env=prod,region=us,service.cluster=test,service.node_id=$(POD_NAME)")
	})

	t.Run("operator attrs only when user attrs empty", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Monitoring = &ledgerv1alpha1.MonitoringConfig{}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "OTEL_RESOURCE_ATTRIBUTES", "service.cluster=test,service.node_id=$(POD_NAME)")
	})
}

func TestBuildEnvVars_LearnerPromotionThreshold(t *testing.T) {
	t.Parallel()

	t.Run("set", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		v := int32(42)
		ls.Spec.Raft = &ledgerv1alpha1.RaftConfig{LearnerPromotionThreshold: &v}
		envs := buildEnvVars(ls, "disabled", nil)
		assertEnv(t, envs, "LEARNER_PROMOTION_THRESHOLD", "42")
	})

	t.Run("nil omitted", func(t *testing.T) {
		t.Parallel()
		ls := newMinimalCluster()
		ls.Spec.Raft = &ledgerv1alpha1.RaftConfig{}
		envs := buildEnvVars(ls, "disabled", nil)
		assertNoEnv(t, envs, "LEARNER_PROMOTION_THRESHOLD")
	})
}
