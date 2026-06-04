package bootstrap

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/node"
)

func TestServiceAdvertiseAddr_WithPort(t *testing.T) {
	t.Parallel()

	cfg := Config{
		RaftConfig: node.NodeConfig{AdvertiseAddr: "10.0.0.1:9000"},
		GRPCPort:   5050,
	}

	require.Equal(t, "10.0.0.1:5050", cfg.ServiceAdvertiseAddr())
}

func TestServiceAdvertiseAddr_WithoutPort(t *testing.T) {
	t.Parallel()

	cfg := Config{
		RaftConfig: node.NodeConfig{AdvertiseAddr: "myhost"},
		GRPCPort:   8080,
	}

	require.Equal(t, "myhost:8080", cfg.ServiceAdvertiseAddr())
}

// validBaseConfig returns a Config with required fields set so that
// Validate() only fails on the aspect under test.
func validBaseConfig() Config {
	return Config{
		ClusterID:  "test-cluster",
		RaftConfig: node.NodeConfig{NodeID: 1},
	}
}

func TestValidateClusterSecretRequiresTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		clusterSecret string
		tlsEnabled    bool
		wantErr       string
	}{
		{
			name:          "cluster secret with TLS enabled",
			clusterSecret: "my-secret",
			tlsEnabled:    true,
			wantErr:       "",
		},
		{
			name:          "cluster secret without TLS",
			clusterSecret: "my-secret",
			tlsEnabled:    false,
			wantErr:       "--tls-cert-file",
		},
		{
			name:          "no cluster secret without TLS",
			clusterSecret: "",
			tlsEnabled:    false,
			wantErr:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := validBaseConfig()
			cfg.ClusterSecret = tt.clusterSecret
			cfg.TLSConfig.Enabled = tt.tlsEnabled

			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidateAuthConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		auth    AuthFlagConfig
		wantErr string
	}{
		{
			name:    "auth enabled with issuer",
			auth:    AuthFlagConfig{Enabled: true, Issuer: "https://issuer.example.com"},
			wantErr: "",
		},
		{
			name:    "auth enabled with ed25519 keys file",
			auth:    AuthFlagConfig{Enabled: true, Ed25519KeysFile: "/path/to/keys"},
			wantErr: "",
		},
		{
			name:    "auth enabled with both issuer and ed25519",
			auth:    AuthFlagConfig{Enabled: true, Issuer: "https://issuer.example.com", Ed25519KeysFile: "/path/to/keys"},
			wantErr: "",
		},
		{
			name:    "auth enabled without credentials",
			auth:    AuthFlagConfig{Enabled: true},
			wantErr: "--auth-enabled requires",
		},
		{
			name:    "auth disabled no flags",
			auth:    AuthFlagConfig{},
			wantErr: "",
		},
		{
			name:    "auth disabled with issuer set",
			auth:    AuthFlagConfig{Enabled: false, Issuer: "https://issuer.example.com"},
			wantErr: "--auth-issuer",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := validBaseConfig()
			cfg.AuthConfig = tt.auth

			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestConfigMismatchError_Error(t *testing.T) {
	t.Parallel()

	err := &ConfigMismatchError{
		Field:     "node-id",
		Persisted: "1",
		Current:   "2",
	}

	msg := err.Error()
	require.Contains(t, msg, "node-id")
	require.Contains(t, msg, "persisted=1")
	require.Contains(t, msg, "current=2")
	require.Contains(t, msg, "--unsafe-skip-config-validation")
}
