package bootstrap

import (
	"testing"
	"time"

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
		TLSConfig:  TLSConfig{Mode: TLSModeDisabled},
		TransportConfig: node.TransportConfig{
			Reception: []int{10, 512, 512},
			Send:      []int{10, 512, 512},
		},
		SnapshotSyncConfig: SnapshotSyncConfig{
			SessionTTL:     5 * time.Minute,
			Parallelism:    4,
			RetryCount:     5,
			FileRetryCount: 3,
		},
		HealthConfig: HealthConfig{
			WALThreshold:        0.8,
			DataThreshold:       0.8,
			WALResumeThreshold:  0.75,
			DataResumeThreshold: 0.75,
		},
	}
}

func TestValidateClusterSecretRequiresTLS(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		clusterSecret string
		tlsMode       TLSMode
		wantErr       string
	}{
		{
			name:          "cluster secret with TLS required",
			clusterSecret: "my-secret",
			tlsMode:       TLSModeRequired,
			wantErr:       "",
		},
		{
			name:          "cluster secret with TLS optional",
			clusterSecret: "my-secret",
			tlsMode:       TLSModeOptional,
			wantErr:       "",
		},
		{
			name:          "cluster secret without TLS",
			clusterSecret: "my-secret",
			tlsMode:       TLSModeDisabled,
			wantErr:       "--cluster-secret requires TLS",
		},
		{
			name:          "no cluster secret without TLS",
			clusterSecret: "",
			tlsMode:       TLSModeDisabled,
			wantErr:       "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := validBaseConfig()
			cfg.ClusterSecret = tt.clusterSecret
			cfg.TLSConfig.Mode = tt.tlsMode
			if tt.tlsMode != TLSModeDisabled {
				// Provide cert/key so the TLS-config validation doesn't trip first.
				cfg.TLSConfig.CertFile = "/tmp/fake.crt"
				cfg.TLSConfig.KeyFile = "/tmp/fake.key"
			}

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

func TestValidateTLSConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		mode              TLSMode
		cert              string
		key               string
		ca                string
		requireClientCert bool
		wantErr           string
	}{
		{
			name:    "disabled requires nothing",
			mode:    TLSModeDisabled,
			wantErr: "",
		},
		{
			name:    "optional requires cert and key",
			mode:    TLSModeOptional,
			wantErr: "requires --tls-cert-file",
		},
		{
			name:    "optional with cert and key",
			mode:    TLSModeOptional,
			cert:    "/tmp/fake.crt",
			key:     "/tmp/fake.key",
			wantErr: "",
		},
		{
			name:    "required with cert and key",
			mode:    TLSModeRequired,
			cert:    "/tmp/fake.crt",
			key:     "/tmp/fake.key",
			wantErr: "",
		},
		{
			name:    "unknown mode rejected",
			mode:    TLSMode("invalid"),
			wantErr: "--tls-mode must be one of",
		},
		{
			name:              "require-client-cert without CA rejected",
			mode:              TLSModeRequired,
			cert:              "/tmp/fake.crt",
			key:               "/tmp/fake.key",
			requireClientCert: true,
			wantErr:           "--tls-require-client-cert requires --tls-ca-file",
		},
		{
			name:              "require-client-cert with CA allowed",
			mode:              TLSModeRequired,
			cert:              "/tmp/fake.crt",
			key:               "/tmp/fake.key",
			ca:                "/tmp/fake-ca.crt",
			requireClientCert: true,
			wantErr:           "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := validBaseConfig()
			cfg.TLSConfig.Mode = tt.mode
			cfg.TLSConfig.CertFile = tt.cert
			cfg.TLSConfig.KeyFile = tt.key
			cfg.TLSConfig.CAFile = tt.ca
			cfg.TLSConfig.RequireClientCert = tt.requireClientCert

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

func TestSnapshotSyncConfigValidate(t *testing.T) {
	t.Parallel()

	valid := SnapshotSyncConfig{
		SessionTTL:     5 * time.Minute,
		Parallelism:    4,
		RetryCount:     5,
		FileRetryCount: 3,
	}

	tests := []struct {
		name    string
		mutate  func(*SnapshotSyncConfig)
		wantErr string
	}{
		{
			name:   "valid defaults",
			mutate: func(c *SnapshotSyncConfig) {},
		},
		{
			name:    "parallelism=0 makes errgroup serial — operational failure mode",
			mutate:  func(c *SnapshotSyncConfig) { c.Parallelism = 0 },
			wantErr: "--snapshot-parallelism",
		},
		{
			name:    "parallelism=-1 makes errgroup unlimited — DoS by misconfig",
			mutate:  func(c *SnapshotSyncConfig) { c.Parallelism = -1 },
			wantErr: "--snapshot-parallelism",
		},
		{
			name:    "retry-count=0 makes the loop run zero attempts",
			mutate:  func(c *SnapshotSyncConfig) { c.RetryCount = 0 },
			wantErr: "--snapshot-retry-count",
		},
		{
			name:    "file-retry-count=0",
			mutate:  func(c *SnapshotSyncConfig) { c.FileRetryCount = 0 },
			wantErr: "--snapshot-file-retry-count",
		},
		{
			name:    "session-ttl=0 reaps the session immediately",
			mutate:  func(c *SnapshotSyncConfig) { c.SessionTTL = 0 },
			wantErr: "--snapshot-session-ttl",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := valid
			tt.mutate(&cfg)

			err := cfg.Validate()
			if tt.wantErr == "" {
				require.NoError(t, err)

				return
			}

			require.Error(t, err)
			require.Contains(t, err.Error(), tt.wantErr)
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
