package bootstrap

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/infra/node"
	"github.com/stretchr/testify/require"
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
