package bootstrap

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// marshalers covers every encoder the config might be rendered with. Redaction
// must hold for all of them (MarshalYAML and MarshalJSON are independent).
var marshalers = map[string]func(any) ([]byte, error){
	"yaml": yaml.Marshal,
	"json": json.Marshal,
}

// TestConfigRedactsSecrets is the regression guard for the secret-leak finding:
// no encoder used to render the config may emit raw secret values.
func TestConfigRedactsSecrets(t *testing.T) {
	t.Parallel()

	const (
		receiptKey    = "super-secret-receipt-signing-key"
		clusterSecret = "super-secret-cluster-secret-value"
		poolToken     = "super-secret-pool-auth-token"
	)

	cfg := Config{
		ClusterID:         "test-cluster",
		ReceiptSigningKey: receiptKey,
		ClusterSecret:     clusterSecret,
	}
	cfg.PoolConfig.AuthToken = poolToken

	for name, marshal := range marshalers {
		// Cover both value and pointer marshaling (startup prints *Config).
		for _, v := range []any{cfg, &cfg} {
			out, err := marshal(v)
			require.NoError(t, err, "%s marshal", name)
			rendered := string(out)

			require.NotContains(t, rendered, receiptKey, "%s: receipt signing key leaked", name)
			require.NotContains(t, rendered, clusterSecret, "%s: cluster secret leaked", name)
			require.NotContains(t, rendered, poolToken, "%s: pool auth token leaked", name)
			require.Equal(t, 3, strings.Count(rendered, RedactedSecretPlaceholder),
				"%s: all three secrets should be redacted", name)
		}
	}

	// The original value must be untouched (redaction happens only on the copy).
	require.Equal(t, receiptKey, cfg.ReceiptSigningKey)
	require.Equal(t, clusterSecret, cfg.ClusterSecret)
	require.Equal(t, poolToken, cfg.PoolConfig.AuthToken)
}

// TestConfigPreservesAbsentSecrets keeps empty secrets visibly empty (presence
// metadata) rather than redacting them, across all encoders.
func TestConfigPreservesAbsentSecrets(t *testing.T) {
	t.Parallel()

	cfg := Config{ClusterID: "test-cluster"}

	for name, marshal := range marshalers {
		out, err := marshal(cfg)
		require.NoError(t, err, "%s marshal", name)
		require.NotContains(t, string(out), RedactedSecretPlaceholder,
			"%s: unset secrets must not be redacted (should render empty)", name)
	}
}
