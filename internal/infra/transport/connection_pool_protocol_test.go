package transport

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectionPoolRejectsBearerTokenOnPlaintext(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{AuthToken: "test-cluster-secret"})
	t.Cleanup(func() { require.NoError(t, pool.Close()) })
	err := pool.AddPeer(1, "127.0.0.1:1")
	require.EqualError(t, err, "peer authentication requires strict TLS for 127.0.0.1:1")
}
