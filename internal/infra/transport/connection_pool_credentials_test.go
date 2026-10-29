package transport

import (
	"context"
	"crypto/tls"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials/insecure"
)

func TestStaticTokenCredentials_GetRequestMetadata(t *testing.T) {
	t.Parallel()

	creds := staticTokenCredentials{token: "my-secret-token"}

	md, err := creds.GetRequestMetadata(context.Background())
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"authorization": "Bearer my-secret-token",
	}, md)
}

func TestStaticTokenCredentials_GetRequestMetadata_WithURI(t *testing.T) {
	t.Parallel()

	creds := staticTokenCredentials{token: "token123"}

	// URI arguments are ignored by this implementation
	md, err := creds.GetRequestMetadata(context.Background(), "https://example.com")
	require.NoError(t, err)
	require.Equal(t, "Bearer token123", md["authorization"])
}

func TestStaticTokenCredentials_SeparatesPeerAndCallerCredentials(t *testing.T) {
	t.Parallel()

	creds := staticTokenCredentials{token: "cluster-secret", header: PeerAuthMetadataKey}
	ctx := WithForwardedBearerToken(context.Background(), "user-jwt")

	md, err := creds.GetRequestMetadata(ctx)
	require.NoError(t, err)
	require.Equal(t, "Bearer cluster-secret", md[PeerAuthMetadataKey])
	require.Equal(t, "Bearer user-jwt", md["authorization"])
}

func TestStaticTokenCredentials_RequireTransportSecurity(t *testing.T) {
	t.Parallel()

	creds := staticTokenCredentials{token: "any-token"}

	require.True(t, creds.RequireTransportSecurity())
}

func TestConnectionPool_AuthTokenRejectsNonStrictTLS(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name   string
		policy TLSPolicy
	}{
		{name: "plaintext", policy: TLSPolicy{}},
		{name: "optional TLS", policy: TLSPolicy{TLSConfig: &tls.Config{}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pool := NewConnectionPool(tt.policy, PoolConfig{AuthToken: "cluster-secret"})
			_, err := pool.decideTLS("peer.example:8889")
			require.EqualError(t, err, "peer authentication requires strict TLS for peer.example:8889")
		})
	}
}

func TestBearerTokenDialOption(t *testing.T) {
	t.Parallel()

	// BearerTokenDialOption should return a non-nil DialOption
	opt := BearerTokenDialOption("my-token")
	require.NotNil(t, opt)
}

func TestDialOptions_WithAuthToken(t *testing.T) {
	t.Parallel()

	cfg := PoolConfig{AuthToken: "secret"}
	cfg.SetDefaults()

	opts := dialOptions(insecure.NewCredentials(), cfg)
	require.NotEmpty(t, opts)

	cfgNoAuth := PoolConfig{}
	cfgNoAuth.SetDefaults()
	optsNoAuth := dialOptions(insecure.NewCredentials(), cfgNoAuth)

	// Auth token adds one more option
	require.Greater(t, len(opts), len(optsNoAuth))
}

func TestConnectionPool_RestartConnection_UnknownPeer(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{})

	err := pool.RestartConnection(999)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no connection for peer 999")
}
