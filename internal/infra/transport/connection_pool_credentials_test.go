package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/credentials"
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

func TestConnectionPool_AuthTokenRejectsDisabledTLS(t *testing.T) {
	t.Parallel()
	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{AuthToken: "cluster-secret"})
	_, err := pool.decideTLS("peer.example:8889")
	require.EqualError(t, err, "peer authentication requires TLS for peer.example:8889")
}

func TestConnectionPool_OptionalTLSWithSecretSelectsTransport(t *testing.T) {
	t.Parallel()
	pool := NewConnectionPool(TLSPolicy{TLSConfig: &tls.Config{}}, PoolConfig{AuthToken: "cluster-secret"})
	pool.probeTLS = func(string, *tls.Config, time.Duration) error { return nil }
	useTLS, err := pool.decideTLS("peer.example:8889")
	require.NoError(t, err)
	require.True(t, useTLS)

	pool.probeTLS = func(string, *tls.Config, time.Duration) error {
		return &tlsHandshakeError{err: errors.New("legacy plaintext peer")}
	}
	useTLS, err = pool.decideTLS("peer.example:8889")
	require.NoError(t, err)
	require.False(t, useTLS)
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

	// A plaintext connection must never attach the static token.
	require.Equal(t, len(optsNoAuth), len(opts))
	tlsOpts := dialOptions(credentials.NewTLS(&tls.Config{}), cfg)
	require.Len(t, tlsOpts, len(optsNoAuth)+1)
}

func TestConnectionPool_RestartConnection_UnknownPeer(t *testing.T) {
	t.Parallel()

	pool := NewConnectionPool(TLSPolicy{}, PoolConfig{})

	err := pool.RestartConnection(999)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no connection for peer 999")
}
