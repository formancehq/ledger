package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// startTestRaftServerWithSecret boots a dual-listener Raft server to exercise
// both the authenticated TLS path and the transitional plaintext path.
func startTestRaftServerWithSecret(t *testing.T, clusterSecret string) *serverTestSetup {
	t.Helper()

	port := freeTCPPort(t)
	certs := newTestCerts(t)
	cert, err := tls.LoadX509KeyPair(certs.ServerCertFile, certs.ServerKeyFile)
	require.NoError(t, err)
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12, NextProtos: []string{"h2"}}
	srv, err := NewRaftServer(port, noopLogger{}, tlsCfg, true, clusterSecret)
	require.NoError(t, err)

	healthpb.RegisterHealthServer(srv.GetServer(), healthShim{})

	require.NoError(t, srv.Listen())

	go func() {
		_ = srv.Serve()
	}()

	t.Cleanup(func() { _ = srv.Stop() })

	return &serverTestSetup{port: port, clientTLSCfg: clientTLSConfigTrustingCA(t, certs.CACertFile)}
}

func dialTLSWithBearer(t *testing.T, setup *serverTestSetup, token string) *grpc.ClientConn {
	t.Helper()

	addr := fmt.Sprintf("localhost:%d", setup.port)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(credentials.NewTLS(setup.clientTLSCfg)),
	}

	if token != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(testBearer{token: token}))
	}

	conn, err := grpc.NewClient(addr, opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

type testBearer struct {
	token string
}

func (b testBearer) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + b.token}, nil
}

func (b testBearer) RequireTransportSecurity() bool { return false }

func TestRaftAuth_EmptySecretSkipsCheck(t *testing.T) {
	t.Parallel()

	// clusterSecret == "" mirrors the historical default: the interceptor is
	// not installed and no token is required.
	setup := startTestRaftServerWithSecret(t, "")
	requireHealthOK(t, dialPlaintext(t, setup.port))
}

func TestRaftAuth_CorrectTokenAccepted(t *testing.T) {
	t.Parallel()

	const secret = "correct-horse-battery-staple"
	setup := startTestRaftServerWithSecret(t, secret)

	requireHealthOK(t, dialTLSWithBearer(t, setup, secret))
}

func TestRaftAuth_WrongTokenRejected(t *testing.T) {
	t.Parallel()

	const secret = "correct-horse-battery-staple"
	setup := startTestRaftServerWithSecret(t, secret)

	conn := dialTLSWithBearer(t, setup, "wrong-token")
	requireHealthUnauthenticated(t, conn)
}

func TestRaftAuth_MissingTokenRejected(t *testing.T) {
	t.Parallel()

	const secret = "correct-horse-battery-staple"
	setup := startTestRaftServerWithSecret(t, secret)

	// No bearer credentials at all — a network attacker who can reach the
	// raft port but does not know the secret.
	conn := dialTLSWithBearer(t, setup, "")
	requireHealthUnauthenticated(t, conn)
}

func TestRaftAuth_NonBearerAuthorizationRejected(t *testing.T) {
	t.Parallel()

	const secret = "correct-horse-battery-staple"
	setup := startTestRaftServerWithSecret(t, secret)

	conn := dialTLSWithBearer(t, setup, "")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Build an authorization header that is present but not Bearer-prefixed.
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Basic "+secret)

	_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	require.Error(t, err)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestRaftAuth_OptionalPlaintextPeerAllowedWithoutToken(t *testing.T) {
	t.Parallel()
	setup := startTestRaftServerWithSecret(t, "cluster-secret")
	requireHealthOK(t, dialPlaintext(t, setup.port))
}

func requireHealthUnauthenticated(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	require.Error(t, err)
	require.Equal(t, codes.Unauthenticated, status.Code(err),
		"Raft RPC without a valid cluster-secret bearer token must be rejected (#310)")
}
