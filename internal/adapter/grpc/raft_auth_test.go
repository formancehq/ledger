package grpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// startTestRaftServerWithSecret boots a Raft gRPC server in plaintext mode
// guarded by the given clusterSecret. We use plaintext (no TLS) to keep the
// test focused on the auth interceptor logic; in production the
// bootstrap.Config.Validate gate refuses --cluster-secret without TLS.
func startTestRaftServerWithSecret(t *testing.T, clusterSecret string) *serverTestSetup {
	t.Helper()

	port := freeTCPPort(t)

	srv, err := NewRaftServer(port, noopLogger{}, nil, true, clusterSecret)
	require.NoError(t, err)

	healthpb.RegisterHealthServer(srv.GetServer(), healthShim{})

	listening := make(chan struct{})

	go func() {
		_ = srv.Start(listening)
	}()

	select {
	case <-listening:
	case <-time.After(2 * time.Second):
		t.Fatal("server did not start listening")
	}

	t.Cleanup(func() { _ = srv.Stop() })

	return &serverTestSetup{port: port}
}

func dialPlaintextWithBearer(t *testing.T, port int, token string) *grpc.ClientConn {
	t.Helper()

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
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

	requireHealthOK(t, dialPlaintextWithBearer(t, setup.port, secret))
}

func TestRaftAuth_WrongTokenRejected(t *testing.T) {
	t.Parallel()

	const secret = "correct-horse-battery-staple"
	setup := startTestRaftServerWithSecret(t, secret)

	conn := dialPlaintextWithBearer(t, setup.port, "wrong-token")
	requireHealthUnauthenticated(t, conn)
}

func TestRaftAuth_MissingTokenRejected(t *testing.T) {
	t.Parallel()

	const secret = "correct-horse-battery-staple"
	setup := startTestRaftServerWithSecret(t, secret)

	// No bearer credentials at all — a network attacker who can reach the
	// raft port but does not know the secret.
	conn := dialPlaintext(t, setup.port)
	requireHealthUnauthenticated(t, conn)
}

func TestRaftAuth_NonBearerAuthorizationRejected(t *testing.T) {
	t.Parallel()

	const secret = "correct-horse-battery-staple"
	setup := startTestRaftServerWithSecret(t, secret)

	conn := dialPlaintext(t, setup.port)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Build an authorization header that is present but not Bearer-prefixed.
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Basic "+secret)

	_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	require.Error(t, err)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
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
