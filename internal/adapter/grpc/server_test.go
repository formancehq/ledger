package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/formancehq/ledger/v3/pkg/testserver"
)

// healthShim returns SERVING for any request so we can use the standard
// health pb to validate connectivity through the multi-server.
type healthShim struct {
	healthpb.UnimplementedHealthServer
}

func (healthShim) Check(_ context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

type serverTestSetup struct {
	port         int
	clientTLSCfg *tls.Config
}

func startTestRaftServer(t *testing.T, certs *testserver.TestCerts, allowTLS, acceptPlaintext bool) *serverTestSetup {
	t.Helper()

	port := freeTCPPort(t)

	var tlsCfg *tls.Config
	if allowTLS {
		cert, err := tls.LoadX509KeyPair(certs.ServerCertFile, certs.ServerKeyFile)
		require.NoError(t, err)

		tlsCfg = &tls.Config{
			Certificates: []tls.Certificate{cert},
			MinVersion:   tls.VersionTLS12,
			NextProtos:   []string{"h2"},
		}
	}

	srv, err := NewRaftServer(port, noopLogger{}, tlsCfg, acceptPlaintext)
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

	setup := &serverTestSetup{port: port}
	if allowTLS {
		setup.clientTLSCfg = clientTLSConfigTrustingCA(t, certs.CACertFile)
	}

	return setup
}

func TestMultiServer_TLSOnly(t *testing.T) {
	t.Parallel()

	certs := newTestCerts(t)
	setup := startTestRaftServer(t, certs, true, false)

	requireHealthOK(t, dialTLS(t, setup.port, setup.clientTLSCfg))
	requireHealthFails(t, dialPlaintext(t, setup.port))
}

func TestMultiServer_PlaintextOnly(t *testing.T) {
	t.Parallel()

	setup := startTestRaftServer(t, nil, false, true)

	requireHealthOK(t, dialPlaintext(t, setup.port))
}

func TestMultiServer_OptionalAcceptsBoth(t *testing.T) {
	t.Parallel()

	certs := newTestCerts(t)
	setup := startTestRaftServer(t, certs, true, true)

	// Both clients succeed against the same dual-listener server.
	requireHealthOK(t, dialTLS(t, setup.port, setup.clientTLSCfg))
	requireHealthOK(t, dialPlaintext(t, setup.port))
}

func TestMultiServer_RejectsEmptyMode(t *testing.T) {
	t.Parallel()

	_, err := NewRaftServer(0, noopLogger{}, nil, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "neither TLS nor plaintext enabled")
}

// --- helpers ---

func freeTCPPort(t *testing.T) int {
	t.Helper()

	lis, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)

	port := lis.Addr().(*net.TCPAddr).Port
	require.NoError(t, lis.Close())

	return port
}

func newTestCerts(t *testing.T) *testserver.TestCerts {
	t.Helper()
	certs, err := testserver.GenerateTestCerts(t.TempDir())
	require.NoError(t, err)

	return certs
}

func clientTLSConfigTrustingCA(t *testing.T, caFile string) *tls.Config {
	t.Helper()

	caPEM, err := os.ReadFile(caFile)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(caPEM))

	return &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
		ServerName: "localhost",
	}
}

func dialTLS(t *testing.T, port int, tlsCfg *tls.Config) *grpc.ClientConn {
	t.Helper()

	addr := fmt.Sprintf("localhost:%d", port)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func dialPlaintext(t *testing.T, port int) *grpc.ClientConn {
	t.Helper()

	addr := fmt.Sprintf("127.0.0.1:%d", port)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

func requireHealthOK(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	client := healthpb.NewHealthClient(conn)

	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err)
	require.Equal(t, healthpb.HealthCheckResponse_SERVING, resp.GetStatus())
}

func requireHealthFails(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	client := healthpb.NewHealthClient(conn)

	_, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	require.Error(t, err)
}
