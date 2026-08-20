package grpc

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"strconv"
	"syscall"
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

	srv, err := NewRaftServer(port, noopLogger{}, tlsCfg, acceptPlaintext, "")
	require.NoError(t, err)

	healthpb.RegisterHealthServer(srv.GetServer(), healthShim{})

	require.NoError(t, srv.Listen())

	go func() {
		_ = srv.Serve()
	}()

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

	_, err := NewRaftServer(0, noopLogger{}, nil, false, "")
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

func TestListenReturnsErrorWhenPortIsBusy(t *testing.T) {
	t.Parallel()

	// Hold the port the server will try to bind.
	blocker, err := net.Listen("tcp4", "0.0.0.0:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = blocker.Close() })

	port := blocker.Addr().(*net.TCPAddr).Port

	srv, err := NewRaftServer(port, noopLogger{}, nil, true, "")
	require.NoError(t, err)

	// EN-1784: this used to be reachable only from inside the serving
	// goroutine, where the error became panic(err) and killed the process.
	// Listen must hand the error back to the caller instead.
	require.NotPanics(t, func() {
		err = srv.Listen()
	})
	require.Error(t, err)
	require.ErrorIs(t, err, syscall.EADDRINUSE)
	require.Contains(t, err.Error(), strconv.Itoa(port),
		"the error must name the port that failed to bind")
}

// TestServeAfterStopIsNotAnError pins the shutdown ordering introduced by the
// Listen/Serve split. Serve re-reads s.listener and closeListener sets it to
// nil, so a startup that fails in a LATER fx hook can run Stop before the
// serve goroutine is ever scheduled. Nothing guarantees a new goroutine runs
// before the goroutine that spawned it continues, so this needs no unusual
// timing. It is a normal shutdown and must not be reported as an error.
func TestServeAfterStopIsNotAnError(t *testing.T) {
	t.Parallel()

	srv, err := NewRaftServer(freeTCPPort(t), noopLogger{}, nil, true, "")
	require.NoError(t, err)

	require.NoError(t, srv.Listen())
	require.NoError(t, srv.Stop())

	// Stands in for the serve goroutine being scheduled only after Stop.
	require.NoError(t, srv.Serve())
}

// TestServeWithoutListenFailsLoudly keeps the genuine contract violation loud:
// a Serve with no preceding Listen is a programming error, and an unreachable-
// by-contract branch must surface it rather than silently returning nil.
func TestServeWithoutListenFailsLoudly(t *testing.T) {
	t.Parallel()

	srv, err := NewRaftServer(freeTCPPort(t), noopLogger{}, nil, true, "")
	require.NoError(t, err)

	err = srv.Serve()
	require.Error(t, err)
	require.Contains(t, err.Error(), "before a successful Listen")
}

// TestListenAdoptsAnInjectedListener pins the seam the e2e fixtures rely on: a
// test binds the port itself and keeps the socket, so nothing can take the port
// between the moment the test learns the number and the moment the node serves
// it (EN-1784). The configured port is deliberately held by another socket here:
// Listen must not touch it.
func TestListenAdoptsAnInjectedListener(t *testing.T) {
	t.Parallel()

	injected, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)

	blocker, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = blocker.Close() })

	configuredPort := blocker.Addr().(*net.TCPAddr).Port

	srv, err := NewRaftServer(configuredPort, noopLogger{}, nil, true, "", WithListener(injected))
	require.NoError(t, err)

	healthpb.RegisterHealthServer(srv.GetServer(), healthShim{})

	require.NoError(t, srv.Listen(), "an adopted listener must not be re-bound")

	go func() {
		_ = srv.Serve()
	}()

	t.Cleanup(func() { _ = srv.Stop() })

	conn, err := grpc.NewClient(injected.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	require.NoError(t, err, "the server must serve the adopted listener")
}

// TestStopClosesAnInjectedListener pins the ownership rule: once a listener
// reaches the server, the lifecycle closes it. A test fixture that had to
// remember which listeners the application consumed would leak the ones it
// forgot, and a node that restarts must find its port free.
func TestStopClosesAnInjectedListener(t *testing.T) {
	t.Parallel()

	injected, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)

	addr := injected.Addr().String()
	port := injected.Addr().(*net.TCPAddr).Port

	srv, err := NewRaftServer(port, noopLogger{}, nil, true, "", WithListener(injected))
	require.NoError(t, err)

	require.NoError(t, srv.Listen())
	require.NoError(t, srv.Stop())

	rebound, err := net.Listen("tcp4", addr)
	require.NoError(t, err, "Stop must release the adopted listener")
	require.NoError(t, rebound.Close())
}
