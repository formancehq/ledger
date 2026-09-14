package transport

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestProbeTLS_AgainstTLSServer verifies that probing a peer that actually
// speaks TLS succeeds and is classified as not-a-handshake-error.
func TestProbeTLS_AgainstTLSServer(t *testing.T) {
	t.Parallel()

	srv := newTLSEchoServer(t)
	defer srv.Close()

	clientCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    srv.pool,
		ServerName: "localhost",
	}

	err := probeTLS(srv.addr(), clientCfg, time.Second)
	require.NoError(t, err)
}

// TestProbeTLS_AgainstPlaintextServer verifies that probing a peer that
// only speaks plaintext fails AND is classified as a TLS handshake error,
// so the pool can fall back to plaintext.
func TestProbeTLS_AgainstPlaintextServer(t *testing.T) {
	t.Parallel()

	srv := newPlaintextEchoServer(t)
	defer srv.Close()

	clientCfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    x509.NewCertPool(),
		ServerName: "localhost",
	}

	err := probeTLS(srv.addr(), clientCfg, time.Second)
	require.Error(t, err)
	require.True(t, isTLSHandshakeError(err), "expected TLS handshake error, got %v", err)
}

// TestProbeTLS_NetworkError verifies that a probe against a closed port
// returns a non-TLS error so the pool surfaces it (instead of silently
// falling back to plaintext on every cluster outage).
func TestProbeTLS_NetworkError(t *testing.T) {
	t.Parallel()

	// Bind and immediately release a port to get a definitely-closed addr.
	lis, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	addr := lis.Addr().String()
	require.NoError(t, lis.Close())

	clientCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "localhost"}

	err = probeTLS(addr, clientCfg, 500*time.Millisecond)
	require.Error(t, err)
	require.False(t, isTLSHandshakeError(err), "network error should not be classified as TLS handshake error")
}

// TestConnectionPool_OptionalProbeFallsBack verifies that adding a peer in
// optional mode against a plaintext-only server falls back to plaintext.
func TestConnectionPool_OptionalProbeFallsBack(t *testing.T) {
	t.Parallel()

	srv := newPlaintextEchoServer(t)
	defer srv.Close()

	clientCfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: "localhost", RootCAs: x509.NewCertPool()}

	pool := NewConnectionPool(TLSPolicy{TLSConfig: clientCfg, Strict: false}, PoolConfig{})
	pool.probeTimeout = 500 * time.Millisecond
	defer func() { _ = pool.Close() }()

	require.NoError(t, pool.AddPeer(1, srv.addr()))

	// The pool should have decided not to use TLS for this peer.
	pool.mu.Lock()
	entry := pool.peers[1]
	pool.mu.Unlock()
	require.NotNil(t, entry)
	require.False(t, entry.usingTLS, "expected plaintext fallback for non-TLS peer")
}

// TestConnectionPool_OptionalProbeKeepsTLS verifies that the pool picks TLS
// when the peer speaks it.
func TestConnectionPool_OptionalProbeKeepsTLS(t *testing.T) {
	t.Parallel()

	srv := newTLSEchoServer(t)
	defer srv.Close()

	clientCfg := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: srv.pool, ServerName: "localhost"}

	pool := NewConnectionPool(TLSPolicy{TLSConfig: clientCfg, Strict: false}, PoolConfig{})
	pool.probeTimeout = time.Second
	defer func() { _ = pool.Close() }()

	require.NoError(t, pool.AddPeer(1, srv.addr()))

	pool.mu.Lock()
	entry := pool.peers[1]
	pool.mu.Unlock()
	require.NotNil(t, entry)
	require.True(t, entry.usingTLS, "expected TLS for TLS-capable peer")
}

// TestConnectionPool_AddressReplacementRetriesCommittedTarget covers
// fail-then-success refreshes in optional TLS mode. A transient probe failure
// must retain the replacement address for both the Raft loop's explicit restart
// and a repeated same-address registration from service-pool wiring.
func TestConnectionPool_AddressReplacementRetriesCommittedTarget(t *testing.T) {
	retries := map[string]func(*ConnectionPool, string) error{
		"raft restart": func(pool *ConnectionPool, _ string) error {
			return pool.RestartConnection(1)
		},
		"service re-registration": func(pool *ConnectionPool, addr string) error {
			return pool.AddPeer(1, addr)
		},
	}

	for name, retry := range retries {
		t.Run(name, func(t *testing.T) {
			oldServer := newPlaintextEchoServer(t)
			defer oldServer.Close()

			clientCfg := &tls.Config{
				MinVersion: tls.VersionTLS12,
				RootCAs:    x509.NewCertPool(),
				ServerName: "localhost",
			}
			pool := NewConnectionPool(TLSPolicy{TLSConfig: clientCfg}, PoolConfig{})
			pool.probeTimeout = 500 * time.Millisecond
			defer func() { require.NoError(t, pool.Close()) }()

			require.NoError(t, pool.AddPeer(1, oldServer.addr()))

			reserved, err := net.Listen("tcp4", "127.0.0.1:0")
			require.NoError(t, err)
			newAddr := reserved.Addr().String()
			require.NoError(t, reserved.Close())

			require.Error(t, pool.AddPeer(1, newAddr), "first probe must observe the endpoint outage")
			require.Equal(t, newAddr, pool.GetPeerAddress(1),
				"the replacement target must survive the failed probe")

			recovered, err := net.Listen("tcp4", newAddr)
			require.NoError(t, err)
			t.Cleanup(func() { _ = recovered.Close() })
			go func() {
				for {
					conn, acceptErr := recovered.Accept()
					if acceptErr != nil {
						return
					}
					_ = conn.Close()
				}
			}()

			require.NoError(t, retry(pool, newAddr),
				"retry after endpoint recovery must dial the replacement target")
			require.Equal(t, newAddr, pool.GetPeerAddress(1))
			require.Len(t, pool.PeerIDs(), 1)
		})
	}
}

// TestConnectionPool_StrictTLSNoProbe verifies that strict mode does NOT
// run a probe (the dial would fail if it did against a plaintext peer).
func TestConnectionPool_StrictTLSNoProbe(t *testing.T) {
	t.Parallel()

	srv := newPlaintextEchoServer(t)
	defer srv.Close()

	clientCfg := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: x509.NewCertPool()}

	pool := NewConnectionPool(TLSPolicy{TLSConfig: clientCfg, Strict: true}, PoolConfig{})
	defer func() { _ = pool.Close() }()

	// AddPeer succeeds (gRPC is lazy) but the connection is configured for
	// TLS. We just assert the choice — the actual RPC would fail.
	require.NoError(t, pool.AddPeer(1, srv.addr()))

	pool.mu.Lock()
	entry := pool.peers[1]
	pool.mu.Unlock()
	require.True(t, entry.usingTLS)
}

// --- in-process test servers ---

type echoServer struct {
	listener net.Listener
	pool     *x509.CertPool

	mu     sync.Mutex
	closed bool
}

func (s *echoServer) addr() string { return s.listener.Addr().String() }

func (s *echoServer) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}

	s.closed = true
	_ = s.listener.Close()
}

// newPlaintextEchoServer returns a TCP server that accepts connections and
// reads/discards bytes. The probe should detect TLS handshake failure
// because the server returns no TLS record.
func newPlaintextEchoServer(t *testing.T) *echoServer {
	t.Helper()

	lis, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)

	srv := &echoServer{listener: lis}

	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}

			// Accept and immediately close — this triggers a TLS handshake
			// failure on a probing client.
			_ = conn.Close()
		}
	}()

	t.Cleanup(srv.Close)

	return srv
}

// newTLSEchoServer returns a TCP server that performs a TLS handshake then
// closes the connection. Probes against it should succeed.
func newTLSEchoServer(t *testing.T) *echoServer {
	t.Helper()

	cert, pool := generateSelfSignedCert(t)

	tlsCfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}

	rawLis, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)

	lis := tls.NewListener(rawLis, tlsCfg)
	srv := &echoServer{listener: lis, pool: pool}

	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}

			// Touch the connection to drive the handshake to completion
			// before closing.
			if tlsConn, ok := conn.(*tls.Conn); ok {
				_ = tlsConn.Handshake()
			}

			_ = conn.Close()
		}
	}()

	t.Cleanup(srv.Close)

	return srv
}

// generateSelfSignedCert returns a self-signed cert + key pair and a CertPool
// that trusts it.
func generateSelfSignedCert(t *testing.T) (tls.Certificate, *x509.CertPool) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"test"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}

	derBytes, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	keyDER, err := x509.MarshalECPrivateKey(key)
	require.NoError(t, err)

	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)

	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(certPEM)

	return cert, pool
}

// Sanity check: imports are used.
var (
	_ = errors.New
	_ = fmt.Sprintf
)
