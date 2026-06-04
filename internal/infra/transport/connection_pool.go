package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/lifecycle"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding/gzip"
)

const (
	GRPCInitialWindowSize     = 16 << 20 // 16 MB
	GRPCInitialConnWindowSize = 64 << 20 // 64 MB
	GRPCReadBufferSize        = 1 << 20  // 1 MB
	GRPCWriteBufferSize       = 1 << 20  // 1 MB
	GRPCMaxMsgSize            = 64 << 20 // 64 MB (large snapshots use chunked streaming)
)

// defaultProbeTimeout caps the TLS handshake probe used in optional mode.
const defaultProbeTimeout = 2 * time.Second

// defaultFailureGrace is how long the pool tolerates a peer's connection in
// TransientFailure before re-probing TLS vs plaintext. Long enough to ignore
// transient blips, short enough to recover before a Raft election timeout
// blames the local node.
const defaultFailureGrace = 3 * time.Second

// PoolConfig holds gRPC dial options shared by all connection pools.
type PoolConfig struct {
	BackoffBaseDelay  time.Duration // Default: 100ms
	BackoffMaxDelay   time.Duration // Default: 1s — must stay below the election timeout
	BackoffMultiplier float64       // Default: 1.6
	BackoffJitter     float64       // Default: 0.2
	Compression       bool          // Enable gzip compression on calls
	AuthToken         string        // Bearer token injected into every outgoing call (for inter-node auth)
}

func (c *PoolConfig) SetDefaults() {
	if c.BackoffBaseDelay == 0 {
		c.BackoffBaseDelay = 100 * time.Millisecond
	}

	if c.BackoffMaxDelay == 0 {
		c.BackoffMaxDelay = time.Second
	}

	if c.BackoffMultiplier == 0 {
		c.BackoffMultiplier = 1.6
	}

	if c.BackoffJitter == 0 {
		c.BackoffJitter = 0.2
	}
}

// TLSPolicy describes the inter-node TLS posture for the connection pool.
//
//   - Mode disabled: TLSConfig is nil, the pool always uses insecure creds.
//   - Mode required: TLSConfig is non-nil and Strict is true, the pool
//     always uses TLS.
//   - Mode optional: TLSConfig is non-nil and Strict is false. The pool
//     probes TLS per peer; on a TLS handshake failure it falls back to
//     plaintext for that peer. A background monitor re-probes when a
//     connection stays unhealthy past a grace window so the pool can
//     follow a peer flipping between modes during a TLS migration.
type TLSPolicy struct {
	TLSConfig *tls.Config
	Strict    bool
}

// AllowsTLS reports whether TLS may be attempted under this policy.
func (p TLSPolicy) AllowsTLS() bool { return p.TLSConfig != nil }

// IsOptional reports whether the pool may fall back from TLS to plaintext
// per peer.
func (p TLSPolicy) IsOptional() bool { return p.TLSConfig != nil && !p.Strict }

// staticTokenCredentials implements grpc.PerRPCCredentials by injecting a static bearer token.
type staticTokenCredentials struct {
	token string
}

func (c staticTokenCredentials) GetRequestMetadata(_ context.Context, _ ...string) (map[string]string, error) {
	return map[string]string{
		"authorization": "Bearer " + c.token,
	}, nil
}

func (c staticTokenCredentials) RequireTransportSecurity() bool {
	return false
}

// BearerTokenDialOption returns a gRPC DialOption that injects a static bearer token
// into every outgoing call. Returns nil if token is empty.
func BearerTokenDialOption(token string) grpc.DialOption {
	return grpc.WithPerRPCCredentials(staticTokenCredentials{token: token})
}

// dialOptions returns the common gRPC dial options derived from PoolConfig.
func dialOptions(creds credentials.TransportCredentials, cfg PoolConfig) []grpc.DialOption {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		// NOTE: BackoffMaxDelay must stay below the election timeout.
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  cfg.BackoffBaseDelay,
				Multiplier: cfg.BackoffMultiplier,
				Jitter:     cfg.BackoffJitter,
				MaxDelay:   cfg.BackoffMaxDelay,
			},
			MinConnectTimeout: 0,
		}),
		grpc.WithInitialWindowSize(GRPCInitialWindowSize),
		grpc.WithInitialConnWindowSize(GRPCInitialConnWindowSize),
		grpc.WithReadBufferSize(GRPCReadBufferSize),
		grpc.WithWriteBufferSize(GRPCWriteBufferSize),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(GRPCMaxMsgSize),
			grpc.MaxCallSendMsgSize(GRPCMaxMsgSize),
		),
	}
	if cfg.Compression {
		opts = append(opts, grpc.WithDefaultCallOptions(
			grpc.UseCompressor(gzip.Name),
		))
	}
	if cfg.AuthToken != "" {
		opts = append(opts, grpc.WithPerRPCCredentials(staticTokenCredentials{token: cfg.AuthToken}))
	}

	return opts
}

// peerEntry holds the per-peer state in the pool.
type peerEntry struct {
	addr        string
	conn        *grpc.ClientConn
	usingTLS    bool
	stopMonitor context.CancelFunc
}

// ConnectionPool manages raw gRPC connections for peers
// This pool can be reused for different services that need gRPC connections.
type ConnectionPool struct {
	mu     sync.Mutex
	peers  map[uint64]*peerEntry
	policy TLSPolicy
	config PoolConfig
	closed bool

	// probeTimeout and failureGrace are overridable in tests.
	probeTimeout time.Duration
	failureGrace time.Duration
}

// NewConnectionPool creates a new gRPC connection pool driven by the given
// TLS policy.
//
// In optional mode, the pool probes each peer's TLS handshake on AddPeer and
// re-probes when a connection stays unhealthy past the failure grace window.
func NewConnectionPool(policy TLSPolicy, cfg PoolConfig) *ConnectionPool {
	cfg.SetDefaults()

	return &ConnectionPool{
		peers:        make(map[uint64]*peerEntry),
		policy:       policy,
		config:       cfg,
		probeTimeout: defaultProbeTimeout,
		failureGrace: defaultFailureGrace,
	}
}

// AddPeer adds a peer to the pool and creates a raw gRPC connection.
// If the peer already exists with the same address, it is a no-op.
func (p *ConnectionPool) AddPeer(id uint64, addr string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return errors.New("connection pool is closed")
	}

	if existing, ok := p.peers[id]; ok && existing.addr == addr {
		return nil
	}

	// Close any pre-existing connection for this peer.
	if existing, ok := p.peers[id]; ok {
		p.teardownLocked(existing)
	}

	entry, err := p.dialPeer(id, addr)
	if err != nil {
		return err
	}

	p.peers[id] = entry

	lifecycle.SendEvent("grpc_connection_created", map[string]any{
		"peerID":  id,
		"address": addr,
		"tls":     entry.usingTLS,
	})

	return nil
}

// dialPeer constructs a peerEntry for the given peer, choosing TLS or
// plaintext according to the pool policy. In optional mode, a probe decides
// per-peer.
func (p *ConnectionPool) dialPeer(id uint64, addr string) (*peerEntry, error) {
	useTLS, err := p.decideTLS(addr)
	if err != nil {
		return nil, err
	}

	conn, err := p.connect(addr, useTLS)
	if err != nil {
		return nil, err
	}

	entry := &peerEntry{
		addr:     addr,
		conn:     conn,
		usingTLS: useTLS,
	}

	if p.policy.IsOptional() {
		ctx, cancel := context.WithCancel(context.Background())
		entry.stopMonitor = cancel

		go p.monitorPeer(ctx, id)
	}

	return entry, nil
}

// decideTLS returns whether the pool should use TLS for this peer. In strict
// modes the answer is purely policy-based. In optional mode the pool probes
// the peer's TLS handshake; a probe that fails with a TLS classification
// falls back to plaintext, whereas a network-level failure is returned to
// the caller (which will retry later).
func (p *ConnectionPool) decideTLS(addr string) (bool, error) {
	switch {
	case !p.policy.AllowsTLS():
		return false, nil
	case p.policy.Strict:
		return true, nil
	}

	err := probeTLS(addr, p.policy.TLSConfig, p.probeTimeout)
	if err == nil {
		return true, nil
	}

	if isTLSHandshakeError(err) {
		return false, nil
	}

	return false, fmt.Errorf("probing TLS for %s: %w", addr, err)
}

// connect builds a *grpc.ClientConn for the given address with the chosen
// transport credentials.
func (p *ConnectionPool) connect(addr string, useTLS bool) (*grpc.ClientConn, error) {
	creds := p.transportCredentials(useTLS)

	return grpc.NewClient("dns:///"+addr, dialOptions(creds, p.config)...)
}

// transportCredentials returns the right TransportCredentials for the chosen
// security level.
func (p *ConnectionPool) transportCredentials(useTLS bool) credentials.TransportCredentials {
	if !useTLS {
		return insecure.NewCredentials()
	}

	return credentials.NewTLS(p.policy.TLSConfig)
}

// RestartConnection forces the peer's connection to be torn down and re-
// dialed. In optional mode this also re-probes TLS, picking up a peer that
// flipped to or from TLS-only.
func (p *ConnectionPool) RestartConnection(id uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	existing, ok := p.peers[id]
	if !ok {
		return fmt.Errorf("no connection for peer %d", id)
	}

	addr := existing.addr
	p.teardownLocked(existing)

	entry, err := p.dialPeer(id, addr)
	if err != nil {
		// On failure the peer entry is gone — caller is expected to retry
		// via AddPeer.
		delete(p.peers, id)

		return err
	}

	p.peers[id] = entry

	return nil
}

// monitorPeer watches a peer's gRPC channel state and triggers a re-probe
// when the channel stays in TransientFailure past the failure grace window.
// Only active in optional mode.
func (p *ConnectionPool) monitorPeer(ctx context.Context, id uint64) {
	for {
		conn := p.getPeerConn(id)
		if conn == nil {
			return
		}

		state := conn.GetState()
		if state == connectivity.Shutdown {
			return
		}

		if state != connectivity.TransientFailure {
			if !conn.WaitForStateChange(ctx, state) {
				return
			}

			continue
		}

		// In TransientFailure: wait the grace window. If still bad, re-probe.
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.failureGrace):
		}

		conn = p.getPeerConn(id)
		if conn == nil || conn.GetState() != connectivity.TransientFailure {
			continue
		}

		// Re-dial; ignore errors here because the next iteration will pick up
		// the new conn (or the peer entry will have been removed).
		_ = p.RestartConnection(id)
	}
}

// getPeerConn returns the live conn for a peer, or nil if the peer has been
// removed.
func (p *ConnectionPool) getPeerConn(id uint64) *grpc.ClientConn {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.peers[id]
	if !ok {
		return nil
	}

	return entry.conn
}

// GetConnection returns the raw gRPC connection for a specific peer, if it exists.
func (p *ConnectionPool) GetConnection(peerID uint64) *grpc.ClientConn {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.peers[peerID]
	if !ok {
		return nil
	}

	return entry.conn
}

// GetPeerAddress returns the address for a specific peer, if it exists.
func (p *ConnectionPool) GetPeerAddress(peerID uint64) string {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.peers[peerID]
	if !ok {
		return ""
	}

	return entry.addr
}

// PeerIDs returns the IDs of all known peers.
func (p *ConnectionPool) PeerIDs() []uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	ids := make([]uint64, 0, len(p.peers))
	for id := range p.peers {
		ids = append(ids, id)
	}

	return ids
}

// RemovePeer removes a peer from the pool and closes its connection.
func (p *ConnectionPool) RemovePeer(id uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	entry, ok := p.peers[id]
	if !ok {
		return nil
	}

	p.teardownLocked(entry)
	delete(p.peers, id)

	lifecycle.SendEvent("grpc_connection_closed", map[string]any{
		"peerID": id,
	})

	return nil
}

// Close closes all gRPC connections.
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.closed = true

	for _, entry := range p.peers {
		p.teardownLocked(entry)
	}

	p.peers = make(map[uint64]*peerEntry)

	return nil
}

func (p *ConnectionPool) teardownLocked(entry *peerEntry) {
	if entry.stopMonitor != nil {
		entry.stopMonitor()
	}

	if entry.conn != nil {
		_ = entry.conn.Close()
	}
}

// probeTLS opens a raw TCP connection to addr and performs a TLS handshake
// using tlsCfg. Returns nil on success, a TLS-classified error on handshake
// failure, or a plain error on network/IO failure.
func probeTLS(addr string, tlsCfg *tls.Config, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	dialer := &net.Dialer{Timeout: timeout}

	rawConn, err := dialer.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer func() { _ = rawConn.Close() }()

	_ = rawConn.SetDeadline(deadline)

	// Clone the TLS config so we don't mutate the caller's struct.
	cfg := tlsCfg.Clone()
	if cfg.ServerName == "" {
		host, _, splitErr := net.SplitHostPort(addr)
		if splitErr == nil {
			cfg.ServerName = host
		}
	}

	tlsConn := tls.Client(rawConn, cfg)
	defer func() { _ = tlsConn.Close() }()

	if err := tlsConn.Handshake(); err != nil {
		return &tlsHandshakeError{err: err}
	}

	return nil
}

// tlsHandshakeError marks an error as originating from a TLS handshake.
// It lets callers distinguish "the peer doesn't speak TLS" from a network
// outage.
type tlsHandshakeError struct {
	err error
}

func (e *tlsHandshakeError) Error() string { return "TLS handshake failed: " + e.err.Error() }
func (e *tlsHandshakeError) Unwrap() error { return e.err }

// isTLSHandshakeError reports whether err originated from a TLS handshake
// failure (server returned non-TLS bytes, alert, version mismatch, etc.) as
// opposed to a network-level failure.
func isTLSHandshakeError(err error) bool {
	var handshakeErr *tlsHandshakeError
	if errors.As(err, &handshakeErr) {
		// Inspect the underlying tls error.
		var recordErr tls.RecordHeaderError
		if errors.As(handshakeErr.err, &recordErr) {
			return true
		}

		// tls.Conn returns a generic error string for many handshake
		// failures, including the "first record does not look like a TLS
		// handshake" case that fires when the peer is plaintext-only.
		if errors.Is(handshakeErr.err, io.EOF) {
			// Closed mid-handshake — treat as a TLS-side failure so we
			// fall back to plaintext. A real network outage surfaces
			// before reaching Handshake().
			return true
		}

		return true
	}

	return false
}
