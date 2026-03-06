package transport

import (
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/encoding/gzip"
)

const (
	GRPCInitialWindowSize     = 16 << 20 // 16 MB
	GRPCInitialConnWindowSize = 64 << 20 // 64 MB
	GRPCReadBufferSize        = 1 << 20  // 1 MB
	GRPCWriteBufferSize       = 1 << 20  // 1 MB
	GRPCMaxMsgSize            = 64 << 20 // 64 MB (large snapshots use chunked streaming)
)

// PoolConfig holds gRPC dial options shared by all connection pools.
type PoolConfig struct {
	BackoffBaseDelay  time.Duration // Default: 100ms
	BackoffMaxDelay   time.Duration // Default: 1s — must stay below the election timeout
	BackoffMultiplier float64       // Default: 1.6
	BackoffJitter     float64       // Default: 0.2
	Compression       bool          // Enable gzip compression on calls
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

	return opts
}

// ConnectionPool manages raw gRPC connections for peers
// This pool can be reused for different services that need gRPC connections.
type ConnectionPool struct {
	mu          sync.Mutex
	peers       map[uint64]string // peer ID -> address
	connections map[uint64]*grpc.ClientConn
	creds       credentials.TransportCredentials
	config      PoolConfig
}

// NewConnectionPool creates a new gRPC connection pool.
func NewConnectionPool(creds credentials.TransportCredentials, cfg PoolConfig) *ConnectionPool {
	cfg.SetDefaults()

	return &ConnectionPool{
		peers:       make(map[uint64]string),
		connections: make(map[uint64]*grpc.ClientConn),
		creds:       creds,
		config:      cfg,
	}
}

// AddPeer adds a peer to the pool and creates a raw gRPC connection.
// If the peer already exists with the same address, it is a no-op.
func (p *ConnectionPool) AddPeer(id uint64, addr string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if existing, ok := p.peers[id]; ok && existing == addr {
		return nil
	}

	// Close any pre-existing connection for this peer.
	if conn, ok := p.connections[id]; ok {
		_ = conn.Close() // best-effort close before replacing
	}

	conn, err := p.connect(addr)
	if err != nil {
		return err
	}

	p.peers[id] = addr
	p.connections[id] = conn

	return nil
}

func (p *ConnectionPool) connect(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient("dns:///"+addr, dialOptions(p.creds, p.config)...)
}

func (p *ConnectionPool) RestartConnection(id uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn := p.connections[id]
	if conn == nil {
		return fmt.Errorf("no connection for peer %d", id)
	}

	if err := conn.Close(); err != nil {
		return err
	}

	var err error

	p.connections[id], err = p.connect(p.peers[id])

	return err
}

// GetConnection returns the raw gRPC connection for a specific peer, if it exists.
func (p *ConnectionPool) GetConnection(peerID uint64) *grpc.ClientConn {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.connections[peerID]
}

// GetPeerAddress returns the address for a specific peer, if it exists.
func (p *ConnectionPool) GetPeerAddress(peerID uint64) string {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.peers[peerID]
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

	conn, ok := p.connections[id]
	if !ok {
		return nil
	}

	err := conn.Close()

	delete(p.connections, id)
	delete(p.peers, id)

	return err
}

// Close closes all gRPC connections.
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conn := range p.connections {
		err := conn.Close()
		if err != nil {
			return err
		}
	}

	p.connections = make(map[uint64]*grpc.ClientConn)
	p.peers = make(map[uint64]string)

	return nil
}
