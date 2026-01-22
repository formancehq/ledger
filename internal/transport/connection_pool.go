package transport

import (
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// ConnectionPool manages raw gRPC connections for peers
// This pool can be reused for different services that need gRPC connections
type ConnectionPool struct {
	mu          sync.Mutex
	peers       map[uint64]string // peer ID -> address
	connections map[uint64]*grpc.ClientConn
}

// NewConnectionPool creates a new gRPC connection pool
func NewConnectionPool() *ConnectionPool {
	return &ConnectionPool{
		peers:       make(map[uint64]string),
		connections: make(map[uint64]*grpc.ClientConn),
	}
}

// AddPeer adds a peer to the pool and creates a raw gRPC connection
func (p *ConnectionPool) AddPeer(id uint64, addr string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	var err error
	p.peers[id] = addr
	p.connections[id], err = p.connect(addr)

	return err
}

func (p *ConnectionPool) connect(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// TODO: Make that configuration
		// TOneverDO: Configure a MaxDelay greater than the election timeout
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   time.Second,
			},
			MinConnectTimeout: 0,
		}),
		grpc.WithInitialWindowSize(16*1024*1024),     // 16MB stream window
		grpc.WithInitialConnWindowSize(64*1024*1024), // 64MB conn window
		grpc.WithReadBufferSize(1*1024*1024),
		grpc.WithWriteBufferSize(1*1024*1024),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64*1024*1024),
			grpc.MaxCallSendMsgSize(64*1024*1024),
		),
		// todo: make configurable
		//grpc.WithDefaultCallOptions(
		//	grpc.UseCompressor(gzip.Name),
		//),
	)
}

func (p *ConnectionPool) RestartConnection(id uint64) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	err := p.connections[id].Close()
	if err != nil {
		return err
	}

	p.connections[id], err = p.connect(p.peers[id])

	return err
}

// GetConnection returns the raw gRPC connection for a specific peer, if it exists
func (p *ConnectionPool) GetConnection(peerID uint64) *grpc.ClientConn {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.connections[peerID]
}

// GetPeerAddress returns the address for a specific peer, if it exists
func (p *ConnectionPool) GetPeerAddress(peerID uint64) string {
	return p.peers[peerID]
}

// Close closes all gRPC connections
func (p *ConnectionPool) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, conn := range p.connections {
		if err := conn.Close(); err != nil {
			return err
		}
	}
	p.connections = make(map[uint64]*grpc.ClientConn)
	p.peers = make(map[uint64]string)

	return nil
}
