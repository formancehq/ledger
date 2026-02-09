package transport

import (
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials/insecure"
)

// ServiceConnectionPool manages gRPC connections to service ports on peer nodes
// This is separate from the Raft ConnectionPool because service API and Raft transport
// use different ports.
type ServiceConnectionPool struct {
	mu          sync.Mutex
	peers       map[uint64]string // peer ID -> service address
	connections map[uint64]*grpc.ClientConn
}

// NewServiceConnectionPool creates a new service connection pool
func NewServiceConnectionPool() *ServiceConnectionPool {
	return &ServiceConnectionPool{
		peers:       make(map[uint64]string),
		connections: make(map[uint64]*grpc.ClientConn),
	}
}

// AddPeer adds a peer to the pool and creates a gRPC connection to its service port
func (p *ServiceConnectionPool) AddPeer(id uint64, serviceAddr string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	conn, err := p.connect(serviceAddr)
	if err != nil {
		return err
	}

	p.peers[id] = serviceAddr
	p.connections[id] = conn

	return nil
}

func (p *ServiceConnectionPool) connect(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient("dns:///"+addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   time.Second,
			},
			MinConnectTimeout: 0,
		}),
		grpc.WithInitialWindowSize(16*1024*1024),
		grpc.WithInitialConnWindowSize(64*1024*1024),
		grpc.WithReadBufferSize(1*1024*1024),
		grpc.WithWriteBufferSize(1*1024*1024),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(64*1024*1024),
			grpc.MaxCallSendMsgSize(64*1024*1024),
		),
	)
}

// GetConnection returns the gRPC connection for a specific peer's service port
func (p *ServiceConnectionPool) GetConnection(peerID uint64) *grpc.ClientConn {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.connections[peerID]
}

// Close closes all gRPC connections
func (p *ServiceConnectionPool) Close() error {
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
