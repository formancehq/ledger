package transport

import (
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ConnectionPool manages raw gRPC connections for peers
// This pool can be reused for different services that need gRPC connections
type ConnectionPool struct {
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
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to create gRPC connection for peer %x: %w", id, err)
	}

	p.peers[id] = addr
	p.connections[id] = conn
	return nil
}

// RemovePeer removes a peer from the pool and closes its gRPC connection
func (p *ConnectionPool) RemovePeer(id uint64) {
	delete(p.peers, id)
	if conn, exists := p.connections[id]; exists {
		_ = conn.Close()
		delete(p.connections, id)
	}
}

// GetConnection returns the raw gRPC connection for a specific peer, if it exists
func (p *ConnectionPool) GetConnection(peerID uint64) *grpc.ClientConn {
	return p.connections[peerID]
}

// GetPeerAddress returns the address for a specific peer, if it exists
func (p *ConnectionPool) GetPeerAddress(peerID uint64) string {
	return p.peers[peerID]
}

// GetAllConnections returns all connections (useful for iterating over all peers)
func (p *ConnectionPool) GetAllConnections() map[uint64]*grpc.ClientConn {
	result := make(map[uint64]*grpc.ClientConn, len(p.connections))
	for id, conn := range p.connections {
		result[id] = conn
	}
	return result
}

// Close closes all gRPC connections
func (p *ConnectionPool) Close() {
	for _, conn := range p.connections {
		_ = conn.Close()
	}
	p.connections = make(map[uint64]*grpc.ClientConn)
	p.peers = make(map[uint64]string)
}
