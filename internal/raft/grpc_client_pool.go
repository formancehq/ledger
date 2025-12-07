package raft

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"

	"github.com/formancehq/ledger-v3-poc/internal/transport"
)

// GRPCClientPool manages Raft-specific gRPC clients using a connection pool
// It wraps transport.ConnectionPool and creates RaftTransportServiceClient instances dynamically
type GRPCClientPool struct {
	connectionPool *transport.ConnectionPool
}

// NewGRPCClientPool creates a new gRPC client pool using the provided connection pool
func NewGRPCClientPool(connectionPool *transport.ConnectionPool) *GRPCClientPool {
	return &GRPCClientPool{
		connectionPool: connectionPool,
	}
}

// AddPeer adds a peer to the connection pool
func (p *GRPCClientPool) AddPeer(id uint64, addr string) error {
	return p.connectionPool.AddPeer(id, addr)
}

// RemovePeer removes a peer from the pool
func (p *GRPCClientPool) RemovePeer(id uint64) {
	p.connectionPool.RemovePeer(id)
}

// GetPeerConnection returns the raw gRPC connection for a specific peer, if it exists
func (p *GRPCClientPool) GetPeerConnection(peerID uint64) *grpc.ClientConn {
	return p.connectionPool.GetConnection(peerID)
}

// GetPeerAddress returns the address for a specific peer, if it exists
func (p *GRPCClientPool) GetPeerAddress(peerID uint64) (string, bool) {
	return p.connectionPool.GetPeerAddress(peerID)
}

// GetConnectionPool returns the underlying connection pool
func (p *GRPCClientPool) GetConnectionPool() *transport.ConnectionPool {
	return p.connectionPool
}

// SendMessage sends a Raft message to a peer via gRPC
func (p *GRPCClientPool) SendMessage(ctx context.Context, peerID uint64, msg raftpb.Message) error {
	conn := p.connectionPool.GetConnection(peerID)
	if conn == nil {
		return fmt.Errorf("no connection for peer %x", peerID)
	}

	// Create client dynamically from the connection
	client := NewRaftTransportServiceClient(conn)

	data, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	req := &SendMessageRequest{Message: data}
	_, err = client.SendMessage(ctx, req)
	return err
}

// Close closes the underlying connection pool
func (p *GRPCClientPool) Close() {
	p.connectionPool.Close()
}
