package raft

import (
	"context"
	"fmt"
	"net"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RegisterRaftService registers the RaftTransportService on the given gRPC server
func (t *Transport) RegisterRaftService(server *grpc.Server) {
	grpcTransportServer := &grpcTransportServerWrapper{transport: t}
	RegisterRaftTransportServiceServer(server, grpcTransportServer)
}

// HandleSendMessage handles unary gRPC calls for sending messages
// This method can be called from an external gRPC server
func (t *Transport) HandleSendMessage(ctx context.Context, req *SendMessageRequest) (*SendMessageResponse, error) {
	var msg raftpb.Message
	if err := msg.Unmarshal(req.Message); err != nil {
		return &SendMessageResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to unmarshal message: %v", err),
		}, nil
	}

	// Send message to recvCh for processing
	select {
	case t.recvCh <- msg:
		t.logger.Debug("Received message via gRPC",
			zap.String("type", msg.Type.String()),
			zap.String("from", fmt.Sprintf("%x", msg.From)),
			zap.String("to", fmt.Sprintf("%x", msg.To)))
		return &SendMessageResponse{Success: true}, nil
	case <-ctx.Done():
		return &SendMessageResponse{
			Success: false,
			Error:   "context cancelled",
		}, nil
	default:
		t.logger.Warn("Recv channel full, dropping message")
		return &SendMessageResponse{
			Success: false,
			Error:   "recv channel full",
		}, nil
	}
}

// grpcClient wraps a gRPC client connection for a peer
type grpcClient struct {
	conn   *grpc.ClientConn
	client RaftTransportServiceClient
	mu     sync.Mutex
}

// sendMessage sends a message using unary gRPC call
func (c *grpcClient) sendMessage(ctx context.Context, msg raftpb.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	data, err := msg.Marshal()
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	req := &SendMessageRequest{Message: data}
	_, err = c.client.SendMessage(ctx, req)
	return err
}

// close closes the gRPC client connection
func (c *grpcClient) close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// newGRPCClient creates a new gRPC client for a peer
func newGRPCClient(addr string) (*grpcClient, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC client: %w", err)
	}

	return &grpcClient{
		conn:   conn,
		client: NewRaftTransportServiceClient(conn),
	}, nil
}

// startGRPCServer starts the gRPC server for the transport
// This method is kept for backward compatibility but should not be used
// when using a unified gRPC server. The transport should be registered
// on the unified server instead.
func (t *Transport) startGRPCServer() error {
	// Parse address to get host:port
	host, port, err := net.SplitHostPort(t.addr)
	if err != nil {
		return fmt.Errorf("invalid address format: %w", err)
	}

	// Create gRPC server
	grpcServer := grpc.NewServer()
	grpcTransportServer := &grpcTransportServerWrapper{transport: t}
	RegisterRaftTransportServiceServer(grpcServer, grpcTransportServer)

	// Start listening
	listener, err := net.Listen("tcp", net.JoinHostPort(host, port))
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", t.addr, err)
	}
	t.listener = listener

	// Start serving in a goroutine
	go func() {
		if err := grpcServer.Serve(listener); err != nil {
			t.logger.Error("gRPC server error", zap.Error(err))
		}
	}()

	// Store grpcServer for cleanup
	t.grpcServer = grpcServer

	return nil
}

// stopGRPCServer stops the gRPC server
func (t *Transport) stopGRPCServer() {
	if t.grpcServer != nil {
		t.grpcServer.GracefulStop()
	}
}

// grpcTransportServerWrapper wraps the transport to implement RaftTransportServiceServer
type grpcTransportServerWrapper struct {
	UnimplementedRaftTransportServiceServer
	transport *Transport
}

func (s *grpcTransportServerWrapper) SendMessage(ctx context.Context, req *SendMessageRequest) (*SendMessageResponse, error) {
	return s.transport.HandleSendMessage(ctx, req)
}
