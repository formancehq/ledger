package raft

import (
	"context"
	"fmt"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
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
		t.logger.WithFields(map[string]any{"type": msg.Type.String(), "from": fmt.Sprintf("%x", msg.From), "to": fmt.Sprintf("%x", msg.To)}).Debugf("Received message via gRPC")
		return &SendMessageResponse{Success: true}, nil
	case <-ctx.Done():
		return &SendMessageResponse{
			Success: false,
			Error:   "context cancelled",
		}, nil
	default:
		t.logger.Infof("WARN: Recv channel full, dropping message")
		return &SendMessageResponse{
			Success: false,
			Error:   "recv channel full",
		}, nil
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

