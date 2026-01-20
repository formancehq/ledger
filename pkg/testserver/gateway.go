package testserver

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// MessageInterceptor allows intercepting messages between nodes
// Return true to allow the message to pass through, false to block it
type MessageInterceptor interface {
	// InterceptRequest is called for each outgoing message from a node
	// fromNodeID: the ID of the node sending the message (1-based)
	// toNodeID: the ID of the node receiving the message (1-based)
	// msg: the Raft message being sent
	// Returns true to allow the message, false to block it
	InterceptRequest(msg *raftpb.Message) bool
}

type MessageInterceptorFunc func(msg *raftpb.Message) bool

func (f MessageInterceptorFunc) InterceptRequest(msg *raftpb.Message) bool {
	return f(msg)
}

// Gateway is a gRPC gateway that forwards requests to backend nodes
// Each port forwards to a specific node, allowing network manipulation during tests
type Gateway struct {
	logger        logging.Logger
	ports         []int
	nodes         []string // node addresses (e.g., "127.0.0.1:8000")
	servers       []*grpc.Server
	listeners     []net.Listener
	conns         []*grpc.ClientConn // client connections to backend nodes
	interceptor   MessageInterceptor
	interceptorMu sync.RWMutex
	wg            sync.WaitGroup
}

// NewGateway creates a new gateway that listens on the given ports and forwards to the given node addresses
// ports and nodes must have the same length, where ports[i] forwards to nodes[i]
func NewGateway(logger logging.Logger, ports []int, nodes []string) (*Gateway, error) {
	if len(ports) != len(nodes) {
		return nil, fmt.Errorf("ports and nodes must have the same length")
	}
	if len(ports) == 0 {
		return nil, fmt.Errorf("at least one port/node pair is required")
	}

	return &Gateway{
		logger:    logger,
		ports:     ports,
		nodes:     nodes,
		servers:   make([]*grpc.Server, len(ports)),
		listeners: make([]net.Listener, len(ports)),
		conns:     make([]*grpc.ClientConn, len(ports)),
	}, nil
}

// SetInterceptor sets the message interceptor for the gateway
// The interceptor will be called for all messages passing through the gateway
// Pass nil to remove the interceptor
func (g *Gateway) SetInterceptor(interceptor MessageInterceptor) {
	g.interceptorMu.Lock()
	defer g.interceptorMu.Unlock()
	g.interceptor = interceptor
}

// GetInterceptor returns the current message interceptor
func (g *Gateway) GetInterceptor() MessageInterceptor {
	g.interceptorMu.RLock()
	defer g.interceptorMu.RUnlock()
	return g.interceptor
}

// RemoveInterceptor removes the message interceptor from the gateway
// This is equivalent to calling SetInterceptor(nil)
func (g *Gateway) RemoveInterceptor() {
	g.interceptorMu.Lock()
	defer g.interceptorMu.Unlock()
	g.interceptor = nil
}

// Start starts the gateway servers on all configured ports
func (g *Gateway) Start(ctx context.Context) error {
	for i, port := range g.ports {
		nodeAddr := g.nodes[i]

		// Create gRPC client connection to the backend node
		conn, err := grpc.NewClient(nodeAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return fmt.Errorf("failed to create connection to node %s: %w", nodeAddr, err)
		}

		// Store connection for cleanup
		g.conns[i] = conn

		// Create gRPC server
		server := grpc.NewServer()

		// Register all services with forwarding implementations
		raft.RegisterRaftTransportServiceServer(server, &raftTransportGateway{
			logger: g.logger.WithFields(map[string]any{
				"gateway_port": port,
				"node_addr":    nodeAddr,
			}),
			client:  raft.NewRaftTransportServiceClient(conn),
			gateway: g,
		})

		ledgerpb.RegisterLedgerServiceServer(server, &ledgerServiceGateway{
			logger: g.logger.WithFields(map[string]any{
				"gateway_port": port,
				"node_addr":    nodeAddr,
			}),
			client: ledgerpb.NewLedgerServiceClient(conn),
		})

		// Start listening
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			return fmt.Errorf("failed to listen on port %d: %w", port, err)
		}

		g.servers[i] = server
		g.listeners[i] = lis

		// Start server in goroutine
		g.wg.Add(1)
		go func(idx int, srv *grpc.Server, l net.Listener) {
			defer g.wg.Done()
			g.logger.WithFields(map[string]any{
				"port":      port,
				"node_addr": nodeAddr,
			}).Infof("Gateway server started on port %d forwarding to %s", port, nodeAddr)
			if err := srv.Serve(l); err != nil {
				g.logger.WithFields(map[string]any{
					"port":      port,
					"node_addr": nodeAddr,
					"error":     err,
				}).Errorf("Gateway server error")
			}
		}(i, server, lis)
	}

	return nil
}

// Stop stops all gateway servers and closes connections
func (g *Gateway) Stop(ctx context.Context) error {
	for i, server := range g.servers {
		if server != nil {
			g.logger.WithFields(map[string]any{
				"port": g.ports[i],
			}).Infof("Stopping gateway server on port %d", g.ports[i])
			server.GracefulStop()
		}
		if g.listeners[i] != nil {
			_ = g.listeners[i].Close()
		}
		if g.conns[i] != nil {
			_ = g.conns[i].Close()
		}
	}

	// Wait for all servers to stop
	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// raftTransportGateway forwards RaftTransportService calls
type raftTransportGateway struct {
	raft.UnimplementedRaftTransportServiceServer
	logger  logging.Logger
	client  raft.RaftTransportServiceClient
	gateway *Gateway
}

func (g *raftTransportGateway) StreamMessages(stream grpc.BidiStreamingServer[raft.SendMessageRequest, raft.SendMessageResponse]) error {
	ctx := stream.Context()

	md, _ := metadata.FromIncomingContext(ctx)

	// Create client stream to backend
	clientStream, err := g.client.StreamMessages(metadata.NewOutgoingContext(ctx, md))
	if err != nil {
		return fmt.Errorf("failed to create client stream: %w", err)
	}

	// Forward messages bidirectionally
	errCh := make(chan error, 2)

	// Forward from client to backend
	go func() {
		defer func() {
			_ = clientStream.CloseSend()
		}()
		for {
			req, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					errCh <- fmt.Errorf("failed to receive from client: %w", err)
				} else {
					errCh <- nil
				}
				return
			}
			if req.GetRaft() == nil {
				continue
			}

			raftMsg := &raftpb.Message{}
			if err := raftMsg.Unmarshal(req.GetRaft().Message); err != nil {
				g.logger.WithFields(map[string]any{
					"error": err,
				}).Errorf("Failed to unmarshal Raft message for interception")
				// Continue forwarding even if unmarshal fails
			} else {
				// Check interceptor
				interceptor := g.gateway.GetInterceptor()
				if interceptor != nil {
					if !interceptor.InterceptRequest(raftMsg) {
						g.logger.WithFields(map[string]any{
							"from_node": raftMsg.From,
							"to_node":   raftMsg.To,
							"msg_type":  raftMsg.Type.String(),
						}).Debugf("Message blocked by interceptor")
						continue // Skip this message
					}
				}
			}

			if err := clientStream.Send(req); err != nil {
				errCh <- fmt.Errorf("failed to send to backend: %w", err)
				return
			}
		}
	}()

	// Forward from backend to client
	go func() {
		for {
			resp, err := clientStream.Recv()
			if err != nil {
				if err != io.EOF {
					errCh <- fmt.Errorf("failed to receive from backend: %w", err)
				} else {
					errCh <- nil
				}
				return
			}

			// Note: RaftResponseMessage doesn't contain the Raft message itself,
			// only Success/Error status. Interception of responses would require
			// tracking request IDs, which is more complex. For now, we only
			// intercept requests where we have the full Raft message.

			if err := stream.Send(resp); err != nil {
				errCh <- fmt.Errorf("failed to send to client: %w", err)
				return
			}
		}
	}()

	// Wait for first error or both streams to close
	err1 := <-errCh
	err2 := <-errCh

	if err1 != nil {
		return err1
	}
	return err2
}

// ledgerServiceGateway forwards LedgerService calls
type ledgerServiceGateway struct {
	ledgerpb.UnimplementedLedgerServiceServer
	logger logging.Logger
	client ledgerpb.LedgerServiceClient
}

func (g *ledgerServiceGateway) CreateTransaction(ctx context.Context, req *ledgerpb.CreateTransactionRequest) (*ledgerpb.Log, error) {
	return g.client.CreateTransaction(ctx, req)
}

func (g *ledgerServiceGateway) RevertTransaction(ctx context.Context, req *ledgerpb.RevertTransactionRequest) (*ledgerpb.Log, error) {
	return g.client.RevertTransaction(ctx, req)
}

func (g *ledgerServiceGateway) SaveAccountMetadata(ctx context.Context, req *ledgerpb.SaveAccountMetadataRequest) (*ledgerpb.Log, error) {
	return g.client.SaveAccountMetadata(ctx, req)
}

func (g *ledgerServiceGateway) SaveTransactionMetadata(ctx context.Context, req *ledgerpb.SaveTransactionMetadataRequest) (*ledgerpb.Log, error) {
	return g.client.SaveTransactionMetadata(ctx, req)
}

func (g *ledgerServiceGateway) DeleteAccountMetadata(ctx context.Context, req *ledgerpb.DeleteAccountMetadataRequest) (*ledgerpb.Log, error) {
	return g.client.DeleteAccountMetadata(ctx, req)
}

func (g *ledgerServiceGateway) DeleteTransactionMetadata(ctx context.Context, req *ledgerpb.DeleteTransactionMetadataRequest) (*ledgerpb.Log, error) {
	return g.client.DeleteTransactionMetadata(ctx, req)
}

func (g *ledgerServiceGateway) CreateLedger(ctx context.Context, req *ledgerpb.CreateLedgerCommand) (*ledgerpb.LedgerInfo, error) {
	return g.client.CreateLedger(ctx, req)
}

func (g *ledgerServiceGateway) DeleteLedger(ctx context.Context, req *ledgerpb.DeleteLedgerCommand) (*ledgerpb.DeleteLedgerResponse, error) {
	return g.client.DeleteLedger(ctx, req)
}

func (g *ledgerServiceGateway) GetAllLedgersInfo(ctx context.Context, req *ledgerpb.GetAllLedgersRequest) (*ledgerpb.GetAllLedgersResponse, error) {
	return g.client.GetAllLedgersInfo(ctx, req)
}

func (g *ledgerServiceGateway) GetLedgerInfo(ctx context.Context, req *ledgerpb.GetLedgerByNameRequest) (*ledgerpb.LedgerInfo, error) {
	return g.client.GetLedgerByName(ctx, req)
}

func (g *ledgerServiceGateway) GetLedgerByName(ctx context.Context, req *ledgerpb.GetLedgerByNameRequest) (*ledgerpb.LedgerInfo, error) {
	return g.client.GetLedgerByName(ctx, req)
}

func (g *ledgerServiceGateway) StreamLogs(req *ledgerpb.StreamLogsRequest, stream ledgerpb.LedgerService_StreamLogsServer) error {
	ctx := stream.Context()

	// Create client stream to backend
	clientStream, err := g.client.StreamLogs(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to create client stream: %w", err)
	}

	// Forward messages from backend to client
	for {
		msg, err := clientStream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("failed to receive from backend: %w", err)
		}
		if err := stream.Send(msg); err != nil {
			return fmt.Errorf("failed to send to client: %w", err)
		}
	}
}
