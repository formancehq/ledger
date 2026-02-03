package testserver

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/snapshotpb"
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

		servicepb.RegisterLedgerServiceServer(server, &ledgerServiceGateway{
			logger: g.logger.WithFields(map[string]any{
				"gateway_port": port,
				"node_addr":    nodeAddr,
			}),
			client: servicepb.NewLedgerServiceClient(conn),
		})

		snapshotpb.RegisterSnapshotServiceServer(server, &snapshotServiceGateway{
			logger: g.logger.WithFields(map[string]any{
				"gateway_port": port,
				"node_addr":    nodeAddr,
			}),
			client: snapshotpb.NewSnapshotServiceClient(conn),
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

			// Handle ping messages (pass through)
			if req.GetPing() != nil {
				if err := clientStream.Send(req); err != nil {
					errCh <- fmt.Errorf("failed to send ping to backend: %w", err)
					return
				}
				continue
			}

			// Handle raft batch messages
			if req.GetRaft() == nil {
				continue
			}

			// Filter messages through interceptor
			interceptor := g.gateway.GetInterceptor()
			if interceptor == nil {
				// No interceptor, forward as-is
				if err := clientStream.Send(req); err != nil {
					errCh <- fmt.Errorf("failed to send to backend: %w", err)
					return
				}
				continue
			}

			// Apply interceptor to each message in the batch
			filteredMessages := make([]*raft.RaftRequestMessage, 0, len(req.GetRaft().Messages))
			for _, raftReqMsg := range req.GetRaft().Messages {
				raftMsg := &raftpb.Message{}
				if err := raftMsg.Unmarshal(raftReqMsg.Message); err != nil {
					g.logger.WithFields(map[string]any{
						"error": err,
					}).Errorf("Failed to unmarshal Raft message for interception")
					// Include message even if unmarshal fails
					filteredMessages = append(filteredMessages, raftReqMsg)
					continue
				}

				// Check interceptor
				if !interceptor.InterceptRequest(raftMsg) {
					g.logger.WithFields(map[string]any{
						"from_node": raftMsg.From,
						"to_node":   raftMsg.To,
						"msg_type":  raftMsg.Type.String(),
					}).Debugf("Message blocked by interceptor")
					continue // Skip this message
				}
				filteredMessages = append(filteredMessages, raftReqMsg)
			}

			// If all messages were filtered out, skip sending
			if len(filteredMessages) == 0 {
				continue
			}

			// Send filtered batch
			filteredReq := &raft.SendMessageRequest{
				Message: &raft.SendMessageRequest_Raft{
					Raft: &raft.RaftRequestBatch{
						Messages: filteredMessages,
					},
				},
			}
			if err := clientStream.Send(filteredReq); err != nil {
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
	servicepb.UnimplementedLedgerServiceServer
	logger logging.Logger
	client servicepb.LedgerServiceClient
}

func (g *ledgerServiceGateway) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	return g.client.Apply(ctx, req)
}

func (g *ledgerServiceGateway) CreateLedger(ctx context.Context, req *servicepb.CreateLedgerRequest) (*commonpb.LedgerInfo, error) {
	resp, err := g.client.Apply(ctx, &servicepb.ApplyRequest{
		Actions: []*servicepb.Request{
			{
				Type: &servicepb.Request_CreateLedger{
					CreateLedger: req,
				},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(resp.Logs) == 0 {
		return nil, fmt.Errorf("no logs returned")
	}
	return resp.Logs[0].Payload.GetCreateLedger().GetInfo(), nil
}

func (g *ledgerServiceGateway) DeleteLedger(ctx context.Context, req *servicepb.DeleteLedgerRequest) (*servicepb.DeleteLedgerResponse, error) {
	_, err := g.client.Apply(ctx, &servicepb.ApplyRequest{
		Actions: []*servicepb.Request{
			{
				Type: &servicepb.Request_DeleteLedger{
					DeleteLedger: req,
				},
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return &servicepb.DeleteLedgerResponse{}, nil
}

func (g *ledgerServiceGateway) GetAllLedgersInfo(req *servicepb.GetAllLedgersRequest, stream servicepb.LedgerService_GetAllLedgersInfoServer) error {
	ctx := stream.Context()

	clientStream, err := g.client.GetAllLedgersInfo(ctx, req)
	if err != nil {
		return err
	}

	for {
		ledger, err := clientStream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(ledger); err != nil {
			return err
		}
	}
}

func (g *ledgerServiceGateway) GetLedger(ctx context.Context, req *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
	return g.client.GetLedger(ctx, req)
}

func (g *ledgerServiceGateway) GetTransaction(ctx context.Context, req *servicepb.GetTransactionRequest) (*commonpb.Transaction, error) {
	return g.client.GetTransaction(ctx, req)
}

func (g *ledgerServiceGateway) StreamLogs(req *servicepb.StreamLogsRequest, stream servicepb.LedgerService_StreamLogsServer) error {
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

// snapshotServiceGateway forwards SnapshotService calls
type snapshotServiceGateway struct {
	snapshotpb.UnimplementedSnapshotServiceServer
	logger logging.Logger
	client snapshotpb.SnapshotServiceClient
}

func (g *snapshotServiceGateway) DescribeSnapshot(ctx context.Context, req *snapshotpb.DescribeSnapshotRequest) (*snapshotpb.DescribeSnapshotResponse, error) {
	return g.client.DescribeSnapshot(ctx, req)
}

func (g *snapshotServiceGateway) FetchSnapshot(req *snapshotpb.FetchSnapshotRequest, stream grpc.ServerStreamingServer[snapshotpb.FetchSnapshotResponse]) error {
	ctx := stream.Context()

	// Create client stream to backend
	clientStream, err := g.client.FetchSnapshot(ctx, req)
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
