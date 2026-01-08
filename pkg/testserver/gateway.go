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
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Gateway is a gRPC gateway that forwards requests to backend nodes
// Each port forwards to a specific node, allowing network manipulation during tests
type Gateway struct {
	logger    logging.Logger
	ports     []int
	nodes     []string // node addresses (e.g., "127.0.0.1:8000")
	servers   []*grpc.Server
	listeners []net.Listener
	conns     []*grpc.ClientConn // client connections to backend nodes
	wg        sync.WaitGroup
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
			client: raft.NewRaftTransportServiceClient(conn),
		})

		service.RegisterSystemServiceServer(server, &systemServiceGateway{
			logger: g.logger.WithFields(map[string]any{
				"gateway_port": port,
				"node_addr":    nodeAddr,
			}),
			client: service.NewSystemServiceClient(conn),
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
	logger logging.Logger
	client raft.RaftTransportServiceClient
}

func (g *raftTransportGateway) StreamMessages(stream grpc.BidiStreamingServer[raft.SendMessageRequest, raft.SendMessageResponse]) error {
	ctx := stream.Context()

	// Create client stream to backend
	clientStream, err := g.client.StreamMessages(ctx)
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
			msg, err := stream.Recv()
			if err != nil {
				if err != io.EOF {
					errCh <- fmt.Errorf("failed to receive from client: %w", err)
				} else {
					errCh <- nil
				}
				return
			}
			if err := clientStream.Send(msg); err != nil {
				errCh <- fmt.Errorf("failed to send to backend: %w", err)
				return
			}
		}
	}()

	// Forward from backend to client
	go func() {
		for {
			msg, err := clientStream.Recv()
			if err != nil {
				if err != io.EOF {
					errCh <- fmt.Errorf("failed to receive from backend: %w", err)
				} else {
					errCh <- nil
				}
				return
			}
			if err := stream.Send(msg); err != nil {
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

// systemServiceGateway forwards SystemService calls
type systemServiceGateway struct {
	service.UnimplementedSystemServiceServer
	logger logging.Logger
	client service.SystemServiceClient
}

func (g *systemServiceGateway) Snapshot(ctx context.Context, req *service.SnapshotRequest) (*service.SnapshotResponse, error) {
	return g.client.Snapshot(ctx, req)
}

func (g *systemServiceGateway) CreateLedger(ctx context.Context, req *service.CreateLedgerRequest) (*service.CreateLedgerResponse, error) {
	return g.client.CreateLedger(ctx, req)
}

func (g *systemServiceGateway) DeleteLedger(ctx context.Context, req *service.DeleteLedgerRequest) (*service.DeleteLedgerResponse, error) {
	return g.client.DeleteLedger(ctx, req)
}

func (g *systemServiceGateway) ResolveLedger(ctx context.Context, req *service.ResolveLedgerRequest) (*service.ResolveLedgerResponse, error) {
	return g.client.ResolveLedger(ctx, req)
}

func (g *systemServiceGateway) GetAllLedgersInfo(ctx context.Context, req *service.GetAllLedgersRequest) (*service.GetAllLedgersResponse, error) {
	return g.client.GetAllLedgersInfo(ctx, req)
}

func (g *systemServiceGateway) GetLedgerInfo(ctx context.Context, req *service.GetLedgerByNameRequest) (*service.GetLedgerByNameResponse, error) {
	return g.client.GetLedgerInfo(ctx, req)
}

func (g *systemServiceGateway) ResolveLedgerLeader(ctx context.Context, req *service.ResolveLedgerLeaderRequest) (*service.ResolveLedgerLeaderResponse, error) {
	return g.client.ResolveLedgerLeader(ctx, req)
}

// ledgerServiceGateway forwards LedgerService calls
type ledgerServiceGateway struct {
	ledgerpb.UnimplementedLedgerServiceServer
	logger logging.Logger
	client ledgerpb.LedgerServiceClient
}

func (g *ledgerServiceGateway) Snapshot(ctx context.Context, req *ledgerpb.LedgerSnapshotRequest) (*ledgerpb.LedgerSnapshotResponse, error) {
	return g.client.Snapshot(ctx, req)
}

func (g *ledgerServiceGateway) CreateTransaction(ctx context.Context, req *ledgerpb.CreateTransactionRequest) (*ledgerpb.CreateTransactionResponse, error) {
	return g.client.CreateTransaction(ctx, req)
}

func (g *ledgerServiceGateway) RevertTransaction(ctx context.Context, req *ledgerpb.RevertTransactionRequest) (*ledgerpb.RevertTransactionResponse, error) {
	return g.client.RevertTransaction(ctx, req)
}

func (g *ledgerServiceGateway) SaveAccountMetadata(ctx context.Context, req *ledgerpb.SaveAccountMetadataRequest) (*ledgerpb.SaveAccountMetadataResponse, error) {
	return g.client.SaveAccountMetadata(ctx, req)
}

func (g *ledgerServiceGateway) SaveTransactionMetadata(ctx context.Context, req *ledgerpb.SaveTransactionMetadataRequest) (*ledgerpb.SaveTransactionMetadataResponse, error) {
	return g.client.SaveTransactionMetadata(ctx, req)
}

func (g *ledgerServiceGateway) DeleteAccountMetadata(ctx context.Context, req *ledgerpb.DeleteAccountMetadataRequest) (*ledgerpb.DeleteAccountMetadataResponse, error) {
	return g.client.DeleteAccountMetadata(ctx, req)
}

func (g *ledgerServiceGateway) DeleteTransactionMetadata(ctx context.Context, req *ledgerpb.DeleteTransactionMetadataRequest) (*ledgerpb.DeleteTransactionMetadataResponse, error) {
	return g.client.DeleteTransactionMetadata(ctx, req)
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

