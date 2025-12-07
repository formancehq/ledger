package grpc

import (
	"context"
	"fmt"
	"net"
	"strings"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/service"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

// SnapshotClient is an interface for snapshot operations
type SnapshotClient interface {
	Snapshot() error
	CreateBucketSnapshot(bucketName string) error
}

// RaftTransportHandler is an interface for registering Raft transport service on a gRPC server
type RaftTransportHandler interface {
	RegisterRaftService(*grpc.Server)
}

type Server struct {
	server               *grpc.Server
	listener             net.Listener
	logger               logging.Logger
	port                 int
	ledgerService        service.Ledger
	raftTransportHandler RaftTransportHandler // Handler for registering Raft service (required)
	snapshotClient       SnapshotClient       // Client for snapshot operations
}

func NewServer(port int, logger logging.Logger, ledgerService service.Ledger, raftHandler RaftTransportHandler, snapshotClient SnapshotClient) *Server {
	if raftHandler == nil {
		panic("raftHandler cannot be nil - unified gRPC server requires Raft transport handler")
	}
	return &Server{
		port:                 port,
		logger:               logger,
		ledgerService:        ledgerService,
		raftTransportHandler: raftHandler,
		snapshotClient:       snapshotClient,
	}
}

func (s *Server) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = lis

	// Create gRPC server with OpenTelemetry instrumentation
	// Filter out RaftTransportService to avoid too many traces
	opts := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler(
			otelgrpc.WithInterceptorFilter(func(info *otelgrpc.InterceptorInfo) bool {
				// Skip tracing for RaftTransportService to avoid too many traces
				if info.UnaryServerInfo != nil {
					return !strings.Contains(info.UnaryServerInfo.FullMethod, "RaftTransportService")
				}
				if info.StreamServerInfo != nil {
					return !strings.Contains(info.StreamServerInfo.FullMethod, "RaftTransportService")
				}
				return true
			}),
		)),
	}
	s.server = grpc.NewServer(opts...)

	// Register LedgerService
	service.RegisterLedgerServiceServer(s.server, newLedgerServiceServer(s.logger, s.ledgerService, s.snapshotClient))

	// Register RaftTransportService (always required for unified server)
	s.raftTransportHandler.RegisterRaftService(s.server)
	s.logger.Infof("Registered RaftTransportService on unified gRPC server")

	s.logger.WithFields(map[string]any{"port": s.port}).Infof("Starting gRPC server")

	go func() {
		if err := s.server.Serve(lis); err != nil {
			s.logger.WithFields(map[string]any{"error": err}).Errorf("gRPC server failed")
		}
	}()

	// Wait for context cancellation
	<-ctx.Done()
	return s.Stop()
}

func (s *Server) Stop() error {
	if s.server != nil {
		s.logger.Infof("Stopping gRPC server")
		s.server.GracefulStop()
	}
	return nil
}
