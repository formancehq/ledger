package grpc

import (
	"fmt"
	"net"
	"strings"

	"github.com/formancehq/go-libs/v3/logging"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

type Server struct {
	server   *grpc.Server
	listener net.Listener
	logger   logging.Logger
	port     int
}

func NewServer(port int, logger logging.Logger) *Server {
	opts := []grpc.ServerOption{
		grpc.StatsHandler(otelgrpc.NewServerHandler(
			otelgrpc.WithFilter(func(info *stats.RPCTagInfo) bool {
				return !strings.Contains(info.FullMethodName, "RaftTransportService")
			}),
		)),
	}

	return &Server{
		port:   port,
		logger: logger,
		server: grpc.NewServer(opts...),
	}
}

func (s *Server) GetServer() *grpc.Server {
	return s.server
}

func (s *Server) Start() error {
	lis, err := net.Listen("tcp4", fmt.Sprintf("0.0.0.0:%d", s.port))
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}
	s.listener = lis

	s.logger.
		WithFields(map[string]any{"addr": lis.Addr().String()}).
		Infof("Starting gRPC server")

	if err := s.server.Serve(lis); err != nil {
		return fmt.Errorf("gRPC server failed: %w", err)
	}
	return nil
}

func (s *Server) Stop() error {
	s.logger.Infof("Stopping gRPC server")
	if s.server != nil {
		// todo: could be a graceful shutdown but we need to handle properly in the raft transport
		s.server.Stop()
		s.server = nil
	}
	if s.listener != nil {
		_ = s.listener.Close()
		s.listener = nil
	}
	return nil
}
